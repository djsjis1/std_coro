#pragma once

#include "event_loop.hpp"

#include <atomic>
#include <coroutine>
#include <deque>
#include <mutex>
#include <utility>

// ============================================================================
// coro::sync — 协程同步原语 (Lock / Semaphore / Event / Condition)
// ============================================================================
//
// 类似 Python asyncio 的同步原语, 用于协程间的协调。
//
// 线程契约 (与 Future 对齐):
//   - 等待 (co_await acquire/wait) 必须发生在协程上下文 (事件循环线程);
//   - 唤醒 (release / set / notify) 可以从任意线程调用: 等待者在挂起时
//     记录了自己所在的事件循环, 唤醒会把句柄路由回该 loop 调度
//     (与 future.hpp 的 Waiter{handle, loop} 模式相同), 不会把协程帧
//     迁移到调用者线程。内部状态由各自 mutex 保护, 跨线程唤醒安全。
//
// Python 映射:
//   async with asyncio.Lock():     →  co_await lock.acquire(); ... lock.release();
//   async with asyncio.Semaphore:  →  co_await sem.acquire(); ... sem.release();
//   await asyncio.Event().wait()   →  co_await event.wait();
//   await asyncio.Condition.wait() →  co_await cond.wait();
// ============================================================================

namespace coro {

    namespace detail {
        /// 等待者条目: 句柄 + 它所在的事件循环。
        /// 唤醒方可能在其他线程 (工作线程算完直接 event.set() 是常见写法),
        /// 必须把句柄投回等待者「家」的 loop —— 路由到调用者线程的 loop
        /// 在该 loop 未运行时会让等待者永久挂死 (future.hpp 修复过同类问题)。
        struct sync_waiter {
            std::coroutine_handle<> handle{};
            EventLoop* loop = nullptr;
        };

        /// 把等待者调度回它「家」的事件循环
        inline void schedule_waiter(const sync_waiter& w) {
            (w.loop ? w.loop : &EventLoop::get())->schedule(w.handle);
        }

        /// 从等待队列摘除指定句柄 (协程帧销毁/被取消时的僵尸清理)。
        /// Lock / Event / Condition 的 on_waiter_destroyed 共用;
        /// 摘除不存在的句柄是无操作。
        inline void erase_waiter(std::deque<sync_waiter>& waiters, std::coroutine_handle<> h) noexcept {
            for (auto it = waiters.begin(); it != waiters.end();) {
                if (it->handle == h)
                    it = waiters.erase(it);
                else
                    ++it;
            }
        }
    } // namespace detail

    // ============================================================================
    // Lock — 互斥锁 (支持同一协程递归获取)
    // ============================================================================
    //
    // 用法:
    //   Lock lock;
    //   co_await lock.acquire();
    //   // ... 临界区 ...
    //   lock.release();
    //
    // 特点:
    //   - FIFO 公平调度: 等待者按 acquire 顺序获取锁
    //   - 递归: 同一协程重复 acquire 会增加递归计数, 需对应次数 release
    //     (注意: 递归持锁期间不要 co_await cond.wait() —— wait 会先释放
    //      最外层, 递归计数会让锁滞留在自己名下, 语义未定义)
    //   - release() 可在未持锁时调用 (无操作)
    //   - Guard (co_await lock.guard()) 析构时带持有者校验: 锁已不在自己
    //     名下时不释放 —— Condition::wait() 的取消展开路径靠它保证不偷锁
    // ============================================================================
    class Lock {
      public:
        Lock() = default;
        Lock(const Lock&) = delete;
        Lock& operator=(const Lock&) = delete;

        /// 获取锁。如果锁空闲则立即获取并设 locked_=true,
        /// 否则挂起当前协程排队等待
        bool await_ready() noexcept {
            // 总是进入 await_suspend 以便获取协程句柄设置 owner
            return false;
        }

        bool await_suspend(std::coroutine_handle<> h) {
            std::lock_guard lk(mtx_);
            // 重入检测: 如果当前协程已持有锁, 增加计数并立即恢复
            if (owner_ == h.address()) {
                ++recursion_count_;
                return false; // 不挂起, 立即恢复
            }
            // 如果锁空闲, 获取锁并设置所有者
            if (!locked_) {
                locked_ = true;
                owner_ = h.address();
                return false; // 不挂起, 立即恢复
            }
            // 否则加入等待队列 (记录等待者的家 loop, 供跨线程唤醒路由)
            waiters_.push_back({h, &EventLoop::get()});
            return true;
        }

        bool await_resume() const noexcept { return true; }

        /// 释放锁。如果有等待者, 直接将锁移交给队首等待者 (locked_ 保持 true)
        /// 如果没有等待者, 设 locked_=false
        /// release() 可从任意线程调用 (唤醒路由回等待者的家 loop)。
        void release() {
            detail::sync_waiter next{};
            {
                std::lock_guard lk(mtx_);
                // 重入: 减少计数, 只有计数到 0 才真正释放
                if (recursion_count_ > 0) {
                    --recursion_count_;
                    return;
                }
                // 未持有而调用 release: 按文档为无操作 (Python asyncio.Lock 在此抛
                // RuntimeError; C++ 侧 release 无调用者身份, 只能选择不破坏状态)
                if (!locked_)
                    return;
                next = unlock_locked_();
            }
            if (next.handle)
                detail::schedule_waiter(next);
        }

        /// 获取锁 (返回自身引用供 co_await 使用)
        Lock& acquire() { return *this; }

        /// 等待者协程帧被销毁时, 从等待队列摘除僵尸句柄
        /// (否则 release 时 schedule 已销毁的帧 → UB)
        ///
        /// 额外职责: 若本协程已被 release()/hand_off() 点名为「下一任持有者」
        /// 但在接管前被取消, 必须把锁继续传下去 —— 否则锁永远停留在已死协程
        /// 名下, 后续所有 acquire 永久挂起 (对齐 asyncio: 取消不得泄漏锁)。
        void on_waiter_destroyed(std::coroutine_handle<> h) noexcept {
            {
                std::lock_guard lk(mtx_);
                detail::erase_waiter(waiters_, h);
                if (owner_ != h.address())
                    return;
            }
            release(); // 把锁交给队首下一位 (或解锁)
        }

        /// 将锁直接移交给一个等待者 (Condition::notify 转移被通知者用):
        /// 锁空闲则立即移交并调度; 否则排入等待队列, 由当前持有者 release()
        /// 时移交。被通知者被唤醒时即已持有锁, 无需再经历一次 acquire 往返 ——
        /// 这正是经典 condition_variable 的实现技巧 (notify 把等待者移到
        /// 互斥量队列), 也是 "wait() 返回时重新持有锁" 语义的落地方式。
        void hand_off(detail::sync_waiter w) {
            bool direct = false;
            {
                std::lock_guard lk(mtx_);
                if (!locked_) {
                    locked_ = true;
                    owner_ = w.handle.address();
                    direct = true;
                } else {
                    waiters_.push_back(w);
                }
            }
            if (direct)
                detail::schedule_waiter(w);
        }

        // ==================================================================
        // RAII 守卫 (对标 Python 的 `async with lock:`)
        // ==================================================================
        //
        // 用法:
        //   {
        //       auto g = co_await lock.guard();   // 获取锁 (可能挂起)
        //       // ... 临界区 ...
        //   } // 离开作用域 → Guard 析构 → 自动 release (异常路径也安全)
        //
        // 持有者校验: Guard 析构时只在锁仍由「获取它的那个协程」持有时才
        // 释放。Condition::wait() 挂起期间已把锁交还, 其取消展开路径若继续
        // release 会偷走通知方持有的锁 (互斥失效); 对齐 asyncio "非持有者
        // 不得释放" 的语义, 这里选择安全忽略而非破坏状态。
        // ==================================================================
        struct Guard {
            Lock* lock_;
            void* owner_; // 获取锁时的协程帧地址 (析构校验用)
            explicit Guard(Lock* l, void* owner) noexcept : lock_(l), owner_(owner) {}
            ~Guard() { lock_->release_if_owner(owner_); }
            Guard(const Guard&) = delete;
            Guard& operator=(const Guard&) = delete;
        };

        /// co_await lock.guard() 的 awaiter: 获取锁, 恢复时返回 Guard
        struct guard_awaiter {
            Lock* lock;
            std::coroutine_handle<> my_handle{}; // 获取者身份 (Guard 校验用)

            bool await_ready() noexcept {
                // 总是进入 await_suspend 以便获取协程句柄设置 owner
                return false;
            }
            bool await_suspend(std::coroutine_handle<> h) {
                my_handle = h;
                std::lock_guard lk(lock->mtx_);
                if (lock->owner_ == h.address()) {
                    ++lock->recursion_count_;
                    return false;
                }
                if (!lock->locked_) {
                    lock->locked_ = true;
                    lock->owner_ = h.address();
                    return false;
                }
                lock->waiters_.push_back({h, &EventLoop::get()});
                return true;
            }
            Guard await_resume() noexcept { return Guard{lock, my_handle.address()}; }

            /// 挂起期间被取消/销毁: 摘链 + 若已被点名移交则把锁传下去 (同 Lock)
            void on_waiter_destroyed(std::coroutine_handle<> h) noexcept { lock->on_waiter_destroyed(h); }
        };

        guard_awaiter guard() noexcept { return {this}; }

        bool is_locked() const noexcept {
            std::lock_guard lk(mtx_);
            return locked_;
        }

      private:
        /// 锁内转移 (调用方必须持有 mtx_): 清除所有者, 有等待者则移交给队首。
        /// 返回要调度的等待者 (无则空句柄), 调度在锁外进行。
        detail::sync_waiter unlock_locked_() {
            owner_ = nullptr;
            if (!waiters_.empty()) {
                auto next = waiters_.front();
                waiters_.pop_front();
                owner_ = next.handle.address();
                return next; // locked_ 保持 true, 转移给新的持有者
            }
            locked_ = false;
            return {};
        }

        /// Guard 析构路径: 仅当锁仍由指定协程持有时释放 (递归计数同样要求
        /// owner 匹配), 否则忽略 —— 见 Guard 注释。
        void release_if_owner(void* owner) {
            detail::sync_waiter next{};
            {
                std::lock_guard lk(mtx_);
                if (owner_ != owner)
                    return; // 锁不在该协程名下: 不偷锁
                if (recursion_count_ > 0) {
                    --recursion_count_;
                    return;
                }
                if (!locked_)
                    return;
                next = unlock_locked_();
            }
            if (next.handle)
                detail::schedule_waiter(next);
        }

        mutable std::mutex mtx_; // 保护下方全部状态 (跨线程 release/set/notify 安全)
        bool locked_ = false;
        void* owner_ = nullptr;   // 当前持有锁的协程帧地址
        int recursion_count_ = 0; // 重入计数
        std::deque<detail::sync_waiter> waiters_;
    };

    // ============================================================================
    // Semaphore — 信号量 (限制并发数)
    // ============================================================================
    //
    // 用法:
    //   Semaphore sem(3);           // 最多 3 个协程同时进入
    //   co_await sem.acquire();     // 获取许可, 若已满则挂起
    //   // ... 受控区域 ...
    //   sem.release();              // 释放许可
    //
    // 典型场景:
    //   - 限制同时发起的 HTTP 连接数
    //   - 限制数据库连接池中的并发请求
    //
    // 取消安全 (与 Queue 相同的「预留」协议):
    //   release() 把许可直接点名交给队首等待者 (reserved = true) 并调度它。
    //   若该等待者在接管前被取消 (on_waiter_destroyed 而非 await_resume),
    //   许可必须传给下一位或归还计数 —— 否则一次取消就永久损失一个许可,
    //   信号量会随取消次数慢慢「漏干」直至死锁。
    //
    // release() 可从任意线程调用 (唤醒路由回等待者的家 loop)。
    // ============================================================================
    class Semaphore {
      public:
        explicit Semaphore(int permits) : permits_(permits) {}

        Semaphore(const Semaphore&) = delete;
        Semaphore& operator=(const Semaphore&) = delete;

        /// 获取许可的 awaiter 基础: 句柄 + 预留标记 + 家 loop。
        /// acquire 与 guard 两种等待者共用同一队列, 因此抽出公共基类。
        struct waiter {
            std::coroutine_handle<> h{};
            EventLoop* loop = nullptr; // 挂起时所在的事件循环 (跨线程唤醒路由)
            bool reserved = false;     // 许可已被点名交给我 (取消时必须传下去)
        };

        struct acquire_awaiter : waiter {
            Semaphore* sem;

            explicit acquire_awaiter(Semaphore* s) noexcept : sem(s) {}

            bool await_ready() noexcept {
                std::lock_guard lk(sem->mtx_);
                if (sem->permits_ > 0) {
                    --sem->permits_;
                    return true;
                }
                return false;
            }

            bool await_suspend(std::coroutine_handle<> h) {
                std::lock_guard lk(sem->mtx_);
                // await_ready() 返回 false 后, 其他线程可能恰好 release() 归还
                // 许可。必须在同一把锁内二次检查, 否则会把等待者挂到队列而
                // 留下无人领取的许可 (永久挂起)。
                if (sem->permits_ > 0) {
                    --sem->permits_;
                    return false;
                }
                this->h = h;
                this->loop = &EventLoop::get();
                sem->waiters_.push_back(this);
                return true;
            }

            bool await_resume() noexcept { return true; }

            /// 取消/销毁路径: 摘链; 已被点名的许可传给下一位
            void on_waiter_destroyed(std::coroutine_handle<>) noexcept { sem->remove_waiter(this); }
        };

        /// 获取许可。如果有可用许可, await_ready 返回 true 立即通过;
        /// 否则挂起等待。注意: await_ready 在可用时负责递减 permits_
        acquire_awaiter acquire() { return acquire_awaiter{this}; }

        /// 释放许可 (可从任意线程调用, 唤醒路由回等待者的家 loop)
        void release() {
            waiter* w = nullptr;
            {
                std::lock_guard lk(mtx_);
                if (!waiters_.empty()) {
                    w = waiters_.front();
                    waiters_.pop_front();
                    w->reserved = true; // 许可直接移交给它 (permits_ 保持 0)
                } else {
                    ++permits_;
                }
            }
            if (w)
                detail::schedule_waiter({w->h, w->loop});
        }

        /// 等待者被取消/销毁时的收尾 (按身份精确处理, 与 Queue 相同)
        void remove_waiter(waiter* w) {
            bool was_reserved = false;
            {
                std::lock_guard lk(mtx_);
                was_reserved = std::exchange(w->reserved, false);
                if (!was_reserved) {
                    for (auto it = waiters_.begin(); it != waiters_.end(); ++it) {
                        if (*it == w) {
                            waiters_.erase(it);
                            break;
                        }
                    }
                }
            }
            if (was_reserved)
                release(); // 许可已点名给我而我没能接管: 传给下一位或归还
        }

        // ==================================================================
        // RAII 守卫 (对标 Python 的 `async with sem:`)
        // 用法: { auto g = co_await sem.guard(); /* ... */ } // 析构自动 release
        // ==================================================================
        struct Guard {
            Semaphore* sem_;
            explicit Guard(Semaphore* s) noexcept : sem_(s) {}
            ~Guard() { sem_->release(); }
            Guard(const Guard&) = delete;
            Guard& operator=(const Guard&) = delete;
        };

        struct guard_awaiter : waiter {
            Semaphore* sem;

            explicit guard_awaiter(Semaphore* s) noexcept : sem(s) {}

            bool await_ready() noexcept {
                std::lock_guard lk(sem->mtx_);
                if (sem->permits_ > 0) {
                    --sem->permits_;
                    return true;
                }
                return false;
            }
            bool await_suspend(std::coroutine_handle<> h) {
                std::lock_guard lk(sem->mtx_);
                // 与 acquire_awaiter 相同: await_ready 与 await_suspend 之间
                // 的 release 不得把可用许可遗留在计数中。
                if (sem->permits_ > 0) {
                    --sem->permits_;
                    return false;
                }
                this->h = h;
                this->loop = &EventLoop::get();
                sem->waiters_.push_back(this);
                return true;
            }
            Guard await_resume() noexcept { return Guard{sem}; }

            /// 同 acquire_awaiter: 摘链 + 预留许可的传递
            void on_waiter_destroyed(std::coroutine_handle<>) noexcept { sem->remove_waiter(this); }
        };

        guard_awaiter guard() noexcept { return guard_awaiter{this}; }

        int available() const noexcept {
            std::lock_guard lk(mtx_);
            return permits_;
        }

      private:
        mutable std::mutex mtx_; // 保护 permits_ / waiters_ (跨线程 release 安全)
        int permits_;
        std::deque<waiter*> waiters_;
    };

    // ============================================================================
    // Event — 事件通知 (一次性)
    // ============================================================================
    //
    // 用法:
    //   Event event;
    //   // 协程 A: 等待事件
    //   co_await event.wait();
    //
    //   // 协程 B (或任意线程): 触发事件
    //   event.set();
    //
    // 特点:
    //   - set() 后所有当前和未来的 wait() 都立即返回
    //   - clear() 重置事件 (可选)
    //   - 与 Python asyncio.Event 行为一致
    //   - set() 可从任意线程调用 (唤醒路由回各等待者的家 loop)
    // ============================================================================
    class Event {
      public:
        Event() = default;
        Event(const Event&) = delete;
        Event& operator=(const Event&) = delete;

        /// 等待事件被 set()
        bool await_ready() const noexcept { return set_.load(std::memory_order_acquire); }

        bool await_suspend(std::coroutine_handle<> h) {
            std::lock_guard lk(mtx_);
            // await_ready() 看到 false 后, 其他线程可能已经 set() 并清空了
            // 当时的等待队列。锁内二次检查防止本协程随后入队而永远漏唤醒。
            if (set_.load(std::memory_order_acquire))
                return false;
            waiters_.push_back({h, &EventLoop::get()});
            return true;
        }

        void await_resume() const noexcept {}

        Event& wait() { return *this; }

        /// 等待者协程帧被销毁时, 从等待队列摘除僵尸句柄
        void on_waiter_destroyed(std::coroutine_handle<> h) noexcept {
            std::lock_guard lk(mtx_);
            detail::erase_waiter(waiters_, h);
        }

        /// 触发事件: 唤醒所有等待者 (路由回各自的家 loop)
        void set() {
            set_.store(true, std::memory_order_release);
            std::deque<detail::sync_waiter> to_wake;
            {
                std::lock_guard lk(mtx_);
                to_wake.swap(waiters_);
            }
            while (!to_wake.empty()) {
                auto w = to_wake.front();
                to_wake.pop_front();
                detail::schedule_waiter(w);
            }
        }

        /// 重置事件
        void clear() { set_.store(false, std::memory_order_release); }

        bool is_set() const noexcept { return set_.load(std::memory_order_acquire); }

      private:
        std::atomic<bool> set_ = false; // 事件标志 (无锁读)
        mutable std::mutex mtx_;        // 保护 waiters_
        std::deque<detail::sync_waiter> waiters_;
    };

    // ============================================================================
    // Condition — 条件变量 (对标 asyncio.Condition)
    // ============================================================================
    //
    // 用法 (调用者必须先持有关联锁):
    //
    //   coro::Lock lock;
    //   coro::Condition cond(&lock);
    //
    //   // 等待方:
    //   {
    //       auto g = co_await lock.guard();
    //       while (!ready)                       // 谓词循环 (防虚假唤醒)
    //           co_await cond.wait();            // 原子地: 释放锁挂起; 唤醒后重新持有锁
    //   }
    //
    //   // 通知方:
    //   {
    //       auto g = co_await lock.guard();
    //       ready = true;
    //       cond.notify();                       // 唤醒一个 (或 notify_all)
    //   }
    //
    // 实现 (直接 awaiter, 不再是子协程):
    //   wait 的挂起点在调用方协程帧内。notify() 不直接唤醒等待者, 而是
    //   把它转移到锁的等待队列 —— 持锁的通知方 release() 时锁直接移交给
    //   队首 (被通知者), 它被唤醒时已持有锁, 与 Python "wait 返回时重新
    //   持有锁" 语义一致。
    //
    // 取消安全 (子协程实现的历史缺陷已修复):
    //   旧实现把 wait 做成 Task 子协程, 取消展开会直接销毁子协程帧
    //   (帧内析构不运行 → 等待队列残留僵尸句柄), 且调用方 Guard 析构的
    //   二次 release 会偷走通知方持有的锁 (互斥失效)。现在:
    //     - 挂起点在调用方帧内, 取消时包装层析构正常运行,
    //       on_waiter_destroyed 把僵尸句柄从 condition/锁两个队列摘除;
    //     - Guard 析构带持有者校验, 锁不在自己名下时不释放。
    // ============================================================================
    class Condition {
      public:
        explicit Condition(Lock* lock) noexcept : lock_(lock) {}

        Condition(const Condition&) = delete;
        Condition& operator=(const Condition&) = delete;

        // ---- wait: 释放锁挂起; 被唤醒时已重新持有锁 ----

        struct wait_awaiter {
            Condition* cond;

            explicit wait_awaiter(Condition* c) noexcept : cond(c) {}

            bool await_ready() const noexcept { return false; }

            void await_suspend(std::coroutine_handle<> h) {
                // 排队 + 释放锁 (原子于事件循环单线程: await_suspend 完成后
                // 控制权才回到事件循环, 通知方不会插队)
                {
                    std::lock_guard lk(cond->mtx_);
                    cond->waiters_.push_back({h, &EventLoop::get()});
                }
                cond->lock_->release();
            }

            void await_resume() const noexcept {} // 被唤醒时锁已被移交给本协程

            /// 等待者协程被取消/销毁: 从 condition 队列和锁队列两边都尝试
            /// 摘除 (摘除不存在的句柄是无操作 —— 等待者可能尚未被 notify
            /// 转移, 也可能已转移)。若锁已被点名移交给自己,
            /// Lock::on_waiter_destroyed 会把锁继续传下去 (不泄漏)。
            void on_waiter_destroyed(std::coroutine_handle<> h) noexcept {
                {
                    std::lock_guard lk(cond->mtx_);
                    detail::erase_waiter(cond->waiters_, h);
                }
                cond->lock_->on_waiter_destroyed(h);
            }
        };

        /// 挂起等待通知 (调用前必须持有锁; 返回时重新持有锁)
        wait_awaiter wait() noexcept { return wait_awaiter{this}; }

        // ---- notify ----

        /// 唤醒至多 n 个等待者 (默认 1)。
        /// 通常在持有锁时调用: 等待者被转入锁的等待队列, 由本次临界区的
        /// release() 完成锁移交。未持锁调用时, 锁若空闲则直接移交给被
        /// 通知者。可从任意线程调用。
        void notify(size_t n = 1) {
            while (n-- > 0) {
                detail::sync_waiter w{};
                {
                    std::lock_guard lk(mtx_);
                    if (waiters_.empty())
                        return;
                    w = waiters_.front();
                    waiters_.pop_front();
                }
                lock_->hand_off(w); // 转移到锁队列: 醒来即持锁
            }
        }

        /// 唤醒所有等待者 (语义同 notify, 见上)
        void notify_all() {
            for (;;) {
                detail::sync_waiter w{};
                {
                    std::lock_guard lk(mtx_);
                    if (waiters_.empty())
                        return;
                    w = waiters_.front();
                    waiters_.pop_front();
                }
                lock_->hand_off(w);
            }
        }

        /// 当前等待者数量
        size_t waiter_count() const noexcept {
            std::lock_guard lk(mtx_);
            return waiters_.size();
        }

        /// 关联的锁 (供调用者获取/守卫使用)
        Lock* lock() const noexcept { return lock_; }

      private:
        Lock* lock_;                              // 关联的互斥锁
        mutable std::mutex mtx_;                  // 保护 waiters_ (跨线程 notify 安全)
        std::deque<detail::sync_waiter> waiters_; // 等待通知的协程
    };

} // namespace coro
