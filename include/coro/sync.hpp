#pragma once

#include "event_loop.hpp"
#include "task.hpp" // Condition::wait() 返回 Task<>, 使用 CancelledError

#include <coroutine>
#include <deque>
#include <optional>
#include <utility>

// ============================================================================
// coro::sync — 协程同步原语 (Lock / Semaphore / Event)
// ============================================================================
//
// 类似 Python asyncio 的同步原语, 用于协程间的协调。
// 所有操作必须在事件循环线程上调用 (单线程模型)。
//
// Python 映射:
//   async with asyncio.Lock():     →  co_await lock.acquire(); ... lock.release();
//   async with asyncio.Semaphore:  →  co_await sem.acquire(); ... sem.release();
//   await asyncio.Event().wait()   →  co_await event.wait();
// ============================================================================

namespace coro {

    namespace detail {
        /// 从等待队列摘除指定句柄 (协程帧销毁/被取消时的僵尸清理)。
        /// Lock / Event / Condition 的 on_waiter_destroyed 共用。
        inline void erase_waiter(std::deque<std::coroutine_handle<>>& waiters, std::coroutine_handle<> h) noexcept {
            for (auto it = waiters.begin(); it != waiters.end();) {
                if (*it == h)
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
    //   - release() 可在未持锁时调用 (无操作)
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
            // 否则加入等待队列
            waiters_.push_back(h);
            return true;
        }

        bool await_resume() const noexcept { return true; }

        /// 释放锁。如果有等待者, 直接将锁移交给队首等待者 (locked_ 保持 true)
        /// 如果没有等待者, 设 locked_=false
        void release() {
            // 重入: 减少计数, 只有计数到 0 才真正释放
            if (recursion_count_ > 0) {
                --recursion_count_;
                return;
            }
            // 未持有而调用 release: 按文档为无操作 (Python asyncio.Lock 在此抛
            // RuntimeError; C++ 侧 release 无调用者身份, 只能选择不破坏状态)
            if (!locked_)
                return;
            // 清除所有者
            owner_ = nullptr;
            if (!waiters_.empty()) {
                auto h = waiters_.front();
                waiters_.pop_front();
                owner_ = h.address();
                EventLoop::get().schedule(h);
                // locked_ 保持 true, 转移给新的持有者
            } else {
                locked_ = false;
            }
        }

        /// 获取锁 (返回自身引用供 co_await 使用)
        Lock& acquire() { return *this; }

        /// 等待者协程帧被销毁时, 从等待队列摘除僵尸句柄
        /// (否则 release 时 schedule 已销毁的帧 → UB)
        ///
        /// 额外职责: 若本协程已被 release() 点名为「下一任持有者」但在接管前
        /// 被取消, 必须把锁继续传下去 —— 否则锁永远停留在已死协程名下,
        /// 后续所有 acquire 永久挂起 (对齐 asyncio: 取消不得泄漏锁)。
        void on_waiter_destroyed(std::coroutine_handle<> h) noexcept {
            detail::erase_waiter(waiters_, h);
            if (owner_ == h.address())
                release(); // 把锁交给队首下一位 (或解锁)
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
        // ==================================================================
        struct Guard {
            Lock* lock_;
            explicit Guard(Lock* l) noexcept : lock_(l) {}
            ~Guard() { lock_->release(); }
            Guard(const Guard&) = delete;
            Guard& operator=(const Guard&) = delete;
        };

        /// co_await lock.guard() 的 awaiter: 获取锁, 恢复时返回 Guard
        struct guard_awaiter {
            Lock* lock;

            bool await_ready() noexcept {
                // 总是进入 await_suspend 以便获取协程句柄设置 owner
                return false;
            }
            bool await_suspend(std::coroutine_handle<> h) {
                // 重入检测: 如果当前协程已持有锁, 增加计数并立即恢复
                if (lock->owner_ == h.address()) {
                    ++lock->recursion_count_;
                    return false;
                }
                // 如果锁空闲, 获取锁并设置所有者
                if (!lock->locked_) {
                    lock->locked_ = true;
                    lock->owner_ = h.address();
                    return false;
                }
                lock->waiters_.push_back(h);
                return true;
            }
            Guard await_resume() noexcept { return Guard{lock}; }

            /// 挂起期间被取消/销毁: 摘链 + 若已被点名移交则把锁传下去 (同 Lock)
            void on_waiter_destroyed(std::coroutine_handle<> h) noexcept { lock->on_waiter_destroyed(h); }
        };

        guard_awaiter guard() noexcept { return {this}; }

        bool is_locked() const noexcept { return locked_; }

      private:
        bool locked_ = false;
        void* owner_ = nullptr;   // 当前持有锁的协程帧地址
        int recursion_count_ = 0; // 重入计数
        std::deque<std::coroutine_handle<>> waiters_;
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
    // ============================================================================
    class Semaphore {
      public:
        explicit Semaphore(int permits) : permits_(permits) {}

        Semaphore(const Semaphore&) = delete;
        Semaphore& operator=(const Semaphore&) = delete;

        /// 获取许可的 awaiter 基础: 句柄 + 预留标记。
        /// acquire 与 guard 两种等待者共用同一队列, 因此抽出公共基类。
        struct waiter {
            std::coroutine_handle<> h{};
            bool reserved = false; // 许可已被点名交给我 (取消时必须传下去)
        };

        struct acquire_awaiter : waiter {
            Semaphore* sem;

            explicit acquire_awaiter(Semaphore* s) noexcept : sem(s) {}

            bool await_ready() noexcept {
                if (sem->permits_ > 0) {
                    --sem->permits_;
                    return true;
                }
                return false;
            }

            void await_suspend(std::coroutine_handle<> h) {
                this->h = h;
                sem->waiters_.push_back(this);
            }

            bool await_resume() noexcept { return true; }

            /// 取消/销毁路径: 摘链; 已被点名的许可传给下一位
            void on_waiter_destroyed(std::coroutine_handle<>) noexcept { sem->remove_waiter(this); }
        };

        /// 获取许可。如果有可用许可, await_ready 返回 true 立即通过;
        /// 否则挂起等待。注意: await_ready 在可用时负责递减 permits_
        acquire_awaiter acquire() { return acquire_awaiter{this}; }

        void release() {
            if (!waiters_.empty()) {
                auto* w = waiters_.front();
                waiters_.pop_front();
                w->reserved = true; // 许可直接移交给它 (permits_ 保持 0)
                EventLoop::get().schedule(w->h);
            } else {
                ++permits_;
            }
        }

        /// 等待者被取消/销毁时的收尾 (按身份精确处理, 与 Queue 相同)
        void remove_waiter(waiter* w) {
            if (std::exchange(w->reserved, false)) {
                release(); // 许可已点名给我而我没能接管: 传给下一位或归还
            } else {
                for (auto it = waiters_.begin(); it != waiters_.end(); ++it) {
                    if (*it == w) {
                        waiters_.erase(it);
                        break;
                    }
                }
            }
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
                if (sem->permits_ > 0) {
                    --sem->permits_;
                    return true;
                }
                return false;
            }
            void await_suspend(std::coroutine_handle<> h) {
                this->h = h;
                sem->waiters_.push_back(this);
            }
            Guard await_resume() noexcept { return Guard{sem}; }

            /// 同 acquire_awaiter: 摘链 + 预留许可的传递
            void on_waiter_destroyed(std::coroutine_handle<>) noexcept { sem->remove_waiter(this); }
        };

        guard_awaiter guard() noexcept { return guard_awaiter{this}; }

        int available() const noexcept { return permits_; }

      private:
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
    //   // 协程 B: 触发事件
    //   event.set();
    //
    // 特点:
    //   - set() 后所有当前和未来的 wait() 都立即返回
    //   - clear() 重置事件 (可选)
    //   - 与 Python asyncio.Event 行为一致
    // ============================================================================
    class Event {
      public:
        Event() = default;
        Event(const Event&) = delete;
        Event& operator=(const Event&) = delete;

        /// 等待事件被 set()
        bool await_ready() const noexcept { return set_; }

        void await_suspend(std::coroutine_handle<> h) { waiters_.push_back(h); }

        void await_resume() const noexcept {}

        Event& wait() { return *this; }

        /// 等待者协程帧被销毁时, 从等待队列摘除僵尸句柄
        void on_waiter_destroyed(std::coroutine_handle<> h) noexcept { detail::erase_waiter(waiters_, h); }

        /// 触发事件: 唤醒所有等待者
        void set() {
            set_ = true;
            while (!waiters_.empty()) {
                auto h = waiters_.front();
                waiters_.pop_front();
                EventLoop::get().schedule(h);
            }
        }

        /// 重置事件
        void clear() { set_ = false; }

        bool is_set() const noexcept { return set_; }

      private:
        bool set_ = false;
        std::deque<std::coroutine_handle<>> waiters_;
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
    //           co_await cond.wait();            // 原子地: 释放锁挂起; 唤醒后重新获取锁
    //   }
    //
    //   // 通知方:
    //   {
    //       auto g = co_await lock.guard();
    //       ready = true;
    //       cond.notify();                       // 唤醒一个 (或 notify_all)
    //   }
    //
    // 实现: wait() 是协程 — 先挂起到等待队列并释放锁, 被唤醒后重新 acquire 锁
    // (与 Python 的 Condition.wait 语义一致: 返回时重新持有锁)。
    // ============================================================================
    class Condition {
      public:
        explicit Condition(Lock* lock) noexcept : lock_(lock) {}

        Condition(const Condition&) = delete;
        Condition& operator=(const Condition&) = delete;

        // ---- wait: 释放锁挂起; 被唤醒后重新获取锁 ----

        struct wait_suspend_awaiter {
            Condition* cond;

            bool await_ready() const noexcept { return false; }

            void await_suspend(std::coroutine_handle<> h) {
                // 排队 + 释放锁 (原子于事件循环单线程: await_suspend 完成后
                // 控制权才回到事件循环, 通知方不会插队)
                cond->waiters_.push_back(h);
                cond->lock_->release();
            }

            void await_resume() const noexcept {}

            /// 等待者协程被取消/销毁: 摘除僵尸句柄
            void on_waiter_destroyed(std::coroutine_handle<> h) noexcept { cond->on_waiter_destroyed(h); }
        };

        /// 挂起等待通知 (调用前必须持有锁; 返回时重新持有锁)
        ///
        /// 取消安全: wait 挂起期间已把锁交还。若等待者被取消, 这里会
        /// 先重新拿回锁再传播 CancelledError——保证调用方的 guard/手动
        /// release 语义正确 (锁恰好被释放一次, 不会双重转移)。
        Task<> wait() {
            bool cancelled = false;
            try {
                co_await wait_suspend_awaiter{this};
            } catch (const CancelledError&) {
                cancelled = true; // 记标志 (MSVC: catch 块内不能 co_await)
            }
            co_await lock_->acquire(); // 正常/取消路径都重新拿锁
            if (cancelled)
                throw CancelledError{}; // 拿回锁后继续传播取消
            co_return;
        }

        // ---- notify ----

        /// 唤醒至多 n 个等待者 (默认 1)
        void notify(size_t n = 1) {
            while (n-- > 0 && !waiters_.empty()) {
                auto h = waiters_.front();
                waiters_.pop_front();
                EventLoop::get().schedule(h);
            }
        }

        /// 唤醒所有等待者
        void notify_all() {
            while (!waiters_.empty()) {
                auto h = waiters_.front();
                waiters_.pop_front();
                EventLoop::get().schedule(h);
            }
        }

        /// 当前等待者数量
        size_t waiter_count() const noexcept { return waiters_.size(); }

        /// 关联的锁 (供调用者获取/守卫使用)
        Lock* lock() const noexcept { return lock_; }

        // ---- 清理 ----

        /// 等待者协程帧被销毁时, 摘除僵尸句柄
        void on_waiter_destroyed(std::coroutine_handle<> h) noexcept { detail::erase_waiter(waiters_, h); }

      private:
        Lock* lock_;                                  // 关联的互斥锁
        std::deque<std::coroutine_handle<>> waiters_; // 等待通知的协程
    };

} // namespace coro
