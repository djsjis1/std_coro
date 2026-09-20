#pragma once

#include "event_loop.hpp"
#include "exceptions.hpp"

#include <memory>
#include <mutex>
#include <optional>
#include <atomic>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <vector>

// ============================================================================
// coro::Promise<T> / coro::Future<T> — 手动完成模式
// ============================================================================
//
// 用于桥接「回调式异步代码」到「协程世界」。
//
// Python 映射:
//   future = asyncio.Future()     →  auto f = promise.get_future();
//   future.set_result(42)         →  promise.set_value(42);
//   await future                  →  co_await future;
//
// ============================================================================
//
// 典型使用场景:
//
//   场景 1: 包装基于回调的老旧 API
//   ─────────────────────────────
//   old_api.async_call([](int result) {
//       // 在回调中完成 Promise
//       promise.set_value(result);
//   });
//   int val = co_await future;  // 挂起直到回调触发
//
//   场景 2: 跨线程通信
//   ─────────────────
//   std::thread([promise = std::move(promise)]() mutable {
//       int result = heavy_computation();
//       promise.set_value(result);  // 在工作线程中完成
//   }).detach();
//   int val = co_await future;  // 在事件循环线程中等待
//
//   场景 3: 多个 Future 并发 (配合 gather)
//   ─────────────────────────────────────
//   auto [a, b] = co_await gather(promise_a.get_future(), ...);
//   但注意: gather 需要的是 Task, 不是 Future。
//   可以用一个小协程包装: co_return co_await future;
//
// ============================================================================
//
// SharedState 机制:
//
//   Promise 和 Future 通过 shared_ptr<SharedState> 共享状态。
//   这解决了生命周期问题:
//     - Future 可以在设置值之前或之后被 co_await
//     - Promise 正常完成后即使析构, Future 仍然可以读取结果
//     - Promise 未完成就析构时, Future 收到 BrokenPromiseError, 不会永久挂起
//     - 多个 Future 可以从同一个 Promise 创建 (共享同一个 SharedState)
//
//   await_ready() 检查:
//     - 如果值已经设置 (ready == true) → 立即返回, 不挂起
//     - 如果还没设置 → 挂起, 等 Promise 通知
//
//   通知机制:
//     Promise::set_value() / set_exception() 调用时:
//       1. 存储值/异常到 SharedState
//       2. 设置 ready = true
//       3. 如果有协程在等待 (continuation 非空), 将其加入就绪队列
//
// ============================================================================

namespace coro {

    template <typename T> class Future;

    // ============================================================================
    // Promise<T> — 生产者端
    // ============================================================================
    //
    // 线程安全: set_value / set_exception 可从任意线程调用 (与事件循环
    // 通过互斥锁 + EventLoop::schedule 的跨线程唤醒协作)。
    // 修复要点:
    //   - 多个等待者: waiters 用 vector, set 时全部唤醒 (旧实现单 continuation
    //     会被后来的等待者覆盖, 前一个永久挂起)
    //   - lost-wakeup 竞态: await_suspend 在锁内二次检查 ready,
    //     防止 set_value 插在 await_ready 与 await_suspend 之间时丢失唤醒
    //   - 重复 set 抛 std::logic_error (对标 Python InvalidStateError)
    // ============================================================================
    template <typename T> class Promise {
      public:
        Promise() : state_(std::make_shared<SharedState>()) {}

        ~Promise() { abandon_pending(); }

        Promise(Promise&& other) noexcept : state_(std::move(other.state_)) {}
        Promise& operator=(Promise&& other) noexcept {
            if (this != &other) {
                abandon_pending();
                state_ = std::move(other.state_);
            }
            return *this;
        }

        Promise(const Promise&) = delete;
        Promise& operator=(const Promise&) = delete;

        /// 创建关联的 Future (可多次调用, 返回多个 Future 共享同一状态)
        Future<T> get_future();

        /// 设置成功值, 并唤醒所有等待中的 Future
        void set_value(T value) {
            require_state();
            {
                std::lock_guard lock(state_->mtx);
                if (state_->ready)
                    throw std::logic_error("Promise: result already set");
                state_->result.emplace(std::move(value));
                state_->ready = true;
            }
            notify();
        }

        /// 设置异常, 并唤醒所有等待中的 Future
        /// await_resume() 会重新抛出此异常
        void set_exception(std::exception_ptr e) {
            require_state();
            if (!e)
                throw std::invalid_argument("Promise: exception must not be null");
            {
                std::lock_guard lock(state_->mtx);
                if (state_->ready)
                    throw std::logic_error("Promise: result already set");
                state_->exception = std::move(e);
                state_->ready = true;
            }
            notify();
        }

        /// 是否已完成
        bool is_done() const noexcept {
            if (!state_)
                return false;
            std::lock_guard lock(state_->mtx);
            return state_->ready;
        }

      private:
        /// 等待者条目: 句柄 + 它所在的 loop。
        /// 旧实现只有单个 owner_loop, 两个等待者分属不同 loop 时,
        /// set_value 会把第一个等待者调度到第二个等待者的 loop 上
        /// (协程帧跨线程迁移 → 数据竞争 / MSVC Debug CRT 堆断言)。
        /// 逐等待者记录 loop 后, 每个等待者都被唤醒回自己「家」的 loop。
        struct Waiter {
            std::coroutine_handle<> handle;
            EventLoop* loop; // 挂起时所在的事件循环 (跨线程唤醒路由)
        };

        /// 通知等待者。
        ///
        /// 这里故意在 state mutex 保护下完成“标记已领取 + schedule”这两个
        /// 动作。若先把 waiter swap 出锁再 schedule，等待者可能在这段窗口
        /// 被取消并销毁，随后通知线程会调度悬空 coroutine_handle。
        void notify() {
            std::lock_guard lock(state_->mtx);
            for (auto& w : state_->waiters) {
                if (w.loop)
                    w.loop->schedule(w.handle);
                else
                    EventLoop::get().schedule(w.handle);
            }
            state_->waiters.clear();
        }

        void require_state() const {
            if (!state_)
                throw std::logic_error("Promise: operation on moved-from promise");
        }

        void abandon_pending() noexcept {
            if (!state_)
                return;
            bool notify_waiters = false;
            {
                std::lock_guard lock(state_->mtx);
                if (!state_->ready) {
                    state_->exception = std::make_exception_ptr(BrokenPromiseError{});
                    state_->ready = true;
                    notify_waiters = true;
                }
            }
            if (notify_waiters)
                notify();
        }

        /// 共享状态 — Promise 和 Future 通过 shared_ptr 共享
        /// mtx 保护以下所有成员 (支持跨线程 set_value)
        struct SharedState {
            mutable std::mutex mtx;
            std::optional<T> result;      // 结果值 (设置后才有)
            std::exception_ptr exception; // 异常 (如果有)
            std::vector<Waiter> waiters;  // 等待此 Future 的协程 (多个, 各带自己的 loop)
            bool ready = false;           // 是否已完成
            bool consumed = false;        // move-only T 的单消费者保护
        };

        std::shared_ptr<SharedState> state_;

        friend class Future<T>;
    };

    // ============================================================================
    // Future<T> — 消费者端 (可被 co_await)
    // ============================================================================
    template <typename T> class Future {
      public:
        Future() = default; // 空未来 (valid()==false); 供"稍后填充"的成员场景
        Future(std::shared_ptr<typename Promise<T>::SharedState> state) : state_(std::move(state)) {}

        /// 是否关联了共享状态 (默认构造/被移动后为 false)
        bool valid() const noexcept { return state_ != nullptr; }

        // ---- Awaitable 接口 ----

        /// 如果值已经设置, 直接取结果, 不需要挂起
        bool await_ready() const {
            if (!state_)
                throw std::logic_error("Future: await on invalid future");
            std::lock_guard lock(state_->mtx);
            return state_->ready;
        }

        /// 挂起当前协程: 记录到等待者列表 (带自己的 loop), 等待 Promise 通知
        void await_suspend(std::coroutine_handle<> h) {
            if (!state_)
                throw std::logic_error("Future: await on invalid future");
            bool already_ready = false;
            {
                std::lock_guard lock(state_->mtx);
                if (state_->ready) {
                    // lost-wakeup 竞态窗口: set_value 恰好发生在
                    // await_ready 返回 false 之后 — 不挂起, 直接恢复自己
                    already_ready = true;
                } else {
                    // 记录等待者所在的 loop: 跨线程 set_value 时唤醒路由回这里
                    state_->waiters.push_back({h, &EventLoop::get()});
                }
            }
            if (already_ready)
                EventLoop::get().schedule(h);
        }

        /// 获取结果: 如果有异常则重新抛出; 多等待者安全 (拷贝语义)
        T await_resume() {
            if (!state_)
                throw std::logic_error("Future: resume on invalid future");
            std::lock_guard lock(state_->mtx);
            if (state_->exception) {
                std::rethrow_exception(state_->exception);
            }
            if constexpr (std::is_copy_constructible_v<T>) {
                return *state_->result; // copyable T 支持多个等待者
            } else {
                if (state_->consumed)
                    throw std::logic_error("Future: move-only result already consumed");
                state_->consumed = true;
                return std::move(*state_->result); // move-only T 只允许一个消费者
            }
        }

        /// 等待者协程帧被销毁时, 从等待列表中摘除自己
        void on_waiter_destroyed(std::coroutine_handle<> h) noexcept {
            if (!state_)
                return;
            std::lock_guard lock(state_->mtx);
            std::erase_if(state_->waiters, [h](typename Promise<T>::Waiter& w) {
                if (w.handle != h)
                    return false;
                return true;
            });
        }

      private:
        std::shared_ptr<typename Promise<T>::SharedState> state_;
    };

    // 分离定义: get_future 需要在 Future<T> 完整定义之后
    template <typename T> Future<T> Promise<T>::get_future() {
        require_state();
        return Future<T>(state_);
    }

    // ============================================================================
    // Promise<void> / Future<void> 特化
    // ============================================================================
    //
    // 与 <T> 版本的区别:
    //   - 不需要 std::optional<T> 存储值 (void 没有值)
    //   - set_value() 无参数, 只设置 has_result = true
    //   - await_resume() 返回 void
    // ============================================================================
    template <> class Promise<void> {
      public:
        Promise() : state_(std::make_shared<SharedState>()) {}

        ~Promise() { abandon_pending(); }

        Promise(Promise&& other) noexcept : state_(std::move(other.state_)) {}
        Promise& operator=(Promise&& other) noexcept {
            if (this != &other) {
                abandon_pending();
                state_ = std::move(other.state_);
            }
            return *this;
        }

        Promise(const Promise&) = delete;
        Promise& operator=(const Promise&) = delete;

        Future<void> get_future();

        void set_value() {
            require_state();
            {
                std::lock_guard lock(state_->mtx);
                if (state_->ready)
                    throw std::logic_error("Promise: result already set");
                state_->has_result = true;
                state_->ready = true;
            }
            notify();
        }

        void set_exception(std::exception_ptr e) {
            require_state();
            if (!e)
                throw std::invalid_argument("Promise<void>: exception must not be null");
            {
                std::lock_guard lock(state_->mtx);
                if (state_->ready)
                    throw std::logic_error("Promise: result already set");
                state_->exception = std::move(e);
                state_->ready = true;
            }
            notify();
        }

        bool is_done() const noexcept {
            if (!state_)
                return false;
            std::lock_guard lock(state_->mtx);
            return state_->ready;
        }

      private:
        // Waiter 定义与语义同 Promise<T> (逐等待者记录所在 loop)
        struct Waiter {
            std::coroutine_handle<> handle;
            EventLoop* loop;
        };

        void notify() {
            std::lock_guard lock(state_->mtx);
            for (auto& w : state_->waiters) {
                if (w.loop)
                    w.loop->schedule(w.handle);
                else
                    EventLoop::get().schedule(w.handle);
            }
            state_->waiters.clear();
        }

        void require_state() const {
            if (!state_)
                throw std::logic_error("Promise<void>: operation on moved-from promise");
        }

        void abandon_pending() noexcept {
            if (!state_)
                return;
            bool notify_waiters = false;
            {
                std::lock_guard lock(state_->mtx);
                if (!state_->ready) {
                    state_->exception = std::make_exception_ptr(BrokenPromiseError{});
                    state_->ready = true;
                    notify_waiters = true;
                }
            }
            if (notify_waiters)
                notify();
        }

        struct SharedState {
            mutable std::mutex mtx;
            bool has_result = false; // 是否已设置 (void 不需要存值)
            std::exception_ptr exception;
            std::vector<Waiter> waiters; // 多个等待者 (各带自己的 loop)
            bool ready = false;
        };

        std::shared_ptr<SharedState> state_;

        friend class Future<void>;
    };

    template <> class Future<void> {
      public:
        Future() = default; // 空未来 (valid()==false)
        Future(std::shared_ptr<typename Promise<void>::SharedState> state) : state_(std::move(state)) {}

        /// 是否关联了共享状态
        bool valid() const noexcept { return state_ != nullptr; }

        bool await_ready() const {
            if (!state_)
                throw std::logic_error("Future<void>: await on invalid future");
            std::lock_guard lock(state_->mtx);
            return state_->ready;
        }

        void await_suspend(std::coroutine_handle<> h) {
            if (!state_)
                throw std::logic_error("Future<void>: await on invalid future");
            bool already_ready = false;
            {
                std::lock_guard lock(state_->mtx);
                if (state_->ready) {
                    // lost-wakeup 竞态窗口: 不挂起, 直接恢复自己
                    already_ready = true;
                } else {
                    // 记录等待者所在的 loop: 跨线程 set_value 时唤醒路由回这里
                    state_->waiters.push_back({h, &EventLoop::get()});
                }
            }
            if (already_ready)
                EventLoop::get().schedule(h);
        }

        void await_resume() {
            if (!state_)
                throw std::logic_error("Future<void>: resume on invalid future");
            std::lock_guard lock(state_->mtx);
            if (state_->exception) {
                std::rethrow_exception(state_->exception);
            }
        }

        /// 等待者协程帧被销毁时, 从等待列表中摘除自己
        void on_waiter_destroyed(std::coroutine_handle<> h) noexcept {
            if (!state_)
                return;
            std::lock_guard lock(state_->mtx);
            std::erase_if(state_->waiters, [h](typename Promise<void>::Waiter& w) {
                if (w.handle != h)
                    return false;
                return true;
            });
        }

      private:
        std::shared_ptr<typename Promise<void>::SharedState> state_;
    };

    inline Future<void> Promise<void>::get_future() {
        require_state();
        return Future<void>(state_);
    }

} // namespace coro
