#pragma once

#include "event_loop.hpp"
#include "exceptions.hpp"
#include "task.hpp"

#include <atomic>
#include <chrono>
#include <coroutine>
#include <memory>
#include <optional>

// ============================================================================
// coro::Timer — 一次性、可取消、可重置的定时器
// ============================================================================
//
// 用法:
//   coro::Timer t{500ms};
//   if (co_await t.wait()) { /* 到期 */ } else { /* 被 cancel */ }
//   t.reset(200ms);            // 把当前等待迁移到新期限, 旧注册作废
//
// 合同 (刻意收窄, 不起后台任务):
//   - 同一时刻只允许一个等待者; 第二个 wait() 直接抛 StructuredConcurrencyError,
//     不静默丢唤醒。
//   - wait() 到期返回 true; cancel() 唤醒等待者并令其返回 false;
//     等待任务自身被取消时照常抛 CancelledError (由 Task 的取消机制负责)。
//   - reset() 区分两种情形: 有等待者时只把它的 deadline 往后搬; 没有等待者时
//     重新开局, 使"周期行为由用户循环 reset 表达"真的可用。
//   - 析构等价于 cancel(): 绝不留下再也醒不过来的等待者。
//   - 全部使用 steady_clock, 不受系统时间调整影响。
//
// 两条设计约束 (都是踩过的坑, 用类型布局固化下来):
//   1. 等待协程必须是**自由协程**且只捕获 shared_ptr<timer_state>: 成员协程的帧会
//      捕获 this, Timer 先于等待者销毁时恢复即访问悬空对象。
//   2. 不给 EventLoop 加"提前唤醒定时器"接口: 唤醒走就绪队列, 原堆条目靠失效标记作废;
//      "取消与到期同时发生"造成的重复入队由事件循环既有的 scheduled_set_ 去重与
//      live_frames_ 代次校验兜住, 终态再由 CAS 单选, 因此不会 double-resume。
// ============================================================================

namespace coro {
    namespace detail {

        /// 等待终态: 除 pending 外互斥, 先到先得且不可翻转
        enum class timer_outcome : int { pending = 0, expired = 1, cancelled = 2 };

        /// Timer 与等待协程共享的状态。等待方只引用它, 因此 Timer 对象本身可以先于
        /// 等待者销毁 (见文件头约束 1)。
        struct timer_state {
            std::atomic<int> result{static_cast<int>(timer_outcome::pending)};
            std::atomic<bool> waiting{false};
            /// 是否已有等待者。在 wait() 被调用的那一刻就占位, 而不是等 await_suspend ——
            /// 后者取决于调度时机, 会让"拒绝第二个等待者"变成看运气的行为。
            std::atomic<bool> wait_taken{false};
            std::shared_ptr<std::atomic<bool>> token; ///< 定时器堆条目的失效标记
            std::coroutine_handle<> handle{};         ///< 等待者 (仅 owner loop 线程解引用)
            EventLoop* owner = nullptr;
            std::optional<std::chrono::steady_clock::time_point> deadline;

            /// 终态单选: 取消与到期竞态时只有先到者成功
            bool claim(timer_outcome o) noexcept {
                int expected = static_cast<int>(timer_outcome::pending);
                return result.compare_exchange_strong(expected, static_cast<int>(o));
            }

            /// 重新登记 deadline。必须先把旧条目的失效标记置起, 否则旧期限照样到期唤醒
            /// 等待者, "迁移期限"就成了空操作。只能在 owner loop 线程调用。
            void rearm() {
                if (token)
                    *token = true;
                token = nullptr;
                if (!deadline.has_value() || !waiting.load())
                    return;
                if (result.load() != static_cast<int>(timer_outcome::pending))
                    return;
                token = std::make_shared<std::atomic<bool>>(false);
                owner->schedule_timer(handle, *deadline, token);
            }
        };

        struct timer_wait_awaiter {
            std::shared_ptr<timer_state> st;

            /// 已有终态时不挂起 (例如 cancel 发生在 wait 之前)
            bool await_ready() const noexcept { return st->result.load() != static_cast<int>(timer_outcome::pending); }

            void await_suspend(std::coroutine_handle<> h) {
                auto& loop = EventLoop::get();
                st->owner = &loop;
                st->handle = h;
                st->waiting.store(true);
                if (st->result.load() != static_cast<int>(timer_outcome::pending)) {
                    // 注册期间已定终态: 自己唤醒自己, 不留无主等待
                    loop.schedule(h);
                    return;
                }
                if (st->deadline.has_value()) {
                    st->token = std::make_shared<std::atomic<bool>>(false);
                    loop.schedule_timer(h, *st->deadline, st->token);
                }
            }

            bool await_resume() noexcept {
                st->waiting.store(false);
                st->wait_taken.store(false); // 允许后续再 wait (一次性语义由 result 约束)
                if (st->token)
                    *st->token = true; // 本次注册已消费, 堆里的条目作废
                if (st->claim(timer_outcome::expired))
                    return true;
                return static_cast<timer_outcome>(st->result.load()) == timer_outcome::expired;
            }
        };

        inline Task<bool> timer_wait(std::shared_ptr<timer_state> st) {
            co_return co_await timer_wait_awaiter{std::move(st)};
        }

    } // namespace detail

    class Timer {
      public:
        using outcome = detail::timer_outcome;

        Timer() = default;

        template <typename Rep, typename Period> explicit Timer(std::chrono::duration<Rep, Period> d) {
            state_->deadline =
                std::chrono::steady_clock::now() + std::chrono::duration_cast<std::chrono::milliseconds>(d);
        }

        Timer(const Timer&) = delete;
        Timer& operator=(const Timer&) = delete;
        Timer(Timer&&) noexcept = default;
        Timer& operator=(Timer&&) noexcept = default;

        /// 析构: 若还有等待者, 以"取消"方式唤醒它 (绝不留永久挂起的协程)
        ~Timer() {
            if (state_)
                do_cancel();
        }

        /// 等待到期。true = 到期, false = 被 Timer::cancel() 唤醒。
        /// 只允许一个等待者: 第二个等待者不被静默丢唤醒, 而是在调用时就拒绝。
        /// 本函数不是协程 —— 挂起点在自由协程里, 帧不捕获 this (见文件头约束 1)。
        Task<bool> wait() {
            bool expected = false;
            if (!state_->wait_taken.compare_exchange_strong(expected, true))
                throw StructuredConcurrencyError("Timer::wait already has a waiter");
            return detail::timer_wait(state_);
        }

        /// 把等待迁移到 now + d。
        ///
        /// 线程合同: **只能在拥有本 Timer 的 EventLoop 线程调用**。原因不是懒,
        /// 而是无等待者分支要普通地写 owner / handle (它们不是原子量, 只有
        /// token 指向的对象、wait_taken、result 是), 而 await_suspend 在 owner 线程
        /// 写同一批字段 —— 跨线程调 reset 就是数据竞争。需要跨线程终止请用
        /// cancel() (它只动原子量并把唤醒投递回 owner loop)。
        ///
        /// 两种情形必须区分, 否则"周期行为由用户循环 reset 表达"不成立:
        ///   - 已有等待者: 只把它的 deadline 往后搬, 不结束等待;
        ///   - 无等待者 (上一轮已结束): 重新开局, 让下一次 wait() 真的再等一次。
        ///     否则到期后的终态会让 wait() 立刻返回, 用户拿到的是一个空转循环。
        template <typename Rep, typename Period> void reset(std::chrono::duration<Rep, Period> d) {
            auto st = state_;
            st->deadline = std::chrono::steady_clock::now() + std::chrono::duration_cast<std::chrono::milliseconds>(d);
            if (st->waiting.load()) {
                // 定时器堆只允许 owner loop 线程碰, 因此重新登记必须投递回去
                if (st->owner != nullptr)
                    st->owner->dispatch([st] { st->rearm(); });
                return;
            }
            if (st->token)
                *st->token = true; // 作废旧条目, 不留一个将来会乱醒的定时器
            st->token = nullptr;
            st->owner = nullptr;
            st->handle = {};
            st->wait_taken.store(false);
            st->result.store(static_cast<int>(outcome::pending));
        }

        /// 请求取消并唤醒等待者 (非阻塞)。可从任意线程调用。
        void cancel() { do_cancel(); }

        /// 是否已进入终态 (到期或被取消)
        bool done() const noexcept { return state_->result.load() != static_cast<int>(outcome::pending); }

        /// 当前是否有等待者
        bool has_waiter() const noexcept { return state_->waiting.load(); }

      private:
        void do_cancel() {
            auto st = state_;
            const bool was_waiting = st->waiting.exchange(false);
            if (!st->claim(outcome::cancelled))
                return; // 已有终态 (通常是同时到期), 不重复唤醒
            if (st->token)
                *st->token = true; // 堆里的条目变僵尸, 到期时被丢弃
            if (was_waiting && st->owner != nullptr)
                st->owner->schedule(st->handle);
        }

        std::shared_ptr<detail::timer_state> state_ = std::make_shared<detail::timer_state>();
    };

} // namespace coro
