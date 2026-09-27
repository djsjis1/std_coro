#pragma once

#include "event_loop.hpp"
#include "exceptions.hpp"
#include "task.hpp"

#include <atomic>
#include <chrono>
#include <coroutine>
#include <memory>
#include <mutex>
#include <optional>
#include <utility>
#include <vector>

// ============================================================================
// coro::Context / CancellationSource / CancellationToken
// ============================================================================
//
// 只承载两件事: 取消关系 与 deadline (steady_clock)。不放日志、不放服务定位、
// 不放任意键值容器 —— 一旦掺进"随手塞对象"的袋子, 取消语义就再也讲不清楚。
//
// 用法 (显式传递, 不用 thread_local 隐式上下文):
//   coro::CancellationSource src;
//   auto ctx  = coro::Context::from(src.token());
//   auto child = ctx.make_child();        // 继承父取消; deadline 取父子较早值
//   co_await child.wait();                // 被取消 -> CancelledError; 超期 -> TimeoutError
//   src.cancel();                         // 任意线程可调用
//
// 合同:
//   - 取消与普通异常分开: 取消抛 CancelledError, deadline 到点抛 TimeoutError。
//   - 先到的终态获胜且不可翻转 (取消与超时同时发生也只报告一个原因)。
//   - 允许多个等待者; 已取消后新的 wait() 立即抛, 不静默吞掉取消事件。
//   - 唤醒一律投递回等待者自己的 EventLoop: 绝不跨线程直接 cancel 别人的任务。
// ============================================================================

namespace coro {
    namespace detail {

        /// 取消状态: source 与所有 token/等待者共享。生命周期由 shared_ptr 保证,
        /// 因此 source 先于子协程析构也不会悬空。
        struct cancellation_state {
            std::mutex mutex;
            std::atomic<bool> cancelled{false};
            std::vector<std::pair<std::coroutine_handle<>, EventLoop*>> waiters;

            /// 终态单选: 只有第一次 cancel 返回 true
            bool claim_cancel() noexcept {
                bool expected = false;
                return cancelled.compare_exchange_strong(expected, true);
            }

            /// 摘一个等待者交给调用方唤醒 (每个等待者至多被摘走一次)
            bool take_waiter(std::pair<std::coroutine_handle<>, EventLoop*>& out) {
                std::lock_guard lock(mutex);
                if (waiters.empty())
                    return false;
                out = waiters.back();
                waiters.pop_back();
                return true;
            }

            void add_waiter(std::coroutine_handle<> h, EventLoop* loop) {
                std::lock_guard lock(mutex);
                waiters.emplace_back(h, loop);
            }

            /// 等待者帧即将销毁 (被取消/提前退出): 摘链, 防止 cancel 唤醒野句柄
            void drop_waiter(std::coroutine_handle<> h) {
                std::lock_guard lock(mutex);
                for (auto it = waiters.begin(); it != waiters.end(); ++it) {
                    if (it->first == h) {
                        waiters.erase(it);
                        return;
                    }
                }
            }
        };

        /// 等待"取消或 deadline"其中之一。原因判定: 取消一旦置位即为原因
        /// (claim_cancel 已将其串行化), 只有未被取消时才报告超时。
        struct context_wait_awaiter {
            std::shared_ptr<cancellation_state> st;
            std::optional<std::chrono::steady_clock::time_point> deadline;
            std::shared_ptr<std::atomic<bool>> timer_token;
            std::coroutine_handle<> registered{}; // add_waiter 用的句柄 (摘链依据)

            bool await_ready() const noexcept { return st->cancelled.load(); }

            void await_suspend(std::coroutine_handle<> h) {
                auto& loop = EventLoop::get();
                registered = h;
                st->add_waiter(h, &loop);
                if (st->cancelled.load()) {
                    // 竞态窗口: 检查之后、挂链之前发生了 cancel, 对方已不再能看到我们
                    retire();
                    loop.schedule(h);
                    return;
                }
                if (deadline.has_value()) {
                    timer_token = std::make_shared<std::atomic<bool>>(false);
                    loop.schedule_timer(h, *deadline, timer_token);
                }
            }

            void await_resume() const {
                if (timer_token)
                    *timer_token = true; // 本次已消费, 堆里的定时器条目作废
                retire();                // 正常结束也要摘链, 否则野句柄留在 waiters 里
                if (st->cancelled.load())
                    throw CancelledError();
                throw TimeoutError();
            }

            /// 摘链必须挂在生命周期终点, 不能只挂在 await_resume: 任务被取消时
            /// CancelledError 由框架的取消检查包装器抛出, **绕过本 awaiter 的
            /// await_resume** —— 于是超时/取消后的协程帧销毁了, waiters 里却仍留着
            /// 它的句柄, 之后任意一次 source.cancel() 都会对野句柄 schedule。
            /// drop_waiter 按句柄查找, 摘不到即空操作, 所以移动导致的二次析构与
            /// await_resume 已摘过的情况都安全。
            void retire() const {
                if (registered)
                    st->drop_waiter(registered);
            }

            ~context_wait_awaiter() { retire(); }
        };

        inline Task<void> context_wait(context_wait_awaiter aw) {
            co_await aw;
            co_return;
        }

        /// 只给"等取消"用的自由协程: 取消在这里是正常结局, 不作为异常向上抛。
        /// 同样不能做成 CancellationToken 的成员协程 —— 令牌是按值传递的临时量,
        /// 挂起后它可能先于等待者销毁。
        inline Task<void> token_wait(std::shared_ptr<cancellation_state> st) {
            try {
                co_await context_wait_awaiter{std::move(st), std::nullopt, nullptr};
            } catch (const CancelledError&) {
                // 取消就是等待的答案
            }
            co_return;
        }

    } // namespace detail

    /// 取消令牌: 观察取消是否发生, 并可挂起等待取消
    class CancellationToken {
      public:
        CancellationToken() = default;
        explicit CancellationToken(std::shared_ptr<detail::cancellation_state> st) : state_(std::move(st)) {}

        /// 默认构造的 token 没有 owner, 也就不可能再有人 cancel; 把它定为"已取消"
        /// 比"永不取消"更安全 —— 后者会静默吞掉本该发生的取消。
        bool cancelled() const noexcept { return !state_ || state_->cancelled.load(); }

        /// 挂起直到被取消 (不带 deadline; 要"取消或超时二选一"用 Context::wait)。
        /// 返回自由协程而不是写成成员协程: 令牌是按值传递的短命对象。
        Task<void> wait_cancelled() const { return detail::token_wait(state_); }

      private:
        friend class CancellationSource;
        friend class Context;
        std::shared_ptr<detail::cancellation_state> state_;
    };

    /// 取消源: 持有发起取消的权利
    class CancellationSource {
      public:
        CancellationSource() : state_(std::make_shared<detail::cancellation_state>()) {}

        CancellationSource(const CancellationSource&) = default;
        CancellationSource& operator=(const CancellationSource&) = default;

        /// 发起取消: 非阻塞, 任意线程可调, 重复调用无副作用
        void cancel() const {
            if (!state_->claim_cancel())
                return;
            std::pair<std::coroutine_handle<>, EventLoop*> waiter{};
            while (state_->take_waiter(waiter)) {
                if (waiter.second != nullptr)
                    waiter.second->schedule(waiter.first);
            }
        }

        bool cancelled() const noexcept { return state_->cancelled.load(); }

        CancellationToken token() const { return CancellationToken{state_}; }

      private:
        std::shared_ptr<detail::cancellation_state> state_;
    };
    /// 上下文: 只携带取消关系与 deadline, 显式传递给下层协程。
    /// 按值可拷贝 (共享同一份取消状态), 拷贝不创建新的取消链。
    class Context {
      public:
        using time_point = std::chrono::steady_clock::time_point;

        /// 根上下文: 无人能取消它, 也没有期限
        Context() : state_(std::make_shared<detail::cancellation_state>()) {}

        /// 从取消源的令牌派生; 不传令牌时相当于根上下文
        static Context from(CancellationToken token, std::optional<time_point> deadline = std::nullopt) {
            auto st = token.state_ ? token.state_ : std::make_shared<detail::cancellation_state>();
            return Context{std::move(st), deadline};
        }

        static Context root() { return Context{}; }

        /// 只带期限、不带取消源的上下文
        template <typename Rep, typename Period> static Context with_timeout(std::chrono::duration<Rep, Period> d) {
            return Context{std::make_shared<detail::cancellation_state>(),
                           std::chrono::steady_clock::now() + std::chrono::duration_cast<std::chrono::milliseconds>(d)};
        }

        /// 子上下文: 继承父的取消关系; deadline 取父子中较早的那个。
        /// 父取消必然中断子, 子取消不影响父 —— 因此不新建状态, 只复用视图。
        Context make_child() const { return Context{state_, deadline_}; }

        /// 派生一个额外带期限的视图 (父不受影响)
        template <typename Rep, typename Period> Context with_deadline(std::chrono::duration<Rep, Period> d) const {
            const auto next =
                std::chrono::steady_clock::now() + std::chrono::duration_cast<std::chrono::milliseconds>(d);
            return Context{state_, earlier(deadline_, next)};
        }

        bool cancelled() const noexcept { return state_->cancelled.load(); }

        bool deadline_passed() const noexcept {
            return deadline_.has_value() && std::chrono::steady_clock::now() >= *deadline_;
        }

        std::optional<time_point> deadline() const noexcept { return deadline_; }

        CancellationToken cancellation() const { return CancellationToken{state_}; }

        /// 同步检查点: 用于长计算里主动跳出 (取消抛 CancelledError, 已超期抛 TimeoutError)
        void throw_if_cancelled() const {
            if (state_->cancelled.load())
                throw CancelledError();
            if (deadline_passed())
                throw TimeoutError();
        }

        /// 挂起直到"取消或超时": 取消抛 CancelledError, 超时抛 TimeoutError
        Task<void> wait() const {
            return detail::context_wait(detail::context_wait_awaiter{state_, deadline_, nullptr});
        }

      private:
        Context(std::shared_ptr<detail::cancellation_state> st, std::optional<time_point> dl)
            : state_(std::move(st)), deadline_(dl) {}

        static std::optional<time_point> earlier(std::optional<time_point> a, std::optional<time_point> b) {
            if (!a.has_value())
                return b;
            if (!b.has_value())
                return a;
            return *a < *b ? a : b;
        }

        std::shared_ptr<detail::cancellation_state> state_;
        std::optional<time_point> deadline_;
    };

} // namespace coro
