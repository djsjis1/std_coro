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

        /// 一次挂起等待的登记记录。outcome 是「先到终态」的仲裁单元:
        /// 0=pending, 1=cancelled, 2=timeout。取消与到期都只能 CAS 成功一次,
        /// 因此恢复时看到的原因就是真正先到的那一个, 不会在 await_resume 里
        /// 重新看共享布尔而被后到事件翻转。
        struct cancel_wait_record {
            std::coroutine_handle<> handle{};
            EventLoop* loop = nullptr;
            std::shared_ptr<std::atomic<int>> outcome;
        };

        /// 终态取值 (与 cancel_wait_record::outcome 一致)
        enum class cancel_outcome : int { pending = 0, cancelled = 1, timeout = 2 };

        /// 取消状态: source 与所有 token/等待者共享。生命周期由 shared_ptr 保证,
        /// 因此 source 先于子协程析构也不会悬空。
        struct cancellation_state {
            std::mutex mutex;
            std::atomic<bool> cancelled{false};
            std::vector<cancel_wait_record> waiters;
            // 屏蔽作用域计数与延后标记都由 mutex 保护 (与 waiters 同一把锁):
            // 把它们放进 state 而不是放进协程帧, 是因为"当前哪个协程在跑"没法在
            // 唤醒/取消路径上可靠得知 —— 屏蔽的是**这个源**, 作用域由 RAII 对象决定。
            int shield_depth = 0;
            bool deferred_cancel = false;

            /// 终态单选: 只有第一次 cancel 返回 true
            bool claim_cancel() noexcept {
                bool expected = false;
                return cancelled.compare_exchange_strong(expected, true);
            }

            /// 带屏蔽判断的申领: 处于 shield 作用域内时**只记账不生效**, 返回 false
            /// 表示"这次取消被延后", 调用方因此既不置 cancelled 也不唤醒等待者。
            bool claim_cancel_shielded() {
                std::lock_guard lock(mutex);
                if (shield_depth > 0) {
                    deferred_cancel = true;
                    return false;
                }
                if (cancelled.load(std::memory_order_acquire))
                    return false;
                cancelled.store(true, std::memory_order_release);
                return true;
            }

            void shield_enter() {
                std::lock_guard lock(mutex);
                ++shield_depth;
            }

            /// 返回 true = 最后一个屏蔽作用域退出且期间有过取消请求, 调用方应当立即
            /// 把它落实 (置终态 + 唤醒等待者)。
            bool shield_exit() {
                std::lock_guard lock(mutex);
                if (shield_depth > 0)
                    --shield_depth;
                if (shield_depth == 0 && deferred_cancel) {
                    deferred_cancel = false;
                    bool expected = false;
                    return cancelled.compare_exchange_strong(expected, true);
                }
                return false;
            }

            /// 摘一个等待者交给调用方唤醒 (每个等待者至多被摘走一次)
            bool take_waiter(cancel_wait_record& out) {
                std::lock_guard lock(mutex);
                if (waiters.empty())
                    return false;
                out = waiters.back();
                waiters.pop_back();
                return true;
            }

            void add_waiter(std::coroutine_handle<> h, EventLoop* loop, std::shared_ptr<std::atomic<int>> outcome) {
                std::lock_guard lock(mutex);
                waiters.push_back(cancel_wait_record{h, loop, std::move(outcome)});
            }

            /// 等待者帧即将销毁 (被取消/提前退出): 摘链, 防止 cancel 唤醒野句柄
            void drop_waiter(std::coroutine_handle<> h) {
                std::lock_guard lock(mutex);
                for (auto it = waiters.begin(); it != waiters.end(); ++it) {
                    if (it->handle == h) {
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
            /// 本次等待的终态仲裁单元 (取消/超时共享同一份 shared_ptr)
            std::shared_ptr<std::atomic<int>> outcome = std::make_shared<std::atomic<int>>(0);
            std::coroutine_handle<> registered{}; // add_waiter 用的句柄 (摘链依据)

            bool await_ready() const noexcept { return st->cancelled.load(); }

            void await_suspend(std::coroutine_handle<> h) {
                auto& loop = EventLoop::get();
                registered = h;
                st->add_waiter(h, &loop, outcome);
                if (st->cancelled.load()) {
                    // 竞态窗口: 检查之后、挂链之前发生了 cancel, 对方已不再能看到我们
                    retire();
                    loop.schedule(h);
                    return;
                }
                if (deadline.has_value()) {
                    timer_token = std::make_shared<std::atomic<bool>>(false);
                    // expire_value=2(timeout): 到期入队时与取消竞争唯一终态
                    loop.schedule_timer(h, *deadline, timer_token, outcome, static_cast<int>(cancel_outcome::timeout));
                }
            }

            void await_resume() {
                retire(); // 先摘链并作废定时器条目, 再判定原因
                switch (static_cast<cancel_outcome>(outcome->load())) {
                    case cancel_outcome::cancelled:
                        throw CancelledError();
                    case cancel_outcome::timeout:
                        throw TimeoutError();
                    case cancel_outcome::pending:
                        break;
                }
                // 没经过仲裁单元就恢复了 (例如已被其它机制唤醒): 按共享状态兜底判定
                if (st->cancelled.load())
                    throw CancelledError();
                throw TimeoutError();
            }

            /// 本次等待的统一退役入口: 作废定时器堆条目 + 从 source 等待链摘除。
            /// 必须同时做两件事 —— 只摘链会让远期 deadline 继续把事件循环拖到原期限
            /// (has_work 看到 timer_heap_ 非空), 表现为“取消后程序迟迟不退出”。
            /// Task 取消会绕过 await_resume 直接销毁帧, 因此析构也必须走这里。
            void retire() {
                if (timer_token) {
                    *timer_token = true;
                    timer_token.reset();
                }
                if (registered) {
                    st->drop_waiter(registered);
                    registered = {};
                }
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
            // 默认构造/移出的 token 没有共享状态, 约定为“已取消”: 等待立即完成。
            // 若在这里继续构造 awaiter, await_ready 会解引用空 state_。
            if (!st)
                co_return;
            try {
                co_await context_wait_awaiter{std::move(st), std::nullopt, nullptr};
            } catch (const CancelledError&) {
                // 取消就是等待的答案
            }
            co_return;
        }

    } // namespace detail

    class cancellation_shield;

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

        /// 发起取消: 非阻塞, 任意线程可调, 重复调用无副作用。
        /// 若当前有 cancellation_shield 作用域生效, 取消会被延后到最后一个作用域退出。
        void cancel() const {
            if (!state_->claim_cancel_shielded())
                return;
            wake_waiters();
        }

        /// 把全部等待者摘走并投递回各自的 EventLoop 唤醒 (取消只发生一次, 摘链即消费)
        void wake_waiters() const {
            detail::cancel_wait_record waiter{};
            while (state_->take_waiter(waiter)) {
                if (waiter.loop == nullptr)
                    continue;
                if (waiter.outcome) {
                    // 先到终态: 已由超时定下结果的等待者不再被“取消”覆盖。
                    // 同时避免对同一帧重复入队 (它可能已经在 ready 队列里)。
                    int expected = static_cast<int>(detail::cancel_outcome::pending);
                    if (!waiter.outcome->compare_exchange_strong(expected,
                                                                 static_cast<int>(detail::cancel_outcome::cancelled)))
                        continue;
                }
                waiter.loop->schedule(waiter.handle);
            }
        }

        /// 进入屏蔽作用域: 作用域存续期间本源的 cancel() 不产生终态, 退出时补发。
        /// 用途是"清理/提交这类步骤不能被取消打断"。嵌套支持, 计数归零才生效。
        cancellation_shield make_shield() const;

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

        /// 从取消源的令牌派生; 不传令牌时相当于根上下文。
        ///
        /// 注意: 默认构造的 CancellationToken (无 owner) 经 token.cancelled()
        /// 报告"已取消" (保守取向: 宁可取消), 而这里为它派生的是无人能取消的
        /// 状态 —— 同一个令牌经两条路径得到相反结论。Context 需要可传递的
        /// 取消链, 只能选择"永不取消"; 需要立即取消的调用方应显式使用
        /// CancellationSource 或已取消的上下文。
        static Context from(CancellationToken token, std::optional<time_point> deadline = std::nullopt) {
            auto st = token.state_ ? token.state_ : std::make_shared<detail::cancellation_state>();
            return Context{std::move(st), deadline};
        }

        static Context root() { return Context{}; }

        /// 只带期限、不带取消源的上下文
        template <typename Rep, typename Period> static Context with_timeout(std::chrono::duration<Rep, Period> d) {
            return Context{std::make_shared<detail::cancellation_state>(),
                           std::chrono::steady_clock::now() + std::chrono::ceil<std::chrono::milliseconds>(d)};
        }

        /// 子上下文: 继承父的取消关系; deadline 取父子中较早的那个。
        /// 父取消必然中断子, 子取消不影响父 —— 因此不新建状态, 只复用视图。
        Context make_child() const { return Context{state_, deadline_}; }

        /// 派生一个额外带期限的视图 (父不受影响)
        template <typename Rep, typename Period> Context with_deadline(std::chrono::duration<Rep, Period> d) const {
            const auto next = std::chrono::steady_clock::now() + std::chrono::ceil<std::chrono::milliseconds>(d);
            return Context{state_, earlier(deadline_, next)};
        }

        /// 绝对期限视图: 取与现有期限中较早的一个。
        /// 嵌套调用应传同一个 deadline —— 逐层传相对时长会把总时限不断延长。
        Context with_deadline_at(time_point at) const { return Context{state_, earlier(deadline_, at)}; }

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

    /// 取消屏蔽作用域: 存续期间所属 CancellationSource 的 cancel() 被延后, 而不是
    /// 立刻在下一个 await 点抛 CancelledError。不可拷贝也不可移动 —— 它就是作用域本身,
    /// 复制会让"退出时机"变得含混。
    class cancellation_shield {
      public:
        cancellation_shield(const cancellation_shield&) = delete;
        cancellation_shield& operator=(const cancellation_shield&) = delete;

        /// 允许移动: 构造函数只对 CancellationSource 开放, 调用方需要它能把它存进
        /// 自己的变量/容器。移出后本作用域立即失效 (析构不再补发), 语义仍然单点。
        cancellation_shield(cancellation_shield&& other) noexcept : state_(std::move(other.state_)) {}
        cancellation_shield& operator=(cancellation_shield&&) = delete;

        /// 析构: 若是最后一层且期间有取消请求, 立即补发 (置终态 + 唤醒等待者)
        ~cancellation_shield() {
            if (!state_ || !state_->shield_exit())
                return;
            detail::cancel_wait_record waiter{};
            while (state_->take_waiter(waiter)) {
                if (waiter.loop == nullptr)
                    continue;
                if (waiter.outcome) {
                    int expected = static_cast<int>(detail::cancel_outcome::pending);
                    if (!waiter.outcome->compare_exchange_strong(expected,
                                                                 static_cast<int>(detail::cancel_outcome::cancelled)))
                        continue;
                }
                waiter.loop->schedule(waiter.handle);
            }
        }

        /// 当前是否仍处于被屏蔽的状态 (供需要自我约束的代码查询)
        bool active() const noexcept { return static_cast<bool>(state_); }

      private:
        friend class CancellationSource;
        explicit cancellation_shield(std::shared_ptr<detail::cancellation_state> st) : state_(std::move(st)) {
            if (state_)
                state_->shield_enter();
        }
        std::shared_ptr<detail::cancellation_state> state_;
    };

    inline cancellation_shield CancellationSource::make_shield() const {
        return cancellation_shield{state_};
    }

} // namespace coro
