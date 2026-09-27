#pragma once

#include "event_loop.hpp"
#include "exceptions.hpp"
#include "sleep.hpp"
#include "task.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>

// ============================================================================
// coro::rate_limiter — 令牌桶限流 (计划 M3c)
// ============================================================================
//
// 典型用法:
//   coro::rate_limiter rl(100, std::chrono::seconds(1)); // 每 1s 补满 100 个令牌
//   co_await rl.acquire();          // 没令牌就等到有
//   if (!rl.try_acquire()) 走降级分支;   // 非阻塞, 用于"能省则省"
//
// 三条明确的设计取舍:
//   1. 令牌数用**定点整数**(每令牌 1024 份)而不是 double: double 无法原子 CAS,
//      而"检查+扣减"必须是单条原子操作, 否则并发 acquire 会超发。
//   2. 等待靠 sleep 到"下一个补充时刻", 不自建等待队列: 队列要处理取消摘链、
//      帧销毁、惊群唤醒三件事, 而 sleep 的 token 作废机制已经把这些解决了。
//      代价是醒来者之间靠 CAS 抢令牌, 抢输的多睡一轮 —— 有界延迟, 无死锁, 无泄漏。
//   3. **不保证 FIFO 公平性**: 只保证不超发、不永久饿死 (桶持续补充)。需要严格
//      排队语义的场景请用 Semaphore 手工限并发。
//
// loop-local: 与 channel 一致, 只在创建它的 EventLoop 线程内使用。
// ============================================================================

namespace coro {
    namespace detail {

        /// 定点数刻度: 1 个令牌 = RATE_TOKEN_SCALE 份。补充速率可以是"每秒 0.5 个"
        /// 这种小数, 用定点表示就不需要浮点原子量。
        inline constexpr std::int64_t rate_token_scale = 1024;

        /// 桶的共享状态。放堆上并由 shared_ptr 持有: 等待中的协程可能比 rate_limiter
        /// 对象活得久 (与 Timer/Channel 同一理由 —— 绝不能让协程帧捕获 this)。
        struct rate_state {
            std::atomic<std::int64_t> tokens{0}; // 当前可用份数 (定点)
            std::atomic<std::int64_t> capacity{0};
            std::atomic<std::int64_t> refill{0};       // 每个 refill_period 补充的份数
            std::atomic<std::int64_t> period_us{1000}; // 补充周期 (微秒)
            std::atomic<std::int64_t> last_us{0};      // 上次结算时刻 (epoch 微秒)
            EventLoop* owner = nullptr;                // loop-local, 见文件头

            static std::int64_t now_us() {
                return std::chrono::duration_cast<std::chrono::microseconds>(
                           std::chrono::steady_clock::now().time_since_epoch())
                    .count();
            }

            /// 按经过的时间结算并补充令牌, 返回结算后的可用份数。
            /// 用 CAS 循环保证"结算 + 扣减"整体线性化; 并发调用最坏多转一圈。
            std::int64_t settle_and_peek() {
                for (;;) {
                    const std::int64_t old_tokens = tokens.load(std::memory_order_relaxed);
                    const std::int64_t old_last = last_us.load(std::memory_order_relaxed);
                    const std::int64_t now = now_us();
                    const std::int64_t cap = capacity.load(std::memory_order_relaxed);
                    const std::int64_t per = refill.load(std::memory_order_relaxed);
                    const std::int64_t period = period_us.load(std::memory_order_relaxed);
                    std::int64_t next = old_tokens;
                    std::int64_t next_last = old_last;
                    if (per > 0 && period > 0 && now > old_last) {
                        const std::int64_t periods = (now - old_last) / period;
                        if (periods > 0) {
                            // 一次跳多个周期时直接按"经过时间 × 速率"补, 避免循环累加;
                            // 上限截到 capacity, 防止长期空闲后攒出一大桶突发。
                            const long double grown =
                                static_cast<long double>(old_tokens) + static_cast<long double>(per * periods);
                            next = grown >= static_cast<long double>(cap) ? cap : static_cast<std::int64_t>(grown);
                            next_last = old_last + periods * period;
                        }
                    }
                    std::int64_t expected_tokens = old_tokens;
                    if (tokens.compare_exchange_weak(expected_tokens, next, std::memory_order_acq_rel)) {
                        last_us.store(next_last, std::memory_order_relaxed);
                        return next;
                    }
                    // 别的线程刚改过 tokens: 重读一遍再结算 (last_us 可能已被更新)
                }
            }

            /// 尝试扣减 need 份 (已结算)。不够就不扣, 返回 false。
            bool try_consume(std::int64_t need) {
                for (;;) {
                    const std::int64_t have = settle_and_peek();
                    if (have < need)
                        return false;
                    std::int64_t expected = have;
                    if (tokens.compare_exchange_weak(expected, have - need, std::memory_order_acq_rel))
                        return true;
                }
            }

            /// 距离"够扣 need 份"还要多久 (微秒)。桶此刻已够则返回 0。
            std::int64_t wait_us_for(std::int64_t need) {
                for (;;) {
                    const std::int64_t have = settle_and_peek();
                    if (have >= need)
                        return 0;
                    const std::int64_t per = refill.load(std::memory_order_relaxed);
                    const std::int64_t period = period_us.load(std::memory_order_relaxed);
                    if (per <= 0 || period <= 0)
                        return -1; // 永不补充: 调用方按错误处理
                    const std::int64_t missing = need - have;
                    const std::int64_t periods = (missing + per - 1) / per;
                    return periods * period;
                }
            }
        };

        /// 等待并扣减 need 份。抢输的对手多睡一轮再试: 有界延迟、无死锁、无泄漏。
        inline Task<> rate_acquire(std::shared_ptr<rate_state> st, std::int64_t need) {
            for (;;) {
                if (st->try_consume(need))
                    co_return;
                const std::int64_t wait = st->wait_us_for(need);
                if (wait < 0)
                    throw StructuredConcurrencyError("rate_limiter: this bucket never refills");
                co_await sleep(std::chrono::microseconds(wait == 0 ? 1 : wait));
            }
        }

    } // namespace detail

    /// 令牌桶限流器。可拷贝 (多个拷贝共享同一个桶), 与 channel 的句柄语义一致。
    class rate_limiter {
      public:
        /// capacity: 桶容量 (允许的突发量); refill: 每个 period 补充的令牌数。
        /// 允许 refill > capacity (等价于"每周期清空重填"), 也允许小于 1 的速率
        /// (例如 refill=1, period=2s 表示每 2 秒 1 个)。
        template <typename Rep, typename Period>
        explicit rate_limiter(std::uint64_t capacity, std::chrono::duration<Rep, Period> period,
                              std::uint64_t refill_per_period = 0)
            : state_(std::make_shared<detail::rate_state>()) {
            using namespace std::chrono;
            const std::int64_t period_us = duration_cast<microseconds>(period).count();
            const std::int64_t cap = static_cast<std::int64_t>(capacity) * detail::rate_token_scale;
            const std::int64_t per = static_cast<std::int64_t>(refill_per_period == 0 ? capacity : refill_per_period) *
                                     detail::rate_token_scale;
            state_->capacity.store(cap);
            state_->refill.store(per);
            state_->period_us.store(period_us < 1 ? 1 : period_us);
            state_->tokens.store(cap); // 初始满桶: 冷启动不该先被限流
            state_->last_us.store(detail::rate_state::now_us());
            state_->owner = &EventLoop::get();
        }

        /// 取 1 个令牌, 不足则挂起到有。取消会照常抛 CancelledError。
        /// 本函数**不是协程**: 挂起发生在 detail::rate_acquire 里, 帧只捕获
        /// shared_ptr<rate_state>。写成成员协程会让帧捕获 this, rate_limiter 是
        /// 可拷贝的小句柄, 极易比等待者先销毁 (Timer/Channel 上都踩过)。
        Task<> acquire() const { return detail::rate_acquire(state_, detail::rate_token_scale); }

        /// 取 n 个令牌 (原子: 要么一次拿到 n 个, 要么一个都不拿)。
        Task<> acquire_n(std::uint64_t n) const {
            return detail::rate_acquire(state_, static_cast<std::int64_t>(n) * detail::rate_token_scale);
        }

        /// 非阻塞: 有就扣, 没有立即返回 false (用于降级/快速失败路径)。
        bool try_acquire(std::uint64_t n = 1) const {
            return state_->try_consume(static_cast<std::int64_t>(n) * detail::rate_token_scale);
        }

        /// 当前可用令牌数 (向下取整)。只用于观测与展示, 不要拿它做判断后再扣 ——
        /// 那之间总有竞态, 需要原子判定就用 try_acquire。
        std::uint64_t available() const {
            return static_cast<std::uint64_t>(state_->settle_and_peek() / detail::rate_token_scale);
        }

        /// 距下次能取 n 个令牌还有多久; n 已可满足时返回 0。供 429 Retry-After 之类用。
        /// 模板参数直接是目标 duration 类型 (调用处写 retry_after<std::chrono::milliseconds>(1))
        template <typename Duration> Duration retry_after(std::uint64_t n = 1) const {
            const std::int64_t us = state_->wait_us_for(static_cast<std::int64_t>(n) * detail::rate_token_scale);
            return std::chrono::duration_cast<Duration>(std::chrono::microseconds(us < 0 ? 0 : us));
        }

        /// 运行时改速率 (例如按订阅档位热更新)。桶里已有的令牌不重置。
        template <typename Rep, typename Period>
        void set_rate(std::uint64_t capacity, std::chrono::duration<Rep, Period> period,
                      std::uint64_t refill_per_period = 0) const {
            using namespace std::chrono;
            const std::int64_t period_us = duration_cast<microseconds>(period).count();
            state_->capacity.store(static_cast<std::int64_t>(capacity) * detail::rate_token_scale);
            state_->refill.store(static_cast<std::int64_t>(refill_per_period == 0 ? capacity : refill_per_period) *
                                 detail::rate_token_scale);
            state_->period_us.store(period_us < 1 ? 1 : period_us);
            // 若调小后当前存量超过新容量, 截一下, 否则要空转很久才收敛
            std::int64_t expected = state_->tokens.load();
            const std::int64_t cap = state_->capacity.load();
            if (expected > cap)
                state_->tokens.compare_exchange_strong(expected, cap);
        }

      private:
        std::shared_ptr<detail::rate_state> state_;
    };

} // namespace coro
