// test_rate_limit.cpp — coro::rate_limiter 的令牌桶合同
//
// 重点验三件事: 不超发、批量原子、等待者被取消时不消耗令牌。
#if defined(CORO_HAS_CONCURRENCY_EXT) && CORO_HAS_CONCURRENCY_EXT

#include <gtest/gtest.h>

#include <coro/coro.hpp>
#include <coro/rate_limit.hpp>

#include <atomic>
#include <chrono>
#include <optional>
#include <vector>

using namespace std::chrono_literals;

namespace {

    coro::Task<> grab(coro::rate_limiter* rl, std::atomic<int>* granted) {
        co_await rl->acquire();
        granted->fetch_add(1);
        co_return;
    }

    coro::Task<> grab_n(coro::rate_limiter* rl, int n, std::atomic<int>* granted) {
        try {
            co_await rl->acquire_n(static_cast<std::uint64_t>(n));
        } catch (const coro::CancelledError&) {
            // 用例里会主动取消这个等待者: 接住它, 否则异常会抛穿测试体
            co_return;
        }
        granted->fetch_add(1);
        co_return;
    }

    /// 等令牌, 把"是否以 CancelledError 退出"记下来。命名协程 —— 不能用临时
    /// lambda 协程, 它的闭包在全表达式结束就销毁, 恢复时读到的就是悬空捕获。
    coro::Task<> acquire_and_report(coro::rate_limiter* rl, std::atomic<bool>* threw_cancel) {
        try {
            co_await rl->acquire();
        } catch (const coro::CancelledError&) {
            threw_cancel->store(true);
        }
        co_return;
    }

    /// 挂起等令牌, 但中途被取消 (取消投递回 owner loop 执行, 不跨线程直接 cancel)
    coro::Task<> cancelled_waiter(coro::rate_limiter* rl, std::atomic<bool>* threw_cancel) {
        auto held = std::make_shared<coro::Task<>>(coro::spawn(acquire_and_report(rl, threw_cancel)));
        co_await coro::yield();
        auto* loop = &coro::EventLoop::get();
        loop->dispatch([held] { held->cancel(); });
        co_await coro::sleep(60ms);
        co_return;
    }

} // namespace

TEST(RateLimitTest, FullBucketAllowsBurstThenRejects) {
    coro::rate_limiter rl(3, 1s);
    EXPECT_TRUE(rl.try_acquire());
    EXPECT_TRUE(rl.try_acquire());
    EXPECT_TRUE(rl.try_acquire());
    EXPECT_FALSE(rl.try_acquire()) << "容量 3 的桶不该放行第 4 个";
    EXPECT_EQ(rl.available(), 0u);
}

TEST(RateLimitTest, RefillRestoresTokensOverTime) {
    coro::rate_limiter rl(1, 40ms);
    EXPECT_TRUE(rl.try_acquire());
    EXPECT_FALSE(rl.try_acquire());
    bool second = false;
    coro::run([&]() -> coro::Task<> {
        co_await coro::sleep(90ms);
        second = rl.try_acquire();
    }());
    EXPECT_TRUE(second) << "两个周期后应已补充";
}

TEST(RateLimitTest, AcquireWaitsInsteadOfFailing) {
    coro::rate_limiter rl(1, 50ms);
    ASSERT_TRUE(rl.try_acquire()); // 先清空, 让 acquire 必须等
    auto start = std::chrono::steady_clock::now();
    std::atomic<int> granted{0};
    coro::run([&]() -> coro::Task<> {
        auto w = coro::spawn(grab(&rl, &granted));
        co_await std::move(w);
    }());
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start);
    EXPECT_EQ(granted.load(), 1);
    EXPECT_GE(elapsed.count(), 40) << "acquire 必须真的等到补充, 不能立刻放行";
}

TEST(RateLimitTest, BatchAcquireIsAllOrNothing) {
    coro::rate_limiter rl(5, 1s);
    (void)rl.try_acquire(4); // 只剩 1 个
    const std::uint64_t before = rl.available();
    std::atomic<int> granted{0};
    coro::run([&]() -> coro::Task<> {
        auto w = coro::spawn(grab_n(&rl, 3, &granted));
        co_await coro::sleep(30ms);
        EXPECT_EQ(granted.load(), 0) << "只有 1 个令牌时不得部分扣减";
        EXPECT_EQ(rl.available(), before) << "等待期间存量不能被吃掉一半";
        w.cancel();
        co_await std::move(w);
    }());
}

TEST(RateLimitTest, ManyWaitersNeverOverissue) {
    // 容量 5 / 每 30ms 补 5: 20 个等待者要全部拿到, 至少得经历 3 轮补充
    coro::rate_limiter rl(5, 30ms);
    std::atomic<int> granted{0};
    auto start = std::chrono::steady_clock::now();
    coro::run([&]() -> coro::Task<> {
        std::vector<coro::Task<>> workers;
        for (int i = 0; i < 20; ++i)
            workers.push_back(coro::spawn(grab(&rl, &granted)));
        for (auto& w : workers)
            co_await std::move(w);
    }());
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start);
    EXPECT_EQ(granted.load(), 20) << "每个等待者最终都该拿到令牌";
    // 初始 5 个 + 每 30ms 5 个 → 15 个额外至少需 3 个周期 = 90ms
    EXPECT_GE(elapsed.count(), 60) << "耗时明显小于限流下界说明发生了超发";
    EXPECT_NE(rl.available(), static_cast<std::uint64_t>(-1)) << "令牌数不应下溢";
}

TEST(RateLimitTest, CancelledWaiterConsumesNothing) {
    // 周期取得远长于观测窗口: 否则等待期间桶自己补充了, "取消没消耗令牌"就测不出来
    coro::rate_limiter rl(2, 10s);
    ASSERT_TRUE(rl.try_acquire(2)); // 清空
    std::atomic<bool> threw{false};
    const std::uint64_t before = rl.available();
    coro::run([&]() -> coro::Task<> {
        auto w = coro::spawn(cancelled_waiter(&rl, &threw));
        co_await std::move(w);
    }());
    EXPECT_TRUE(threw.load()) << "取消必须以 CancelledError 浮现";
    EXPECT_EQ(rl.available(), before) << "被取消的等待者不能吃掉令牌";
}

TEST(RateLimitTest, RetryAfterIsZeroWhenAvailable) {
    coro::rate_limiter rl(2, 500ms);
    EXPECT_EQ(rl.retry_after<std::chrono::milliseconds>(1).count(), 0);
    (void)rl.try_acquire(2);
    EXPECT_GT(rl.retry_after<std::chrono::milliseconds>(1).count(), 0) << "空桶要报出可等待时长";
}

TEST(RateLimitTest, SetRateClampsExistingTokens) {
    coro::rate_limiter rl(100, 1s);
    EXPECT_EQ(rl.available(), 100u);
    rl.set_rate(10, 1s); // 调小容量
    EXPECT_LE(rl.available(), 10u) << "调小容量后存量必须被截断, 否则要空转很久才收敛";
}

#endif // CORO_HAS_CONCURRENCY_EXT
