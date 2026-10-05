// test_signal.cpp — 信号事件: wait / handle / cancel / allow / disallow / notify
#if defined(_WIN32) || defined(__linux__)
#include <gtest/gtest.h>

#include <coro/coro.hpp>
#include <coro/signal.hpp>

#include "test_util.h"

#include <csignal>

using namespace std::chrono_literals;

namespace {

    // ── 共享协程辅助 ──

    // 单次等待: 挂起 → 信号到达 → 返回信号编号
    coro::Task<int> wait_one(int sig) {
        co_return co_await coro::signal::wait(sig);
    }

    // ── 核心等待: 一次 raise → 等待者收到正确信号号 ──

    coro::Task<> wait_basic_task(int sig, int* received) {
        auto t = coro::spawn(wait_one(sig));
        co_await coro::sleep(30ms); // 确保等待者已注册
        std::raise(sig);
        *received = co_await std::move(t);
    }

    // ── 多等待者: 一次投递唤醒全部 ──

    coro::Task<int> waiter(int sig, int id, std::vector<int>* order) {
        int s = co_await coro::signal::wait(sig);
        order->push_back(id);
        co_return s;
    }

    coro::Task<> wait_multi_task(int sig, int* woken, std::vector<int>* order) {
        // 同 loop 单线程, 无需 mutex
        auto a = coro::spawn(waiter(sig, 1, order));
        auto b = coro::spawn(waiter(sig, 2, order));
        auto c = coro::spawn(waiter(sig, 3, order));
        co_await coro::sleep(30ms);
        std::raise(sig);
        co_await std::move(a);
        co_await std::move(b);
        co_await std::move(c);
        *woken = (int)order->size();
    }

    // ── handle: 两次信号 → factory 执行两次 ──

    coro::Task<> handle_tick(int* counter) {
        ++(*counter);
        co_return;
    }

    coro::Task<> handle_task(int sig, int* fires) {
        int counter = 0;
        auto h = coro::signal::handle(sig, [&counter] { return handle_tick(&counter); });
        co_await coro::sleep(30ms); // 确保 handle 循环协程已挂起在 wait 上
        std::raise(sig);
        // 轮询等待, 避免固定 sleep 在慢 CI 上 flaky
        auto deadline = std::chrono::steady_clock::now() + 200ms;
        while (counter < 1 && std::chrono::steady_clock::now() < deadline)
            co_await coro::sleep(5ms);
        std::raise(sig);
        while (counter < 2 && std::chrono::steady_clock::now() < deadline)
            co_await coro::sleep(5ms);
        *fires = counter;
        (void)h; // RAII: 协程退出时析构注销
    }

    // ── 超时取消: wait_for 超时后等待者应被干净摘除 ──

    coro::Task<> wait_cancel_timeout_task(int sig, bool* timed_out) {
        auto t = coro::spawn(wait_one(sig));
        co_await coro::sleep(30ms);
        try {
            co_await coro::wait_for(std::move(t), 50ms);
            *timed_out = false;
        } catch (const coro::TimeoutError&) {
            *timed_out = true;
        }
    }

    // ── 自定义信号: allow → wait → notify ──

    coro::Task<> custom_notify_task(int sig, int* received) {
        auto t = coro::spawn(wait_one(sig));
        co_await coro::sleep(30ms);
        coro::signal::notify(sig);
        *received = co_await std::move(t);
    }

#ifndef _WIN32
    // Linux: 自定义实时信号经 raise() 真实信号路径到达
    coro::Task<> custom_raise_task(int sig, int* received) {
        auto t = coro::spawn(wait_one(sig));
        co_await coro::sleep(30ms);
        std::raise(sig);
        *received = co_await std::move(t);
    }
#endif

    // ── 有活跃等待者时 disallow 拒绝, 等待者仍能正常唤醒 ──

    coro::Task<> disallow_with_waiter_task(int sig, bool* blocked, int* received) {
        auto t = coro::spawn(wait_one(sig));
        co_await coro::sleep(30ms);
        *blocked = !coro::signal::disallow(sig); // 有等待者 → false
        coro::signal::notify(sig);               // 信号仍有效: 等待者正常唤醒
        *received = co_await std::move(t);
        coro::signal::disallow(sig); // 等待者退场后干净注销
    }

    // ── 未注册信号 wait 抛 invalid_argument ──

    coro::Task<> unallowed_wait_throws(int sig, bool* threw) {
        try {
            int s = co_await coro::signal::wait(sig);
            (void)s;
            *threw = false;
        } catch (const std::invalid_argument&) {
            *threw = true;
        }
    }

} // namespace

// ============================================================================
// 核心等待
// ============================================================================

TEST(SignalTest, WaitReceivesSignal) {
    int received = 0;
    test_util::run_task([&] { return wait_basic_task(SIGINT, &received); });
    EXPECT_EQ(received, SIGINT);
}

TEST(SignalTest, MultipleWaitersAllWoken) {
    int woken = 0;
    std::vector<int> order;
    test_util::run_task([&] { return wait_multi_task(SIGINT, &woken, &order); });
    EXPECT_EQ(woken, 3); // 一次投递 → 三个等待者全部唤醒
}

// ============================================================================
// handle: 持续处理
// ============================================================================

TEST(SignalTest, HandleFiresPerSignal) {
    int fires = 0;
    test_util::run_task([&] { return handle_task(SIGTERM, &fires); });
    EXPECT_EQ(fires, 2); // 两次 SIGTERM → handler 执行两次
}

// ============================================================================
// 超时取消
// ============================================================================

TEST(SignalTest, CancelledWaitIsClean) {
    bool timed_out = false;
    test_util::run_task([&] { return wait_cancel_timeout_task(SIGINT, &timed_out); });
    EXPECT_TRUE(timed_out);
    // 超时后旧等待者已被摘除; 再次 raise 不应崩溃或悬挂 (验证无 use-after-free)
    std::raise(SIGINT);
}

// ============================================================================
// 自定义信号注册: allow / disallow / notify
// ============================================================================
// 编号约定:
//   42     — allow 注册的自定义信号 (notify + raise 路径)
//   41     — allow/disallow 往返测试
//   43     — 全程未注册的负例探测号
// Linux 上 41/42 落在 SIGRTMIN..SIGRTMAX 实时信号区间, raise() 可达;
// Windows 上它们只是投递键, 只能经 notify() 唤醒。

TEST(SignalTest, AllowRegistersCustomSignal) {
    EXPECT_TRUE(coro::signal::allow(42));
    EXPECT_TRUE(coro::signal::allow(42)); // 幂等: 重复注册返回 true
    // 注册后 wait 能正常挂起 (不再抛 invalid_argument)
    int received = 0;
    test_util::run_task([&] { return custom_notify_task(42, &received); });
    EXPECT_EQ(received, 42);
}

TEST(SignalTest, AllowRejectsInvalidNumbers) {
    EXPECT_FALSE(coro::signal::allow(0));
    EXPECT_FALSE(coro::signal::allow(100)); // 超范围
#ifndef _WIN32
    EXPECT_FALSE(coro::signal::allow(SIGKILL)); // 不可捕获
    EXPECT_FALSE(coro::signal::allow(SIGSTOP)); // 不可捕获
    // 32/33 是 glibc 线程库保留号, 不在标准信号范围也不在实时信号区间
    EXPECT_FALSE(coro::signal::allow(32));
    EXPECT_FALSE(coro::signal::allow(33));
#endif
}

TEST(SignalTest, UnallowedWaitThrows) {
    bool threw = false;
    test_util::run_task([&] { return unallowed_wait_throws(43, &threw); });
    EXPECT_TRUE(threw);
}

TEST(SignalTest, CustomSignalViaNotify) {
    ASSERT_TRUE(coro::signal::allow(42));
    int received = 0;
    test_util::run_task([&] { return custom_notify_task(42, &received); });
    EXPECT_EQ(received, 42);
}

#ifndef _WIN32
TEST(SignalTest, CustomSignalViaRaise) {
    ASSERT_TRUE(coro::signal::allow(42));
    int received = 0;
    test_util::run_task([&] { return custom_raise_task(42, &received); });
    EXPECT_EQ(received, 42);
}
#endif

TEST(SignalTest, DisallowRoundTrip) {
    ASSERT_TRUE(coro::signal::allow(41));
    ASSERT_TRUE(coro::signal::disallow(41));  // 无等待者: 注销成功
    EXPECT_FALSE(coro::signal::disallow(41)); // 未注册: 返回 false
    // 注销后 wait 回到拒绝状态
    bool threw = false;
    test_util::run_task([&] { return unallowed_wait_throws(41, &threw); });
    EXPECT_TRUE(threw);
}

TEST(SignalTest, DisallowWithActiveWaiterFails) {
    ASSERT_TRUE(coro::signal::allow(41));
    bool blocked = false;
    int received = 0;
    test_util::run_task([&] { return disallow_with_waiter_task(41, &blocked, &received); });
    EXPECT_TRUE(blocked);    // disallow 被拒绝
    EXPECT_EQ(received, 41); // 等待者正常唤醒, 未被注销打断
}

#endif // _WIN32 || __linux__
