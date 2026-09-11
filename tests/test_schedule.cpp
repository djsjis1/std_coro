// test_schedule.cpp — call_soon / call_later / call_at
#include <gtest/gtest.h>

#include <coro/coro.hpp>

#include "test_util.h"

using namespace std::chrono_literals;

namespace
{

    // ── 命名协程函数 (共享状态用静态变量, 每个测试开头重置) ──

    int g_order = 0;
    bool g_coro_cb_done = false;
    int g_fired = 0;

    void plain_cb_append(int digit)
    {
        g_order = g_order * 10 + digit;
    }

    coro::Task<> coro_cb()
    {
        co_await coro::sleep(10ms);
        g_coro_cb_done = true;
        co_return;
    }

    coro::Task<> schedule_scenario()
    {
        g_order = 0;
        g_coro_cb_done = false;

        coro::call_soon([]
                        { plain_cb_append(1); });
        coro::call_later(30ms, []
                         { plain_cb_append(2); });
        coro::call_at(std::chrono::steady_clock::now() + 60ms,
                      []
                      { plain_cb_append(3); });
        coro::call_later(90ms, coro_cb); // 协程回调 (传函数本身)

        co_await coro::sleep(150ms); // 等所有回调执行完
    }

    // 时序校验: call_later(50ms) 不应在 30ms 前触发。
    // 注意: 事件循环会等待所有定时器完成 (活跃协程计数 > 0),
    //       所以要在协程内检查 30ms 时间点, 再继续等到 50ms 之后。
    coro::Task<> timing_scenario(int *at30ms)
    {
        g_fired = 0;
        coro::call_later(50ms, []
                         { g_fired = 1; });
        co_await coro::sleep(30ms);
        *at30ms = g_fired;          // 30ms 时检查: 应为 0
        co_await coro::sleep(40ms); // 等到 70ms, call_later 已触发
    }

} // namespace

TEST(ScheduleTest, CallOrderAndCoroutineCallback)
{
    test_util::run_task([]
                        { return schedule_scenario(); });
    EXPECT_EQ(g_order, 123);     // 普通回调按时间顺序执行 (1,2,3)
    EXPECT_TRUE(g_coro_cb_done); // 协程回调被执行 (第 4 步)
}

TEST(ScheduleTest, CallLaterNotEarly)
{
    int at30ms = -1;
    test_util::run_task([&]
                        { return timing_scenario(&at30ms); });
    EXPECT_EQ(at30ms, 0);  // 30ms 时尚未触发
    EXPECT_EQ(g_fired, 1); // 70ms 后已触发
}
