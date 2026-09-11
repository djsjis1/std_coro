// test_thread.cpp — to_thread: 线程池桥接阻塞函数 / 异常传播 / 并发
#include <gtest/gtest.h>

#include <coro/coro.hpp>

#include "test_util.h"

#include <atomic>
#include <thread>

using namespace std::chrono_literals;

namespace
{

    // ── 命名协程函数 ──

    coro::Task<> single_blocking(int *out)
    {
        // 阻塞函数在工作线程执行, 事件循环线程不阻塞
        *out = co_await coro::to_thread([]
                                        {
            std::this_thread::sleep_for(20ms);
            return 42; });
    }

    coro::Task<> void_blocking(bool *done)
    {
        co_await coro::to_thread([]
                                 { std::this_thread::sleep_for(10ms); });
        *done = true;
    }

    coro::Task<> exception_from_thread(bool *caught)
    {
        try
        {
            co_await coro::to_thread([]() -> int
                                     {
                throw std::runtime_error("thread boom");
                return 0; });
        }
        catch (const std::runtime_error &e)
        {
            *caught = (std::string(e.what()) == "thread boom");
        }
    }

    // 并发: 多个 to_thread 同时跑 (gather 组合)
    coro::Task<> concurrent_threads(int *sum)
    {
        auto [a, b, c] = co_await coro::gather(
            coro::to_thread([]
                            {
                std::this_thread::sleep_for(30ms);
                return 10; }),
            coro::to_thread([]
                            {
                std::this_thread::sleep_for(10ms);
                return 20; }),
            coro::to_thread([]
                            {
                std::this_thread::sleep_for(20ms);
                return 30; }));
        *sum = a + b + c;
    }

    // 事件循环在等待 to_thread 期间仍能处理其他协程
    coro::Task<> loop_keeps_running(bool *tick_ran, int *result)
    {
        auto slow = coro::spawn(coro::to_thread([]
                                                {
            std::this_thread::sleep_for(30ms);
            return 7; }));
        // 等 to_thread 期间, 事件循环处理这个定时器:
        coro::call_later(10ms, [tick_ran]
                         { *tick_ran = true; });
        *result = co_await std::move(slow);
    }

} // namespace

TEST(ThreadTest, ToThreadReturnsResult)
{
    int out = 0;
    test_util::run_task([&]
                        { return single_blocking(&out); });
    EXPECT_EQ(out, 42);
}

TEST(ThreadTest, ToThreadVoid)
{
    bool done = false;
    test_util::run_task([&]
                        { return void_blocking(&done); });
    EXPECT_TRUE(done);
}

TEST(ThreadTest, ExceptionPropagatesAcrossThreads)
{
    bool caught = false;
    test_util::run_task([&]
                        { return exception_from_thread(&caught); });
    EXPECT_TRUE(caught);
}

TEST(ThreadTest, ConcurrentToThreadWithGather)
{
    int sum = 0;
    test_util::run_task([&]
                        { return concurrent_threads(&sum); });
    EXPECT_EQ(sum, 60);
}

TEST(ThreadTest, EventLoopKeepsRunningWhileWaiting)
{
    bool tick_ran = false;
    int result = 0;
    test_util::run_task([&]
                        { return loop_keeps_running(&tick_ran, &result); });
    EXPECT_TRUE(tick_ran); // 事件循环未被阻塞
    EXPECT_EQ(result, 7);
}
