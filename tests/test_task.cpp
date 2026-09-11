// test_task.cpp — Task 基础: 惰性启动 / 返回值 / 移动 / 异常 / spawn
#include <gtest/gtest.h>

#include <coro/coro.hpp>

#include "test_util.h"

#include <string>

using namespace std::chrono_literals;

namespace
{

    // ── 命名协程函数 (参数进帧, MSVC Debug 安全) ──

    coro::Task<int> compute_42()
    {
        co_await coro::yield();
        co_return 42;
    }

    coro::Task<> flag_setter(bool *flag)
    {
        *flag = true;
        co_return;
    }

    coro::Task<> thrower()
    {
        throw std::runtime_error("boom");
        co_return; // MSVC Debug: throw 后必须补 co_return
    }

    coro::Task<> catch_from(std::string *out)
    {
        try
        {
            co_await thrower();
        }
        catch (const std::runtime_error &e)
        {
            *out = e.what();
        }
    }

    coro::Task<> spawn_two(int *a, int *b)
    {
        auto t1 = coro::spawn(compute_42());
        auto t2 = coro::spawn(compute_42());
        *a = co_await std::move(t1);
        *b = co_await std::move(t2);
    }

    coro::Task<> sleep_and_set(int ms, int *out)
    {
        co_await coro::sleep(std::chrono::milliseconds(ms));
        *out = 1;
    }

    coro::Task<> compute_and_store(int *out)
    {
        *out = co_await compute_42();
    }

    coro::Task<> try_await_thrower(bool *caught)
    {
        try
        {
            co_await thrower();
        }
        catch (const std::runtime_error &)
        {
            *caught = true;
        }
    }

    coro::Task<int> thrower_int()
    {
        throw std::runtime_error("run boom");
        co_return 0;
    }

} // namespace

// ── 惰性启动: 创建不执行 ──
TEST(TaskTest, LazyStart)
{
    bool flag = false;
    {
        auto t = flag_setter(&flag); // 创建即挂起, 协程体不执行
        EXPECT_FALSE(flag);
    } // 析构销毁未启动的帧
    EXPECT_FALSE(flag);
}

// ── co_await 返回结果 (串行等待) ──
TEST(TaskTest, CoAwaitResult)
{
    int out = -1;
    test_util::run_task([&]
                        { return compute_and_store(&out); });
    EXPECT_EQ(out, 42);
}

// ── spawn 并发 ──
TEST(TaskTest, SpawnConcurrent)
{
    int a = 0, b = 0;
    test_util::run_task([&]
                        { return spawn_two(&a, &b); });
    EXPECT_EQ(a, 42);
    EXPECT_EQ(b, 42);
}

// ── 异常跨协程传播 ──
TEST(TaskTest, ExceptionPropagation)
{
    std::string msg;
    test_util::run_task([&]
                        { return catch_from(&msg); });
    EXPECT_EQ(msg, "boom");
}

// ── 异常到达等待者 (未捕获时) ──
TEST(TaskTest, ExceptionReachesWaiter)
{
    bool caught = false;
    test_util::run_task([&]
                        { return try_await_thrower(&caught); });
    EXPECT_TRUE(caught);
}

// ── sleep 实际等待 (下限校验) ──
TEST(TaskTest, SleepActuallyWaits)
{
    auto t0 = std::chrono::steady_clock::now();
    int out = 0;
    test_util::run_task([&]
                        { return sleep_and_set(60, &out); });
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - t0)
                       .count();
    EXPECT_EQ(out, 1);
    EXPECT_GE(elapsed, 50); // 至少等了 50ms
}

// ── Task 移动语义 ──
TEST(TaskTest, MoveSemantics)
{
    auto t1 = compute_42();
    auto t2 = std::move(t1); // 所有权转移
    t2.start();
    coro::EventLoop::get().run();

    // t1 已空, 不崩溃
    EXPECT_FALSE(t1.is_started());
}

// ── Task<void> 特化 ──
TEST(TaskTest, VoidTask)
{
    bool done = false;
    test_util::run_task([&]() -> coro::Task<>
                        { return flag_setter(&done); });
    EXPECT_TRUE(done);
}

// ── coro::run 返回主协程结果 (对标 asyncio.run) ──
TEST(TaskTest, RunReturnsResult)
{
    int result = coro::run(compute_42()); // 直接拿结果
    EXPECT_EQ(result, 42);
}

TEST(TaskTest, RunVoidTask)
{
    bool flag = false;
    coro::run(flag_setter(&flag));
    EXPECT_TRUE(flag);
}

TEST(TaskTest, RunPropagatesException)
{
    bool caught = false;
    try
    {
        coro::run(thrower_int()); // 异常从 take_result 重新抛出
    }
    catch (const std::runtime_error &)
    {
        caught = true;
    }
    EXPECT_TRUE(caught);
}
