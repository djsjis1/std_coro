// test_concurrency.cpp — gather / gather_all / gather_void / wait_for / wait_any
#include <gtest/gtest.h>

#include <coro/coro.hpp>

#include "test_util.h"

#include <string>
#include <vector>

using namespace std::chrono_literals;

namespace
{

    // ── 命名协程函数 ──

    coro::Task<std::string> fetch_name()
    {
        co_await coro::sleep(20ms);
        co_return std::string("alice");
    }

    coro::Task<int> fetch_count()
    {
        co_await coro::sleep(10ms);
        co_return 42;
    }

    coro::Task<> gather_two(std::string *name, int *count)
    {
        auto [s, n] = co_await coro::gather(fetch_name(), fetch_count());
        *name = s;
        *count = n;
    }

    coro::Task<int> make_num(int i)
    {
        co_await coro::sleep(5ms);
        co_return i * 10;
    }

    coro::Task<> gather_dynamic(std::vector<int> *out)
    {
        std::vector<coro::Task<int>> tv;
        for (int i = 0; i < 4; ++i)
            tv.push_back(make_num(i));
        *out = co_await coro::gather_all(std::move(tv));
    }

    coro::Task<int> fail_after_ms(int ms)
    {
        co_await coro::sleep(std::chrono::milliseconds(ms));
        throw std::runtime_error("sub task failed");
        co_return 0;
    }

    // gather 异常: 第一个异常传播, 其余任务继续完成
    coro::Task<> gather_with_exception(std::string *msg, int *completed)
    {
        auto slow = coro::spawn(fetch_count()); // 正常任务 (10ms)
        try
        {
            co_await coro::gather(fetch_name(), fail_after_ms(5));
        }
        catch (const std::runtime_error &e)
        {
            *msg = e.what();
        }
        *completed = co_await std::move(slow); // 其他任务不受影响
    }

    coro::Task<> tiny()
    {
        co_await coro::sleep(5ms);
        co_return;
    }

    coro::Task<> long_sleep_void()
    {
        co_await coro::sleep(10s);
        co_return;
    }

    coro::Task<> gather_void_ok(int *sum)
    {
        auto a = coro::spawn(tiny());
        auto b = coro::spawn(tiny());
        co_await coro::gather_void(tiny());
        co_await std::move(a);
        co_await std::move(b);
        *sum = 3;
    }

    // gather_void 异常传播 (本轮修复的回归测试)
    coro::Task<> fail_void_after_ms(int ms)
    {
        co_await coro::sleep(std::chrono::milliseconds(ms));
        throw std::runtime_error("void sub failed");
        co_return;
    }

    coro::Task<> gather_void_exception(bool *caught)
    {
        try
        {
            co_await coro::gather_void(tiny(), fail_void_after_ms(10));
        }
        catch (const std::runtime_error &)
        {
            *caught = true;
        }
    }

    coro::Task<int> slow_count()
    {
        co_await coro::sleep(200ms);
        co_return 1;
    }

    coro::Task<> wait_for_timeout(bool *caught)
    {
        try
        {
            co_await coro::wait_for(slow_count(), 30ms); // 200ms 任务, 30ms 超时
        }
        catch (const coro::TimeoutError &)
        {
            *caught = true;
        }
    }

    coro::Task<> wait_for_ok(int *result)
    {
        *result = co_await coro::wait_for(fetch_count(), 1s);
    }

    coro::Task<> wait_for_void_timeout(bool *caught)
    {
        try
        {
            co_await coro::wait_for(long_sleep_void(), 30ms);
        }
        catch (const coro::TimeoutError &)
        {
            *caught = true;
        }
    }

    coro::Task<int> fast_num(int v, int ms)
    {
        co_await coro::sleep(std::chrono::milliseconds(ms));
        co_return v;
    }

    coro::Task<> wait_any_fast(int *result)
    {
        *result = co_await coro::wait_any(fast_num(100, 20), fast_num(200, 100));
    }

} // namespace

TEST(ConcurrencyTest, GatherStaticMixedTypes)
{
    std::string name;
    int count = 0;
    test_util::run_task([&]
                        { return gather_two(&name, &count); });
    EXPECT_EQ(name, "alice");
    EXPECT_EQ(count, 42);
}

TEST(ConcurrencyTest, GatherAllDynamic)
{
    std::vector<int> out;
    test_util::run_task([&]
                        { return gather_dynamic(&out); });
    ASSERT_EQ(out.size(), 4u);
    EXPECT_EQ(out[0], 0);
    EXPECT_EQ(out[1], 10);
    EXPECT_EQ(out[2], 20);
    EXPECT_EQ(out[3], 30);
}

TEST(ConcurrencyTest, GatherPropagatesFirstException)
{
    std::string msg;
    int completed = 0;
    test_util::run_task([&]
                        { return gather_with_exception(&msg, &completed); });
    EXPECT_EQ(msg, "sub task failed");
    EXPECT_EQ(completed, 42); // 其他任务正常完成
}

TEST(ConcurrencyTest, GatherVoidOk)
{
    int sum = 0;
    test_util::run_task([&]
                        { return gather_void_ok(&sum); });
    EXPECT_EQ(sum, 3);
}

TEST(ConcurrencyTest, GatherVoidPropagatesException)
{
    bool caught = false;
    test_util::run_task([&]
                        { return gather_void_exception(&caught); });
    EXPECT_TRUE(caught);
}

TEST(ConcurrencyTest, WaitForTimeout)
{
    bool caught = false;
    test_util::run_task([&]
                        { return wait_for_timeout(&caught); });
    EXPECT_TRUE(caught);
}

TEST(ConcurrencyTest, WaitForSuccess)
{
    int result = 0;
    test_util::run_task([&]
                        { return wait_for_ok(&result); });
    EXPECT_EQ(result, 42);
}

TEST(ConcurrencyTest, WaitForVoidTask)
{
    bool caught = false;
    test_util::run_task([&]
                        { return wait_for_void_timeout(&caught); });
    EXPECT_TRUE(caught);
}

TEST(ConcurrencyTest, WaitAnyFirstWins)
{
    int result = 0;
    test_util::run_task([&]
                        { return wait_any_fast(&result); });
    EXPECT_EQ(result, 100); // 快者胜
}
