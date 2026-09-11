// test_wait_tasks.cpp — N 路 wait: FirstCompleted / FirstException / AllCompleted
#include <gtest/gtest.h>

#include <coro/coro.hpp>

#include "test_util.h"

#include <vector>

using namespace std::chrono_literals;

namespace
{

    // ── 命名协程函数 ──

    coro::Task<int> num_after(int value, int ms)
    {
        co_await coro::sleep(std::chrono::milliseconds(ms));
        co_return value;
    }

    coro::Task<int> fail_after(int ms)
    {
        co_await coro::sleep(std::chrono::milliseconds(ms));
        throw std::runtime_error("wait task failed");
        co_return 0;
    }

    std::vector<coro::Task<int>> make_nums()
    {
        std::vector<coro::Task<int>> v;
        v.push_back(num_after(100, 50)); // 最慢
        v.push_back(num_after(200, 10)); // 最快
        v.push_back(num_after(300, 30));
        return v;
    }

    // ── FirstCompleted: 最快的任务立即返回 ──
    coro::Task<> first_completed_scenario(int *result, int *elapsed_ms, size_t *count)
    {
        auto t0 = std::chrono::steady_clock::now();
        auto results = co_await coro::wait_tasks(make_nums(),
                                                 coro::WaitMode::FirstCompleted);
        *elapsed_ms = (int)std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::steady_clock::now() - t0)
                          .count();
        *count = results.size(); // 断言放 TEST (协程内不能用 ASSERT 宏)
        *result = results.empty() ? -1 : results[0];
        co_return;
    }

    // ── FirstCompleted: 第一个完成的若是失败 → 抛 ──
    coro::Task<> first_completed_fail(bool *caught)
    {
        std::vector<coro::Task<int>> v;
        v.push_back(fail_after(10));    // 最先失败
        v.push_back(num_after(1, 100)); // 慢
        try
        {
            co_await coro::wait_tasks(std::move(v), coro::WaitMode::FirstCompleted);
        }
        catch (const std::runtime_error &)
        {
            *caught = true;
        }
    }

    // ── FirstException: 任一失败立即抛, 不等其他 ──
    coro::Task<> first_exception_scenario(bool *caught, int *elapsed_ms)
    {
        std::vector<coro::Task<int>> v;
        v.push_back(num_after(1, 200)); // 慢
        v.push_back(fail_after(10));    // 10ms 失败
        v.push_back(num_after(2, 200)); // 慢
        auto t0 = std::chrono::steady_clock::now();
        try
        {
            co_await coro::wait_tasks(std::move(v), coro::WaitMode::FirstException);
        }
        catch (const std::runtime_error &)
        {
            *caught = true;
        }
        *elapsed_ms = (int)std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::steady_clock::now() - t0)
                          .count();
    }

    // ── FirstException: 全部成功 → 等全部完成, 返回全部结果 ──
    coro::Task<> first_exception_all_ok(int *sum)
    {
        auto results = co_await coro::wait_tasks(make_nums(),
                                                 coro::WaitMode::FirstException);
        *sum = 0;
        for (int r : results)
            *sum += r;
    }

    // ── AllCompleted: 全部完成, 返回全部结果 ──
    coro::Task<> all_completed_scenario(int *sum)
    {
        auto results = co_await coro::wait_tasks(make_nums(),
                                                 coro::WaitMode::AllCompleted);
        *sum = 0;
        for (int r : results)
            *sum += r;
    }

    // ── AllCompleted: 有失败 → 抛第一个异常 ──
    coro::Task<> all_completed_fail(bool *caught)
    {
        std::vector<coro::Task<int>> v;
        v.push_back(num_after(1, 10));
        v.push_back(fail_after(20));
        v.push_back(fail_after(30));
        try
        {
            co_await coro::wait_tasks(std::move(v), coro::WaitMode::AllCompleted);
        }
        catch (const std::runtime_error &)
        {
            *caught = true;
        }
    }

    // ── 空任务列表 ──
    coro::Task<> empty_scenario(size_t *size)
    {
        auto results = co_await coro::wait_tasks(std::vector<coro::Task<int>>{},
                                                 coro::WaitMode::AllCompleted);
        *size = results.size();
    }

} // namespace

TEST(WaitTasksTest, FirstCompletedReturnsFastest)
{
    int result = -1, elapsed = 0;
    size_t count = 0;
    test_util::run_task([&]
                        { return first_completed_scenario(&result, &elapsed, &count); });
    EXPECT_EQ(count, 1u);   // 只含第一个结果
    EXPECT_EQ(result, 200); // 最快任务 (10ms) 的结果
    EXPECT_LT(elapsed, 45); // 远早于最慢任务 (50ms) 完成
}

TEST(WaitTasksTest, FirstCompletedFailureThrows)
{
    bool caught = false;
    test_util::run_task([&]
                        { return first_completed_fail(&caught); });
    EXPECT_TRUE(caught);
}

TEST(WaitTasksTest, FirstExceptionFailsFast)
{
    bool caught = false;
    int elapsed = 0;
    test_util::run_task([&]
                        { return first_exception_scenario(&caught, &elapsed); });
    EXPECT_TRUE(caught);
    EXPECT_LT(elapsed, 150); // 10ms 失败立即返回, 不等 200ms 任务
}

TEST(WaitTasksTest, FirstExceptionAllOkReturnsAll)
{
    int sum = 0;
    test_util::run_task([&]
                        { return first_exception_all_ok(&sum); });
    EXPECT_EQ(sum, 600); // 100+200+300
}

TEST(WaitTasksTest, AllCompletedReturnsAll)
{
    int sum = 0;
    test_util::run_task([&]
                        { return all_completed_scenario(&sum); });
    EXPECT_EQ(sum, 600);
}

TEST(WaitTasksTest, AllCompletedPropagatesFirstFailure)
{
    bool caught = false;
    test_util::run_task([&]
                        { return all_completed_fail(&caught); });
    EXPECT_TRUE(caught);
}

TEST(WaitTasksTest, EmptyTaskList)
{
    size_t size = 1;
    test_util::run_task([&]
                        { return empty_scenario(&size); });
    EXPECT_EQ(size, 0u);
}
