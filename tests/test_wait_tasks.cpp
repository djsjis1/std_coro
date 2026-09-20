// test_wait_tasks.cpp — N 路 wait: FirstCompleted / FirstException / AllCompleted
#include <gtest/gtest.h>

#include <coro/coro.hpp>

#include "test_util.h"

#include <vector>

using namespace std::chrono_literals;

namespace {

    struct NonDefault {
        explicit NonDefault(int v) : value(v) {}
        NonDefault() = delete;
        NonDefault(const NonDefault&) = delete;
        NonDefault& operator=(const NonDefault&) = delete;
        NonDefault(NonDefault&&) noexcept = default;
        NonDefault& operator=(NonDefault&&) = delete;

        int value;
    };

    // ── 命名协程函数 ──

    coro::Task<int> num_after(int value, int ms) {
        co_await coro::sleep(std::chrono::milliseconds(ms));
        co_return value;
    }

    coro::Task<int> fail_after(int ms) {
        co_await coro::sleep(std::chrono::milliseconds(ms));
        throw std::runtime_error("wait task failed");
        co_return 0;
    }

    coro::Task<int> fail_immediately() {
        throw std::runtime_error("late failure");
        co_return 0;
    }

    coro::Task<int> succeed_immediately(int value) {
        co_return value;
    }

    coro::Task<NonDefault> non_default_value(int value) {
        co_return NonDefault{value};
    }

    std::vector<coro::Task<int>> make_nums() {
        std::vector<coro::Task<int>> v;
        v.push_back(num_after(100, 50)); // 最慢
        v.push_back(num_after(200, 10)); // 最快
        v.push_back(num_after(300, 30));
        return v;
    }

    // ── FirstCompleted: 最快的任务立即返回 ──
    coro::Task<> first_completed_scenario(int* result, int* elapsed_ms, size_t* count) {
        auto t0 = std::chrono::steady_clock::now();
        auto results = co_await coro::wait_tasks(make_nums(), coro::WaitMode::FirstCompleted);
        *elapsed_ms =
            (int)std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
        *count = results.size(); // 断言放 TEST (协程内不能用 ASSERT 宏)
        *result = results.empty() ? -1 : results[0];
        co_return;
    }

    // ── FirstCompleted: 第一个完成的若是失败 → 抛 ──
    coro::Task<> first_completed_fail(bool* caught) {
        std::vector<coro::Task<int>> v;
        v.push_back(fail_after(10));    // 最先失败
        v.push_back(num_after(1, 100)); // 慢
        try {
            co_await coro::wait_tasks(std::move(v), coro::WaitMode::FirstCompleted);
        } catch (const std::runtime_error&) {
            *caught = true;
        }
    }

    // ── FirstException: 任一失败立即抛, 不等其他 ──
    coro::Task<> first_exception_scenario(bool* caught, int* elapsed_ms) {
        std::vector<coro::Task<int>> v;
        v.push_back(num_after(1, 200)); // 慢
        v.push_back(fail_after(10));    // 10ms 失败
        v.push_back(num_after(2, 200)); // 慢
        auto t0 = std::chrono::steady_clock::now();
        try {
            co_await coro::wait_tasks(std::move(v), coro::WaitMode::FirstException);
        } catch (const std::runtime_error&) {
            *caught = true;
        }
        *elapsed_ms =
            (int)std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
    }

    // ── FirstException: 全部成功 → 等全部完成, 返回全部结果 ──
    coro::Task<> first_exception_all_ok(int* sum) {
        auto results = co_await coro::wait_tasks(make_nums(), coro::WaitMode::FirstException);
        *sum = 0;
        for (int r : results)
            *sum += r;
    }

    // ── AllCompleted: 全部完成, 返回全部结果 ──
    coro::Task<> all_completed_scenario(int* sum) {
        auto results = co_await coro::wait_tasks(make_nums(), coro::WaitMode::AllCompleted);
        *sum = 0;
        for (int r : results)
            *sum += r;
    }

    // ── AllCompleted: 有失败 → 抛第一个异常 ──
    coro::Task<> all_completed_fail(bool* caught) {
        std::vector<coro::Task<int>> v;
        v.push_back(num_after(1, 10));
        v.push_back(fail_after(20));
        v.push_back(fail_after(30));
        try {
            co_await coro::wait_tasks(std::move(v), coro::WaitMode::AllCompleted);
        } catch (const std::runtime_error&) {
            *caught = true;
        }
    }

    // ── 空任务列表 ──
    coro::Task<> empty_scenario(size_t* size) {
        auto results = co_await coro::wait_tasks(std::vector<coro::Task<int>>{}, coro::WaitMode::AllCompleted);
        *size = results.size();
    }

    coro::Task<> first_completed_success_is_stable(int* result, bool* threw) {
        std::vector<coro::Task<int>> tasks;
        tasks.push_back(succeed_immediately(17));
        tasks.push_back(fail_immediately());
        try {
            auto results = co_await coro::wait_tasks(std::move(tasks), coro::WaitMode::FirstCompleted);
            *result = results.front();
        } catch (...) {
            *threw = true;
        }
    }

    coro::Task<> non_default_results_scenario(int* gather_sum, int* wait_sum, int* any_value) {
        // 显式走 Task 的移动赋值路径；结果类型本身不可赋值。
        auto reassigned = non_default_value(1);
        reassigned = non_default_value(2);

        std::vector<coro::Task<NonDefault>> gathered;
        gathered.push_back(non_default_value(3));
        gathered.push_back(non_default_value(4));
        auto gather_results = co_await coro::gather_all(std::move(gathered));
        *gather_sum = gather_results[0].value + gather_results[1].value;

        std::vector<coro::Task<NonDefault>> waited;
        waited.push_back(non_default_value(5));
        waited.push_back(non_default_value(6));
        auto wait_results = co_await coro::wait_tasks(std::move(waited), coro::WaitMode::AllCompleted);
        *wait_sum = wait_results[0].value + wait_results[1].value;

        auto first = co_await coro::wait_any(non_default_value(7), non_default_value(8));
        *any_value = first.value;
    }

} // namespace

TEST(WaitTasksTest, FirstCompletedReturnsFastest) {
    int result = -1, elapsed = 0;
    size_t count = 0;
    test_util::run_task([&] { return first_completed_scenario(&result, &elapsed, &count); });
    EXPECT_EQ(count, 1u);   // 只含第一个结果
    EXPECT_EQ(result, 200); // 最快任务 (10ms) 的结果
    EXPECT_LT(elapsed, 45); // 远早于最慢任务 (50ms) 完成
}

TEST(WaitTasksTest, FirstCompletedFailureThrows) {
    bool caught = false;
    test_util::run_task([&] { return first_completed_fail(&caught); });
    EXPECT_TRUE(caught);
}

TEST(WaitTasksTest, FirstExceptionFailsFast) {
    bool caught = false;
    int elapsed = 0;
    test_util::run_task([&] { return first_exception_scenario(&caught, &elapsed); });
    EXPECT_TRUE(caught);
    EXPECT_LT(elapsed, 150); // 10ms 失败立即返回, 不等 200ms 任务
}

TEST(WaitTasksTest, FirstExceptionAllOkReturnsAll) {
    int sum = 0;
    test_util::run_task([&] { return first_exception_all_ok(&sum); });
    EXPECT_EQ(sum, 600); // 100+200+300
}

TEST(WaitTasksTest, AllCompletedReturnsAll) {
    int sum = 0;
    test_util::run_task([&] { return all_completed_scenario(&sum); });
    EXPECT_EQ(sum, 600);
}

TEST(WaitTasksTest, AllCompletedPropagatesFirstFailure) {
    bool caught = false;
    test_util::run_task([&] { return all_completed_fail(&caught); });
    EXPECT_TRUE(caught);
}

TEST(WaitTasksTest, EmptyTaskList) {
    size_t size = 1;
    test_util::run_task([&] { return empty_scenario(&size); });
    EXPECT_EQ(size, 0u);
}

TEST(WaitTasksTest, FirstCompletedIgnoresLaterFailure) {
    int result = 0;
    bool threw = false;
    test_util::run_task([&] { return first_completed_success_is_stable(&result, &threw); });
    EXPECT_FALSE(threw);
    EXPECT_EQ(result, 17);
}

TEST(WaitTasksTest, SupportsMoveConstructOnlyResults) {
    int gather_sum = 0;
    int wait_sum = 0;
    int any_value = 0;
    test_util::run_task([&] { return non_default_results_scenario(&gather_sum, &wait_sum, &any_value); });
    EXPECT_EQ(gather_sum, 7);
    EXPECT_EQ(wait_sum, 11);
    EXPECT_TRUE(any_value == 7 || any_value == 8);
}
