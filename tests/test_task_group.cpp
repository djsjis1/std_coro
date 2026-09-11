// test_task_group.cpp — TaskGroup 结构化并发: 成功 / 失败取消 / 聚合 / 空组 / 兜底
#include <gtest/gtest.h>

#include <coro/coro.hpp>

#include "test_util.h"

using namespace std::chrono_literals;

namespace
{

    // ── 命名协程函数 ──

    coro::Task<> worker_sleep(int ms, int *ran)
    {
        co_await coro::sleep(std::chrono::milliseconds(ms));
        *ran += 1;
        co_return;
    }

    coro::Task<> failing_task(int ms, const char *msg)
    {
        co_await coro::sleep(std::chrono::milliseconds(ms));
        throw std::runtime_error(msg);
        co_return;
    }

    // 带清理观察: 被组取消时必须执行清理 (验证取消传播到原任务)
    coro::Task<> cancel_observable(int *cleaned)
    {
        struct Guard
        {
            int *p;
            ~Guard() { *p = 1; }
        } g{cleaned};
        co_await coro::sleep(10s); // 长睡眠, 被组取消
        co_return;
    }

    // ── 场景 1: 全部成功 ──
    coro::Task<> all_succeed(int *count)
    {
        coro::TaskGroup group;
        group.spawn(worker_sleep(10, count));
        group.spawn(worker_sleep(20, count));
        group.spawn(worker_sleep(5, count));
        co_await group.wait();
    }

    // ── 场景 2: 一个失败 → 取消其余 → ExceptionGroup(1 个异常) ──
    coro::Task<> one_fails(int *exception_count, int *cleaned)
    {
        int dummy = 0; // 15ms worker 的输出位置 (不可传 nullptr)
        coro::TaskGroup group;
        group.spawn(failing_task(5, "boom"));
        group.spawn(cancel_observable(cleaned)); // 应被组取消 (清理执行)
        group.spawn(worker_sleep(15, &dummy));
        try
        {
            co_await group.wait();
        }
        catch (const coro::ExceptionGroup &eg)
        {
            *exception_count = (int)eg.exceptions().size();
        }
    }

    // ── 场景 3: 多个失败 → ExceptionGroup 聚合多个 ──
    // 注意: 三个任务用相同 deadline (同时失败)。若错开 deadline,
    // 第一个失败会取消其余 (正确语义), 聚合数就只剩 1。
    coro::Task<> multi_fail(int *exception_count)
    {
        coro::TaskGroup group;
        group.spawn(failing_task(5, "first"));
        group.spawn(failing_task(5, "second"));
        group.spawn(failing_task(5, "third"));
        try
        {
            co_await group.wait();
        }
        catch (const coro::ExceptionGroup &eg)
        {
            *exception_count = (int)eg.exceptions().size();
        }
    }

    // ── 场景 4: 空组 wait 立即返回 ──
    coro::Task<> empty_group(bool *returned)
    {
        coro::TaskGroup group;
        co_await group.wait(); // 不挂起
        *returned = true;
    }

    // ── 场景 5: 混合返回类型子任务 ──
    coro::Task<int> typed_worker(int v)
    {
        co_await coro::sleep(5ms);
        co_return v;
    }

    coro::Task<> mixed_types(bool *done)
    {
        coro::TaskGroup group;
        group.spawn(typed_worker(1)); // Task<int>
        int void_done = 0;
        group.spawn(worker_sleep(5, &void_done)); // Task<void>
        group.spawn(typed_worker(3));             // Task<int>
        co_await group.wait();
        *done = (void_done == 1);
    }

    // ── 场景 6: 忘记 wait, 析构兜底取消残留子任务 ──
    coro::Task<> forgotten_wait(int *cleaned)
    {
        {
            coro::TaskGroup group;
            group.spawn(cancel_observable(cleaned)); // 10s 睡眠, 不会被 wait
            co_await coro::sleep(5ms);               // 让任务真正跑起来挂到 sleep 上
        } // 析构: 自动取消 → 任务被唤醒 → Guard 析构 (清理执行)
        // 给取消传播一点时间
        co_await coro::sleep(20ms);
    }

    // ── 场景 7: 失败后 wait 抛出后组进入终态, 不再有残留 ──
    coro::Task<> post_failure_state(bool *group_done)
    {
        coro::TaskGroup group;
        group.spawn(failing_task(5, "x"));
        try
        {
            co_await group.wait();
        }
        catch (const coro::ExceptionGroup &)
        {
        }
        *group_done = group.done();
    }

} // namespace

TEST(TaskGroupTest, AllSucceed)
{
    int count = 0;
    test_util::run_task([&]
                        { return all_succeed(&count); });
    EXPECT_EQ(count, 3); // 三个任务都跑完
}

TEST(TaskGroupTest, OneFailsCancelsOthers)
{
    int exception_count = 0, cleaned = 0;
    test_util::run_task([&]
                        { return one_fails(&exception_count, &cleaned); });
    EXPECT_EQ(exception_count, 1); // 单个异常也打包成 ExceptionGroup
    EXPECT_EQ(cleaned, 1);         // 其余任务被取消 → 清理逻辑执行
}

TEST(TaskGroupTest, MultiFailAggregates)
{
    int exception_count = 0;
    test_util::run_task([&]
                        { return multi_fail(&exception_count); });
    EXPECT_EQ(exception_count, 3); // 三个异常全部聚合
}

TEST(TaskGroupTest, EmptyGroupImmediateReturn)
{
    bool returned = false;
    test_util::run_task([&]
                        { return empty_group(&returned); });
    EXPECT_TRUE(returned);
}

TEST(TaskGroupTest, MixedReturnTypes)
{
    bool done = false;
    test_util::run_task([&]
                        { return mixed_types(&done); });
    EXPECT_TRUE(done);
}

TEST(TaskGroupTest, ForgottenWaitDestructorCancels)
{
    int cleaned = 0;
    test_util::run_task([&]
                        { return forgotten_wait(&cleaned); });
    EXPECT_EQ(cleaned, 1); // 析构兜底取消, 子任务清理执行
}

TEST(TaskGroupTest, GroupDoneAfterWait)
{
    bool group_done = false;
    test_util::run_task([&]
                        { return post_failure_state(&group_done); });
    EXPECT_TRUE(group_done); // wait 后组进入终态
}
