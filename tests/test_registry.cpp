// test_registry.cpp — Queue task_done/join + Condition + 任务注册表
#include <gtest/gtest.h>

#include <coro/coro.hpp>

#include "test_util.h"

using namespace std::chrono_literals;

namespace
{

    // ── 命名协程函数 ──

    // Queue: 生产者放 N 个, 消费者处理完每个调 task_done, join 等全部处理完
    coro::Task<> q_producer(coro::Queue<int> *q, int n)
    {
        for (int i = 0; i < n; ++i)
            co_await q->put(i);
    }

    coro::Task<> q_consumer(coro::Queue<int> *q, int n, int *sum)
    {
        for (int i = 0; i < n; ++i)
        {
            int v = co_await q->get();
            *sum += v;
            q->task_done(); // 处理完成
        }
    }

    coro::Task<> join_scenario(int *sum, bool *join_returned, size_t *unfinished)
    {
        coro::Queue<int> q;
        auto p = coro::spawn(q_producer(&q, 10));
        auto c = coro::spawn(q_consumer(&q, 10, sum));
        co_await q.join(); // 挂起直到 10 个任务全部 task_done
        *join_returned = true;
        *unfinished = q.unfinished_count();
        co_await std::move(p);
        co_await std::move(c);
    }

    // Condition: 生产者通知, 消费者等待谓词成立
    coro::Task<> cond_wait(coro::Condition *cond, bool *ready, int *observed)
    {
        auto g = co_await cond->lock()->guard();
        while (!*ready) // 谓词循环 (防虚假唤醒)
            co_await cond->wait();
        *observed = 1; // wait 返回时重新持有锁
        // guard 析构自动释放
    }

    coro::Task<> cond_notify(coro::Condition *cond, bool *ready)
    {
        co_await coro::sleep(10ms);
        {
            auto g = co_await cond->lock()->guard();
            *ready = true;
            cond->notify();
        }
    }

    coro::Task<> condition_scenario(int *observed)
    {
        coro::Lock lock;
        coro::Condition cond(&lock);
        bool ready = false;
        auto w = coro::spawn(cond_wait(&cond, &ready, observed));
        auto n = coro::spawn(cond_notify(&cond, &ready));
        co_await std::move(w);
        co_await std::move(n);
    }

    // Condition: notify_all 唤醒多个等待者
    coro::Task<> cond_wait_multi(coro::Condition *cond, bool *go, int *woken)
    {
        auto g = co_await cond->lock()->guard();
        while (!*go)
            co_await cond->wait();
        ++*woken;
    }

    coro::Task<> notify_all_scenario(int *woken)
    {
        coro::Lock lock;
        coro::Condition cond(&lock);
        bool go = false;
        auto a = coro::spawn(cond_wait_multi(&cond, &go, woken));
        auto b = coro::spawn(cond_wait_multi(&cond, &go, woken));
        auto c = coro::spawn(cond_wait_multi(&cond, &go, woken));
        co_await coro::sleep(10ms); // 三个等待者都挂上
        {
            auto g = co_await lock.guard();
            go = true;
            cond.notify_all();
        }
        co_await std::move(a);
        co_await std::move(b);
        co_await std::move(c);
    }

    // 任务注册表: 活跃任务数与 current_task
    coro::Task<> count_inside(bool *valid_current)
    {
        // 协程内: current_task 应非空且等于自己
        auto cur = coro::EventLoop::current_task();
        *valid_current = (bool)cur;
        co_await coro::sleep(5ms);
    }

    coro::Task<> registry_scenario(size_t *count_during, bool *current_valid,
                                   size_t *count_after)
    {
        auto a = coro::spawn(count_inside(current_valid));
        auto b = coro::spawn(count_inside(current_valid));
        *count_during = coro::EventLoop::get().active_task_count();
        co_await std::move(a);
        co_await std::move(b);
        *count_after = coro::EventLoop::get().active_task_count();
    }

    // Condition 取消安全: wait 挂起期间被取消, 锁语义不损坏
    coro::Task<> cond_wait_cancellable(coro::Condition *cond, bool *cleaned)
    {
        struct Guard2
        {
            bool *p;
            ~Guard2() { *p = true; }
        } g2{cleaned};
        auto g = co_await cond->lock()->guard();
        co_await cond->wait(); // 挂起 (锁已交还), 将被取消
    }

    coro::Task<> condition_cancel_scenario(bool *cleaned, bool *lock_free_after)
    {
        coro::Lock lock;
        coro::Condition cond(&lock);
        auto w = coro::spawn(cond_wait_cancellable(&cond, cleaned));
        co_await coro::sleep(10ms); // 等待者挂到 wait 上 (锁已交还)
        w.cancel();                 // 取消: wait 内部应拿回锁再传播 → guard 析构释放一次
        try
        {
            co_await std::move(w);
        }
        catch (const coro::CancelledError &)
        {
        }
        // 锁语义完好: 能再次获取并释放
        {
            auto g = co_await lock.guard();
            *lock_free_after = true;
        }
    }

} // namespace

TEST(RegistryTest, QueueJoinWaitsForAllTasks)
{
    int sum = 0;
    bool join_returned = false;
    size_t unfinished = 99;
    test_util::run_task([&]
                        { return join_scenario(&sum, &join_returned, &unfinished); });
    EXPECT_TRUE(join_returned);
    EXPECT_EQ(sum, 45);        // 0+1+...+9
    EXPECT_EQ(unfinished, 0u); // join 返回时全部 task_done
}

TEST(RegistryTest, ConditionWaitNotify)
{
    int observed = 0;
    test_util::run_task([&]
                        { return condition_scenario(&observed); });
    EXPECT_EQ(observed, 1);
}

TEST(RegistryTest, ConditionNotifyAll)
{
    int woken = 0;
    test_util::run_task([&]
                        { return notify_all_scenario(&woken); });
    EXPECT_EQ(woken, 3);
}

TEST(RegistryTest, ActiveTaskCountAndCurrentTask)
{
    size_t count_during = 0, count_after = 99;
    bool current_valid = false;
    test_util::run_task([&]
                        { return registry_scenario(&count_during, &current_valid, &count_after); });
    EXPECT_GE(count_during, 3); // 主协程 + 两个子任务 (可能含 monitor)
    EXPECT_TRUE(current_valid); // 协程内 current_task 非空
    EXPECT_EQ(count_after, 1u); // 子任务完成后只剩主协程自己
}

TEST(RegistryTest, CurrentTaskEmptyOutsideCoroutine)
{
    // 非协程上下文: current_task 为空
    EXPECT_FALSE(coro::EventLoop::current_task());
}

TEST(RegistryTest, ConditionCancellationKeepsLockSemantics)
{
    bool cleaned = false, lock_free_after = false;
    test_util::run_task([&]
                        { return condition_cancel_scenario(&cleaned, &lock_free_after); });
    EXPECT_TRUE(cleaned);         // 协程清理执行
    EXPECT_TRUE(lock_free_after); // 锁语义完好 (未被双重转移破坏)
}
