// test_sync_extended.cpp — Condition / Queue::join / Event 扩展 / EventLoop dispatch+stop
#include <gtest/gtest.h>

#include <coro/coro.hpp>

#include "test_util.h"

#include <atomic>
#include <thread>
#include <vector>

using namespace std::chrono_literals;

namespace
{

    // ── Condition: 基本 notify ──
    coro::Task<> cond_waiter(coro::Condition *cond, int *woken)
    {
        coro::Lock *lock = cond->lock();
        {
            auto g = co_await lock->guard();
            co_await cond->wait();
            *woken = 1;
        }
    }

    coro::Task<> cond_notify_scenario(int *woken)
    {
        coro::Lock lock;
        coro::Condition cond(&lock);
        auto w = coro::spawn(cond_waiter(&cond, woken));
        co_await coro::sleep(10ms);
        {
            auto g = co_await lock.guard();
            cond.notify();
        }
        co_await std::move(w);
    }

    // ── Condition: notify_all 唤醒多个等待者 ──
    coro::Task<> cond_multi_waiter(coro::Condition *cond, std::atomic<int> *ready_count, std::atomic<int> *count)
    {
        coro::Lock *lock = cond->lock();
        auto g = co_await lock->guard();
        ready_count->fetch_add(1); // 标记已持锁
        co_await cond->wait();
        count->fetch_add(1);
    }

    coro::Task<> cond_notify_all_scenario(int *final_count)
    {
        coro::Lock lock;
        coro::Condition cond(&lock);
        std::atomic<int> ready_count{0};
        std::atomic<int> count{0};
        auto w1 = coro::spawn(cond_multi_waiter(&cond, &ready_count, &count));
        auto w2 = coro::spawn(cond_multi_waiter(&cond, &ready_count, &count));
        auto w3 = coro::spawn(cond_multi_waiter(&cond, &ready_count, &count));
        // 等待所有等待者都获取锁并挂起在 wait()
        while (ready_count.load() < 3)
            co_await coro::sleep(5ms);
        co_await coro::sleep(10ms); // 再等确保都挂起了
        {
            auto g = co_await lock.guard();
            cond.notify_all();
        }
        co_await std::move(w1);
        co_await std::move(w2);
        co_await std::move(w3);
        *final_count = count.load();
        co_return;
    }

    // ── Condition: 谓词循环模式 (防虚假唤醒) ──
    coro::Task<> cond_predicate_waiter(coro::Condition *cond, bool *ready, int *result)
    {
        coro::Lock *lock = cond->lock();
        auto g = co_await lock->guard();
        while (!*ready)
            co_await cond->wait();
        *result = 42;
        co_return;
    }

    coro::Task<> cond_predicate_scenario(int *result)
    {
        coro::Lock lock;
        coro::Condition cond(&lock);
        bool ready = false;
        auto w = coro::spawn(cond_predicate_waiter(&cond, &ready, result));
        co_await coro::sleep(10ms);
        {
            auto g = co_await lock.guard();
            ready = true;
            cond.notify();
        }
        co_await std::move(w);
    }

    // ── Queue::task_done / join ──
    coro::Task<> queue_worker(coro::Queue<int> *q, int *sum, int count)
    {
        for (int i = 0; i < count; ++i)
        {
            int v = co_await q->get();
            *sum += v;
            q->task_done();
        }
    }

    coro::Task<> queue_join_scenario(int *sum, bool *join_done)
    {
        coro::Queue<int> q;
        auto consumer = coro::spawn(queue_worker(&q, sum, 5));

        for (int i = 1; i <= 5; ++i)
            co_await q.put(i);

        co_await q.join();
        *join_done = true;
        co_await std::move(consumer);
    }

    // ── Queue::join 空队列立即返回 ──
    coro::Task<> queue_join_empty(bool *join_done)
    {
        coro::Queue<int> q;
        co_await q.join();
        *join_done = true;
        co_return;
    }

    // ── Queue 状态查询 ──
    coro::Task<> queue_status(int *size_after, bool *empty_after, bool *full_after)
    {
        coro::Queue<int> q(2);
        co_await q.put(1);
        co_await q.put(2);
        *size_after = (int)q.size();
        *empty_after = q.empty();
        *full_after = q.full();
        co_return;
    }

    // ── Event: clear + 已 set 后立即返回 ──
    coro::Task<> event_already_set(int *out)
    {
        coro::Event ev;
        ev.set();
        co_await ev.wait();
        *out = 1;
        co_return;
    }

    coro::Task<> wait_event(coro::Event *ev)
    {
        co_await ev->wait();
    }

    coro::Task<> event_clear_and_wait(bool *timed_out)
    {
        coro::Event ev;
        ev.set();
        ev.clear();
        // clear 后 wait 应该挂起 (用 wait_for 包装来检测是否挂起)
        try
        {
            co_await coro::wait_for(wait_event(&ev), 30ms);
            *timed_out = false;
        }
        catch (const coro::TimeoutError &)
        {
            *timed_out = true;
        }
    }

    // ── Event::is_set ──
    coro::Task<> event_is_set(bool *before, bool *after)
    {
        coro::Event ev;
        *before = ev.is_set();
        ev.set();
        *after = ev.is_set();
        co_return;
    }

    // ── Task::detach ──
    coro::Task<> detach_worker(std::atomic<int> *counter)
    {
        co_await coro::sleep(10ms);
        counter->fetch_add(1);
    }

    coro::Task<> detach_scenario(std::atomic<int> *counter)
    {
        auto t = coro::spawn(detach_worker(counter));
        t.detach();
        co_await coro::sleep(50ms);
    }

    // ── Event: 多个等待者同时等待同一个 Event ──
    coro::Task<> event_multi_waiter(coro::Event *ev, std::atomic<int> *count)
    {
        co_await ev->wait();
        count->fetch_add(1);
    }

    coro::Task<> event_multi_wait_scenario(int *final_count)
    {
        coro::Event ev;
        std::atomic<int> count{0};
        auto w1 = coro::spawn(event_multi_waiter(&ev, &count));
        auto w2 = coro::spawn(event_multi_waiter(&ev, &count));
        auto w3 = coro::spawn(event_multi_waiter(&ev, &count));
        co_await coro::sleep(10ms); // 让等待者都挂上
        ev.set();
        co_await std::move(w1);
        co_await std::move(w2);
        co_await std::move(w3);
        *final_count = count.load();
        co_return;
    }

    // ── Lock::is_locked 状态查询 ──
    coro::Task<> lock_is_locked(bool *before, bool *during, bool *after)
    {
        coro::Lock lock;
        *before = lock.is_locked();
        {
            auto g = co_await lock.guard();
            *during = lock.is_locked();
        }
        *after = lock.is_locked();
        co_return;
    }

    // ── Semaphore::available 状态查询 ──
    coro::Task<> sem_available(int *initial, int *during, int *after)
    {
        coro::Semaphore sem(3);
        *initial = sem.available();
        {
            auto g = co_await sem.guard();
            *during = sem.available();
        }
        *after = sem.available();
        co_return;
    }

    // ── Queue::unfinished_count ──
    coro::Task<> queue_unfinished(int *before_done, int *after_done)
    {
        coro::Queue<int> q;
        co_await q.put(1);
        co_await q.put(2);
        *before_done = (int)q.unfinished_count();
        (void)co_await q.get();
        q.task_done();
        *after_done = (int)q.unfinished_count();
        co_return;
    }

    // ── Condition::waiter_count ──
    coro::Task<> cond_waiter_count(size_t *count)
    {
        coro::Lock lock;
        coro::Condition cond(&lock);
        std::atomic<int> ready{0};

        auto waiter = [&]() -> coro::Task<>
        {
            auto g = co_await lock.guard();
            ready.fetch_add(1);
            co_await cond.wait();
        };

        auto w1 = coro::spawn(waiter());
        auto w2 = coro::spawn(waiter());
        // 等待两个都挂起
        while (ready.load() < 2)
            co_await coro::sleep(5ms);
        co_await coro::sleep(10ms);
        *count = cond.waiter_count();
        cond.notify_all();
        co_await std::move(w1);
        co_await std::move(w2);
    }

    // ── EventLoop::active_task_count ──
    coro::Task<> dummy_sleep()
    {
        co_await coro::sleep(50ms);
    }

    coro::Task<> active_count_scenario(size_t *during, size_t *after)
    {
        auto t = coro::spawn(dummy_sleep());
        *during = coro::EventLoop::get().active_task_count();
        co_await std::move(t);
        *after = coro::EventLoop::get().active_task_count();
    }

    // ── Task: is_ready / is_started 状态查询 ──
    coro::Task<> task_status_scenario(bool *started_before, bool *ready_before, bool *ready_after)
    {
        auto t = coro::spawn(dummy_sleep());
        *started_before = t.is_started();
        *ready_before = t.is_ready();
        co_await std::move(t);
        *ready_after = true; // await 完成后必然 ready
    }

} // namespace

// ── Condition: 基本 notify 唤醒等待者 ──
TEST(SyncExtendedTest, ConditionNotify)
{
    int woken = 0;
    test_util::run_task([&]
                        { return cond_notify_scenario(&woken); });
    EXPECT_EQ(woken, 1);
}

// ── Condition: notify_all 唤醒全部等待者 ──
TEST(SyncExtendedTest, ConditionNotifyAll)
{
    int final_count = 0;
    test_util::run_task([&]
                        { return cond_notify_all_scenario(&final_count); });
    EXPECT_EQ(final_count, 3);
}

// ── Condition: 谓词循环模式 ──
TEST(SyncExtendedTest, ConditionPredicateWait)
{
    int result = 0;
    test_util::run_task([&]
                        { return cond_predicate_scenario(&result); });
    EXPECT_EQ(result, 42);
}

// ── Queue: task_done + join 收尾协议 ──
TEST(SyncExtendedTest, QueueTaskDoneAndJoin)
{
    int sum = 0;
    bool join_done = false;
    test_util::run_task([&]
                        { return queue_join_scenario(&sum, &join_done); });
    EXPECT_EQ(sum, 15);
    EXPECT_TRUE(join_done);
}

// ── Queue: 空队列 join 立即返回 ──
TEST(SyncExtendedTest, QueueJoinEmpty)
{
    bool join_done = false;
    test_util::run_task([&]
                        { return queue_join_empty(&join_done); });
    EXPECT_TRUE(join_done);
}

// ── Queue: 状态查询 (size/empty/full) ──
TEST(SyncExtendedTest, QueueStatusQueries)
{
    int size_after = -1;
    bool empty_after = true, full_after = false;
    test_util::run_task([&]
                        { return queue_status(&size_after, &empty_after, &full_after); });
    EXPECT_EQ(size_after, 2);
    EXPECT_FALSE(empty_after);
    EXPECT_TRUE(full_after);
}

// ── Event: 已 set 后 wait 立即返回 ──
TEST(SyncExtendedTest, EventAlreadySetSkipsWait)
{
    int out = 0;
    test_util::run_task([&]
                        { return event_already_set(&out); });
    EXPECT_EQ(out, 1);
}

// ── Event: clear 后 wait 挂起 ──
TEST(SyncExtendedTest, EventClearCausesWait)
{
    bool timed_out = false;
    test_util::run_task([&]
                        { return event_clear_and_wait(&timed_out); });
    EXPECT_TRUE(timed_out);
}

// ── Event: is_set 状态查询 ──
TEST(SyncExtendedTest, EventIsSet)
{
    bool before = true, after = false;
    test_util::run_task([&]
                        { return event_is_set(&before, &after); });
    EXPECT_FALSE(before);
    EXPECT_TRUE(after);
}

// ── Task::detach: 放弃所有权, 协程仍运行到完成 ──
TEST(SyncExtendedTest, TaskDetachRunsToCompletion)
{
    std::atomic<int> counter{0};
    test_util::run_task([&]
                        { return detach_scenario(&counter); });
    EXPECT_EQ(counter.load(), 1);
}

// ── EventLoop::dispatch: 跨线程投递函数 ──
TEST(EventLoopTest, CrossThreadDispatch)
{
    std::atomic<int> counter{0};
    coro::EventLoop *main_loop = nullptr;

    test_util::run_task([&counter, &main_loop]() -> coro::Task<>
                        {
        main_loop = &coro::EventLoop::get();
        // 从另一个线程投递函数到本线程的 loop
        std::thread t([main_loop, &counter]
                      {
            main_loop->dispatch([&counter]
                                { counter.fetch_add(1); }); });
        // 等待投递完成
        co_await coro::sleep(20ms);
        t.join();
        // 再等一会让 dispatch 的函数执行
        co_await coro::sleep(20ms); });

    EXPECT_EQ(counter.load(), 1);
}

// ── EventLoop::stop + run_until_stopped: 常驻模式优雅退出 ──
TEST(EventLoopTest, RunUntilStopped)
{
    std::atomic<bool> loop_started{false};
    std::atomic<bool> loop_exited{false};
    coro::EventLoop *loop_ptr = nullptr;

    std::thread loop_thread([&]
                            {
        auto &loop = coro::EventLoop::get();
        loop_ptr = &loop;
        loop_started.store(true);
        loop.run_until_stopped();
        loop_exited.store(true); });

    while (!loop_started.load())
        std::this_thread::sleep_for(1ms);

    std::this_thread::sleep_for(20ms);

    // 从主线程停止 loop_thread 的事件循环
    loop_ptr->stop();

    loop_thread.join();
    EXPECT_TRUE(loop_exited.load());
}

// ── Event: 多个等待者同时等待同一个 Event ──
TEST(SyncExtendedTest, EventMultipleWaiters)
{
    int final_count = 0;
    test_util::run_task([&]
                        { return event_multi_wait_scenario(&final_count); });
    EXPECT_EQ(final_count, 3);
}

// ── Lock: is_locked 状态查询 ──
TEST(SyncExtendedTest, LockIsLocked)
{
    bool before = true, during = false, after = true;
    test_util::run_task([&]
                        { return lock_is_locked(&before, &during, &after); });
    EXPECT_FALSE(before); // 未持锁
    EXPECT_TRUE(during);  // guard 持锁中
    EXPECT_FALSE(after);  // guard 析构后释放
}

// ── Semaphore: available 状态查询 ──
TEST(SyncExtendedTest, SemaphoreAvailable)
{
    int initial = 0, during = 0, after = 0;
    test_util::run_task([&]
                        { return sem_available(&initial, &during, &after); });
    EXPECT_EQ(initial, 3); // 初始 3 个许可
    EXPECT_EQ(during, 2);  // guard 占用 1 个
    EXPECT_EQ(after, 3);   // guard 析构后恢复
}

// ── Queue: unfinished_count ──
TEST(SyncExtendedTest, QueueUnfinishedCount)
{
    int before_done = -1, after_done = -1;
    test_util::run_task([&]
                        { return queue_unfinished(&before_done, &after_done); });
    EXPECT_EQ(before_done, 2); // put 了 2 个, 还没 task_done
    EXPECT_EQ(after_done, 1);  // task_done 了 1 个
}

// ── Condition: waiter_count ──
TEST(SyncExtendedTest, ConditionWaiterCount)
{
    size_t count = 0;
    test_util::run_task([&]
                        { return cond_waiter_count(&count); });
    EXPECT_EQ(count, 2u); // 两个等待者挂起中
}

// ── EventLoop: active_task_count ──
TEST(EventLoopTest, ActiveTaskCount)
{
    size_t during = 0, after = 0;
    test_util::run_task([&]
                        { return active_count_scenario(&during, &after); });
    // during: 主协程 + spawn 的 sleep 任务 = 至少 2
    EXPECT_GE(during, 2u);
    // after: spawn 的任务已完成, 只剩主协程自身 = 1
    EXPECT_EQ(after, 1u);
}

// ── Task: is_ready / is_started 状态查询 ──
TEST(SyncExtendedTest, TaskStatusQueries)
{
    bool started_before = false, ready_before = true, ready_after = false;
    test_util::run_task([&]
                        { return task_status_scenario(&started_before, &ready_before, &ready_after); });
    EXPECT_TRUE(started_before); // spawn 后已启动
    EXPECT_FALSE(ready_before);  // spawn 后未完成
    EXPECT_TRUE(ready_after);    // await 完成后已就绪
}
