// test_sync.cpp — Lock / Semaphore / Event / Queue
#include <gtest/gtest.h>

#include <coro/coro.hpp>

#include "test_util.h"

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>

#include <vector>

using namespace std::chrono_literals;

namespace {

    // ── Lock: 互斥 (计数累加) ──
    coro::Task<> lock_worker(coro::Lock* lock, int* counter, int rounds) {
        for (int i = 0; i < rounds; ++i) {
            co_await lock->acquire();
            int cur = *counter;
            co_await coro::yield(); // 临界区内让出, 若锁失效则计数被破坏
            *counter = cur + 1;
            lock->release();
        }
    }

    coro::Task<> lock_scenario(int* final_count) {
        coro::Lock lock;
        int counter = 0;
        auto a = coro::spawn(lock_worker(&lock, &counter, 50));
        auto b = coro::spawn(lock_worker(&lock, &counter, 50));
        auto c = coro::spawn(lock_worker(&lock, &counter, 50));
        co_await std::move(a);
        co_await std::move(b);
        co_await std::move(c);
        *final_count = counter;
    }

    // ── Semaphore: 并发上限 ──
    coro::Task<> sem_worker(coro::Semaphore* sem, int* cur, int* max_seen) {
        co_await sem->acquire();
        ++*cur;
        if (*cur > *max_seen)
            *max_seen = *cur;
        co_await coro::yield();
        --*cur;
        sem->release();
    }

    coro::Task<> sem_scenario(int* max_seen) {
        coro::Semaphore sem(2);
        int cur = 0;
        *max_seen = 0;
        auto a = coro::spawn(sem_worker(&sem, &cur, max_seen));
        auto b = coro::spawn(sem_worker(&sem, &cur, max_seen));
        auto c = coro::spawn(sem_worker(&sem, &cur, max_seen));
        auto d = coro::spawn(sem_worker(&sem, &cur, max_seen));
        co_await std::move(a);
        co_await std::move(b);
        co_await std::move(c);
        co_await std::move(d);
    }

    // ── Event: 等待者被唤醒 ──
    coro::Task<> event_waiter(coro::Event* ev, int* woken) {
        co_await ev->wait();
        *woken = 1;
    }

    coro::Task<> event_scenario(int* woken) {
        coro::Event ev;
        auto w = coro::spawn(event_waiter(&ev, woken));
        co_await coro::yield(); // 等待者挂到 ev 上
        ev.set();
        co_await std::move(w);
    }

    // ── Queue: 生产者-消费者往返 ──
    coro::Task<> queue_producer(coro::Queue<int>* q, int n) {
        for (int i = 0; i < n; ++i)
            co_await q->put(i);
    }

    coro::Task<> queue_consumer(coro::Queue<int>* q, int n, int* sum) {
        for (int i = 0; i < n; ++i)
            *sum += co_await q->get();
    }

    coro::Task<> queue_scenario(int* sum, int* size_after) {
        coro::Queue<int> q;
        auto p = coro::spawn(queue_producer(&q, 100));
        auto c = coro::spawn(queue_consumer(&q, 100, sum));
        co_await std::move(p);
        co_await std::move(c);
        *size_after = (int)q.size();
    }

    // ── Queue 有界: put 满则挂起, get 腾出空间 ──
    coro::Task<> bounded_scenario(int* put_rounds, int* final_sum) {
        coro::Queue<int> q(2); // 容量 2
        auto producer = coro::spawn(queue_producer(&q, 10));
        // 消费者: 逐个取出
        int sum = 0;
        for (int i = 0; i < 10; ++i)
            sum += co_await q.get();
        co_await std::move(producer);
        *put_rounds = 10;
        *final_sum = sum;
    }

    // ── Lock RAII 守卫: 离开作用域自动 release (含异常路径) ──
    coro::Task<> guard_worker(coro::Lock* lock, int* counter, int rounds) {
        for (int i = 0; i < rounds; ++i) {
            {
                auto g = co_await lock->guard(); // RAII: 离开块自动 release
                int cur = *counter;
                co_await coro::yield();
                *counter = cur + 1;
            } // ← 析构自动 release
        }
    }

    coro::Task<> guard_scenario(int* final_count) {
        coro::Lock lock;
        int counter = 0;
        auto w1 = coro::spawn(guard_worker(&lock, &counter, 50));
        auto w2 = coro::spawn(guard_worker(&lock, &counter, 50));
        co_await std::move(w1);
        co_await std::move(w2);
        *final_count = counter;
    }

    // 守卫的异常安全: 临界区抛异常, 锁仍被释放
    coro::Task<> guard_exception(bool* released) {
        coro::Lock lock;
        try {
            auto g = co_await lock.guard();
            throw std::runtime_error("x");
            co_return;
        } catch (...) {
        }
        // 异常路径后锁应已释放: 再次获取应立即成功
        auto g2 = co_await lock.guard();
        *released = true;
    }

    // ── Semaphore RAII 守卫 ──
    coro::Task<> sem_guard_worker(coro::Semaphore* sem, int* cur, int* max_seen) {
        {
            auto g = co_await sem->guard();
            ++*cur;
            if (*cur > *max_seen)
                *max_seen = *cur;
            co_await coro::yield();
            --*cur;
        } // 析构自动 release
    }

    coro::Task<> sem_guard_scenario(int* max_seen) {
        coro::Semaphore sem(2);
        int cur = 0;
        *max_seen = 0;
        auto a = coro::spawn(sem_guard_worker(&sem, &cur, max_seen));
        auto b = coro::spawn(sem_guard_worker(&sem, &cur, max_seen));
        auto c = coro::spawn(sem_guard_worker(&sem, &cur, max_seen));
        co_await std::move(a);
        co_await std::move(b);
        co_await std::move(c);
    }

    // ── Queue 非阻塞接口 ──
    coro::Task<> nowait_scenario(int* empty_gets, int* rejected_puts) {
        coro::Queue<int> q(2);

        // get_nowait: 空 → nullopt
        auto none = q.get_nowait();
        *empty_gets = none.has_value() ? 0 : 1;

        // put_nowait: 有空间 → true; 满 → false
        bool ok1 = q.put_nowait(1);
        bool ok2 = q.put_nowait(2);
        bool ok3 = q.put_nowait(3); // 满 → false
        *rejected_puts = (ok1 && ok2 && !ok3) ? 1 : 0;

        // get_nowait 有数据
        auto v = q.get_nowait();
        if (!v || *v != 1)
            *empty_gets = 0;
        co_return; // 显式 co_return (MSVC: 以 if 结尾的协程报 C4716)
    }

} // namespace

TEST(SyncTest, LockMutualExclusion) {
    int final_count = 0;
    test_util::run_task([&] { return lock_scenario(&final_count); });
    EXPECT_EQ(final_count, 150); // 3 协程 × 50 轮, 无丢失
}

TEST(SyncTest, SemaphoreLimitsConcurrency) {
    int max_seen = 0;
    test_util::run_task([&] { return sem_scenario(&max_seen); });
    EXPECT_LE(max_seen, 2); // 信号量上限
    EXPECT_GE(max_seen, 1);
}

TEST(SyncTest, EventWakesWaiter) {
    int woken = 0;
    test_util::run_task([&] { return event_scenario(&woken); });
    EXPECT_EQ(woken, 1);
}

TEST(SyncTest, QueueRoundTrip) {
    int sum = 0, size_after = -1;
    test_util::run_task([&] { return queue_scenario(&sum, &size_after); });
    EXPECT_EQ(sum, 4950); // 0+1+...+99
    EXPECT_EQ(size_after, 0);
}

TEST(SyncTest, BoundedQueueBackpressure) {
    int put_rounds = 0, final_sum = 0;
    test_util::run_task([&] { return bounded_scenario(&put_rounds, &final_sum); });
    EXPECT_EQ(put_rounds, 10);
    EXPECT_EQ(final_sum, 45); // 0+1+...+9
}

TEST(SyncTest, LockGuardRAII) {
    int final_count = 0;
    test_util::run_task([&] { return guard_scenario(&final_count); });
    EXPECT_EQ(final_count, 100); // 2 协程 × 50 轮, 无丢失
}

TEST(SyncTest, LockGuardExceptionSafety) {
    bool released = false;
    test_util::run_task([&] { return guard_exception(&released); });
    EXPECT_TRUE(released); // 异常路径后锁已释放
}

TEST(SyncTest, SemaphoreGuardRAII) {
    int max_seen = 0;
    test_util::run_task([&] { return sem_guard_scenario(&max_seen); });
    EXPECT_LE(max_seen, 2);
    EXPECT_GE(max_seen, 1);
}

TEST(SyncTest, QueueNowait) {
    int empty_gets = 0, rejected_puts = 0;
    test_util::run_task([&] { return nowait_scenario(&empty_gets, &rejected_puts); });
    EXPECT_EQ(empty_gets, 1);
    EXPECT_EQ(rejected_puts, 1);
}

// ============================================================================
// 取消与等待队列的交互 (回归: sync.hpp 的 erase_waiter / 预留点名协议)
//
// 这两条测试针对的是"取消不得把原语弄成永久卡死"的不变量:
//   - 排队中的等待者被取消后, 持有者释放时绝不能去调度已销毁的协程帧;
//   - 被取消的等待者不能吞掉锁/许可 —— 否则一次取消就让后续所有人永久挂起。
// 用 sleep 建立确定性的先后关系, 不用单次 yield 假定跨任务时序。
// ============================================================================

namespace {

    coro::Task<> lock_holder(coro::Lock* lk, std::atomic<bool>* held, std::atomic<bool>* released) {
        auto guard = co_await lk->guard();
        held->store(true);
        co_await coro::sleep(300ms); // 持锁期间让别人排队 (长持: 见下方取消测试的时序说明)
        released->store(true);
        co_return; // guard 在此析构 -> release()
    }

    coro::Task<> lock_taker(coro::Lock* lk, std::atomic<bool>* got) {
        auto guard = co_await lk->guard();
        got->store(true);
        co_return;
    }

    /// 排队者被取消 -> 持有者释放 -> 第三个人必须仍能拿到锁
    coro::Task<> cancelled_queued_lock_waiter(bool* third_got, bool* holder_done) {
        coro::Lock lk;
        std::atomic<bool> held{false}, released{false}, b_got{false}, c_got{false};

        // 与信号量版同一理由: Windows 计时器精度 ~15.6ms, 间隔必须留足刻度余量,
        // 保证 cancel 落在持有者释放之前 (否则 B 已真实取得锁, 测的不是本合同)。
        auto holder = coro::spawn(lock_holder(&lk, &held, &released));
        co_await coro::sleep(30ms); // 确保持有者已持锁

        auto b = std::make_shared<coro::Task<>>(coro::spawn(lock_taker(&lk, &b_got)));
        co_await coro::sleep(30ms); // B 已挂进等待队列
        auto c = std::make_shared<coro::Task<>>(coro::spawn(lock_taker(&lk, &c_got)));
        co_await coro::sleep(30ms); // C 也挂进队列 (排在 B 之后)

        auto* loop = &coro::EventLoop::get();
        loop->dispatch([b] { b->cancel(); });
        co_await coro::sleep(400ms); // 等持有者释放并把锁交给 C

        co_await std::move(holder);
        *holder_done = released.load();
        *third_got = c_got.load();
        EXPECT_FALSE(b_got.load()) << "被取消的等待者不该最终拿到锁";
        co_return;
    }

    coro::Task<> sem_holder(coro::Semaphore* sm, std::atomic<bool>* acquired, int hold_ms) {
        co_await sm->acquire();
        acquired->store(true);
        co_await coro::sleep(std::chrono::milliseconds(hold_ms));
        sm->release();
        co_return;
    }

    /// 信号量: 排队者被取消后许可不该被吞掉
    coro::Task<> cancelled_queued_semaphore_waiter(bool* third_got) {
        coro::Semaphore sm(1);
        std::atomic<bool> a{false}, b{false}, c{false};

        // Windows 默认计时器精度 ~15.6ms: 所有间隔必须留出 ≥10 个刻度的余量,
        // 否则 cancel 可能落在 A 释放之后 —— B 已真实取得许可, 测的就不是
        // "排队中被取消" 的合同了 (曾在 Windows CI 稳定失败)。
        auto holder = coro::spawn(sem_holder(&sm, &a, 300)); // A 长持许可
        co_await coro::sleep(30ms);                          // A 已占住唯一许可

        auto vb = std::make_shared<coro::Task<>>(coro::spawn(sem_holder(&sm, &b, 1)));
        co_await coro::sleep(30ms); // B 已挂进等待队列
        auto vc = std::make_shared<coro::Task<>>(coro::spawn(sem_holder(&sm, &c, 1)));
        co_await coro::sleep(30ms); // C 也挂进队列 (排在 B 之后)

        auto* loop = &coro::EventLoop::get();
        loop->dispatch([vb] { vb->cancel(); });
        co_await coro::sleep(400ms); // A 释放后许可应落到 C

        co_await std::move(holder);
        *third_got = c.load();
        EXPECT_FALSE(b.load()) << "被取消的等待者不该最终拿到许可";
        co_return;
    }

} // namespace

TEST(SyncTest, CancelledQueuedLockWaiterDoesNotStallTheQueue) {
    bool third_got = false;
    bool holder_done = false;
    test_util::run_task([&] { return cancelled_queued_lock_waiter(&third_got, &holder_done); });
    EXPECT_TRUE(holder_done);
    EXPECT_TRUE(third_got) << "排队者被取消后, 锁队列卡死了 (第三个人永远拿不到锁)";
}

TEST(SyncTest, CancelledQueuedSemaphoreWaiterDoesNotLeakPermits) {
    bool third_got = false;
    test_util::run_task([&] { return cancelled_queued_semaphore_waiter(&third_got); });
    EXPECT_TRUE(third_got) << "取消的等待者吞掉了许可, 信号量漏干";
}

// ============================================================================
// Condition 取消窗口的互斥保持
//
// 回归 (旧实现的两个叠加缺陷):
//   1. wait() 是子协程, 取消展开直接销毁子协程帧 (帧内析构不运行),
//      "重新拿锁"的代码永不执行, 等待队列残留僵尸句柄;
//   2. 调用方 Guard 析构的二次 release 会把通知方持有的锁转移给队首
//      等待者 —— 通知方与新持有者并发进入临界区, 互斥失效。
// 修复后 (wait 改为直接 awaiter + Guard 持有者校验): 取消窗口内锁被
// 通知方持有时, 取消路径不碰锁, 探针必须等通知方正常释放后才进入。
// ============================================================================

namespace {

    coro::Task<> cond_cancel_target(coro::Condition* cond, bool* caught) {
        try {
            auto g = co_await cond->lock()->guard();
            while (true)
                co_await cond->wait(); // 挂起 (锁已交还), 将被取消
        } catch (const coro::CancelledError&) {
            *caught = true; // 取消应正常传播 (锁语义不损坏)
        }
    }

    coro::Task<> cond_cancel_notifier(coro::Lock* lock, coro::Condition* cond, std::atomic<int>* in_critical,
                                      int* violations) {
        auto g = co_await lock->guard();
        if (in_critical->fetch_add(1) != 0)
            ++*violations;          // 临界区重叠 = 互斥失效
        co_await coro::sleep(50ms); // 持锁窗口: 期间取消等待者
        in_critical->fetch_sub(1);
        cond->notify();
    } // g 析构 → 正常释放

    coro::Task<> cond_cancel_probe(coro::Lock* lock, std::atomic<int>* in_critical, int* violations) {
        auto g = co_await lock->guard();
        if (in_critical->fetch_add(1) != 0)
            ++*violations;
        co_await coro::yield();
        in_critical->fetch_sub(1);
    }

    coro::Task<> cond_cancel_scenario(int* violations, bool* caught, bool* lock_intact) {
        coro::Lock lock;
        coro::Condition cond(&lock);
        std::atomic<int> in_critical{0};

        auto target = coro::spawn(cond_cancel_target(&cond, caught));
        co_await coro::yield(); // target 拿锁并挂到 cond 上 (锁已交还)

        auto notifier = coro::spawn(cond_cancel_notifier(&lock, &cond, &in_critical, violations));
        auto probe = coro::spawn(cond_cancel_probe(&lock, &in_critical, violations));
        co_await coro::sleep(10ms); // notifier 已持锁进入临界区, probe 已在锁队列排队

        target.cancel(); // 取消窗口: 锁被通知方持有

        co_await std::move(notifier);
        co_await std::move(probe);
        co_await std::move(target);

        // 取消风暴过后锁语义完好
        {
            auto g = co_await lock.guard();
            *lock_intact = true;
        }
    }

} // namespace

TEST(SyncTest, ConditionCancelDoesNotStealLockFromHolder) {
    int violations = 0;
    bool caught = false, lock_intact = false;
    test_util::run_task([&] { return cond_cancel_scenario(&violations, &caught, &lock_intact); });
    EXPECT_EQ(violations, 0) << "取消 cond.wait 的展开路径偷走了通知方持有的锁 (互斥失效)";
    EXPECT_TRUE(caught) << "取消应正常传播为 CancelledError";
    EXPECT_TRUE(lock_intact) << "取消后锁应能正常获取";
}

// ============================================================================
// 跨线程唤醒路由 (回归: set/release 必须把等待者路由回其家 loop)
//
// 旧实现用 EventLoop::get().schedule(h) 唤醒 —— 解析到调用者线程的 loop:
// 从无事件循环的工作线程 set() 会把句柄投到新建的 (未运行的) loop,
// 等待者永久挂死。修复后等待者挂起时记录家 loop, 唤醒路由回去
// (与 future.hpp 的 Waiter{handle, loop} 模式对齐)。
// ============================================================================

namespace {

    coro::Task<> cross_thread_event_waiter(coro::Event* ev, std::atomic<bool>* woken) {
        co_await ev->wait();
        woken->store(true);
    }

    coro::Task<> cross_thread_sem_waiter(coro::Semaphore* sem, std::atomic<int>* acquired) {
        co_await sem->acquire(); // 初始 0 许可 → 挂起
        ++*acquired;
        sem->release();
    }

    coro::Task<> cross_thread_scenario(bool* event_woken, bool* sem_passed) {
        coro::Event ev;
        coro::Semaphore sem(0);
        std::atomic<bool> woken{false};
        std::atomic<int> acquired{0};

        auto w = coro::spawn(cross_thread_event_waiter(&ev, &woken));
        auto s = coro::spawn(cross_thread_sem_waiter(&sem, &acquired));
        co_await coro::yield(); // 两个等待者均已挂起

        // 从无事件循环的裸线程唤醒: 必须路由回等待者的家 loop
        std::thread t([&] {
            ev.set();
            sem.release();
        });
        t.join();

        co_await std::move(w);
        co_await std::move(s);
        *event_woken = woken.load();
        *sem_passed = acquired.load() == 1;
    }

} // namespace

TEST(SyncTest, CrossThreadWakeRoutesToHomeLoop) {
    bool event_woken = false, sem_passed = false;
    test_util::run_task([&] { return cross_thread_scenario(&event_woken, &sem_passed); });
    EXPECT_TRUE(event_woken) << "跨线程 Event::set 没有把等待者唤醒回它的家 loop";
    EXPECT_TRUE(sem_passed) << "跨线程 Semaphore::release 没有把等待者唤醒回它的家 loop";
}
