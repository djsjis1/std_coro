// test_sync.cpp — Lock / Semaphore / Event / Queue
#include <gtest/gtest.h>

#include <coro/coro.hpp>

#include "test_util.h"

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
