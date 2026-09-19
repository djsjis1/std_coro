// test_stress_races.cpp — 并发竞争压力测试: 针对已修复的 race condition 回归
#include <gtest/gtest.h>

#include <coro/coro.hpp>

#include "test_util.h"

#include <atomic>
#include <thread>
#include <vector>

using namespace std::chrono_literals;

namespace {

    // ── 多线程同时 wake 同一个 EventLoop ──
    // 回归: 旧实现 wake() 访问 io_uring ring, 多线程并发导致 data race
    // 修复: wake() 使用 eventfd, 不访问 ring
    coro::Task<> multi_wake_worker(coro::Promise<int>* p, int id) {
        // 每个 worker 从不同线程 set_value → 触发 cross_thread_wake
        std::thread([p, id]() mutable {
            std::this_thread::sleep_for(std::chrono::milliseconds(id % 5));
            try {
                p->set_value(id);
            } catch (...) {
                // 重复 set 会抛 logic_error, 忽略
            }
        }).detach();
    }

    coro::Task<> multi_wake_scenario(int* result, int n_threads) {
        coro::Promise<int> p;
        auto f = p.get_future();

        // 从多个线程同时 set_value (只有第一个成功)
        std::vector<std::thread> threads;
        for (int i = 0; i < n_threads; ++i) {
            threads.emplace_back([&p, i] {
                std::this_thread::sleep_for(std::chrono::milliseconds(i % 3));
                try {
                    p.set_value(i);
                } catch (...) {
                }
            });
        }

        *result = co_await f; // 只收到第一个值

        for (auto& t : threads)
            t.join();
    }

    // ── 大量跨线程 dispatch + cancel 竞争 ──
    coro::Task<> cancel_race_worker(std::atomic<int>* completed) {
        try {
            co_await coro::sleep(5s); // 长 sleep, 等待被取消
        } catch (const coro::CancelledError&) {
            // 正常取消路径
        }
        ++*completed;
    }

    // ── 快速创建/销毁 Scheduler ──
    // 回归: ~Scheduler 中 stop() 访问正在销毁的 EventLoop
    coro::Task<> rapid_scheduler_task(std::atomic<int>* total_tasks) {
        ++*total_tasks;
        co_return;
    }

    void rapid_scheduler_lifecycle(std::atomic<int>* total_tasks) {
        for (int round = 0; round < 5; ++round) {
            coro::Scheduler sched(2);
            for (int i = 0; i < 20; ++i) {
                sched.spawn_any([total_tasks] {
                    return rapid_scheduler_task(total_tasks);
                });
            }
            sched.wait_all();
        }
    }

    // ── 跨线程 dispatch 风暴 ──
    // 多线程同时向同一个 EventLoop dispatch 协程
    coro::Task<> dispatch_storm_scenario(std::atomic<int>* completed, int n_threads, int per_thread) {
        // 捕获当前线程的 EventLoop: 工作线程需要 dispatch 到这个 loop
        auto* main_loop = &coro::EventLoop::get();
        std::vector<std::thread> threads;
        for (int t = 0; t < n_threads; ++t) {
            threads.emplace_back([completed, per_thread, main_loop] {
                for (int i = 0; i < per_thread; ++i) {
                    main_loop->dispatch([completed] { ++*completed; });
                }
            });
        }
        // 给工作线程时间 dispatch
        co_await coro::sleep(200ms);
        for (auto& t : threads)
            t.join();
    }

    // ── 并发 Future 多等待者 + 跨线程 set_value ──
    coro::Task<int> future_waiter_count(coro::Future<int>* f) {
        co_return co_await *f;
    }

    coro::Task<> multi_waiter_cross_thread(std::atomic<int>* sum, int n_waiters) {
        coro::Promise<int> p;
        auto f = p.get_future();

        std::vector<coro::Task<int>> waiters;
        for (int i = 0; i < n_waiters; ++i)
            waiters.push_back(future_waiter_count(&f));

        // 从另一个线程 set_value
        std::thread worker([p = std::move(p)]() mutable {
            std::this_thread::sleep_for(30ms);
            p.set_value(42);
        });

        for (auto& w : waiters)
            *sum += co_await std::move(w);

        worker.join();
    }

} // namespace

// ── 多线程同时 wake 同一个 EventLoop ──
TEST(StressRaceTest, MultiThreadWakeSameLoop) {
    int result = 0;
    test_util::run_task([&] { return multi_wake_scenario(&result, 10); });
    EXPECT_GE(result, 0); // 收到某个线程的值 (不崩溃即通过)
    EXPECT_LT(result, 10);
}

// ── 快速创建/销毁 Scheduler (回归 ~Scheduler race) ──
TEST(StressRaceTest, RapidSchedulerLifecycle) {
    std::atomic<int> total_tasks{0};
    rapid_scheduler_lifecycle(&total_tasks);
    EXPECT_EQ(total_tasks.load(), 100); // 5 轮 × 20 任务
}

// ── 跨线程 dispatch 风暴 ──
TEST(StressRaceTest, CrossThreadDispatchStorm) {
    std::atomic<int> completed{0};
    test_util::run_task([&] { return dispatch_storm_scenario(&completed, 8, 100); });
    EXPECT_EQ(completed.load(), 800); // 8 线程 × 100 dispatch
}

// ── 多等待者 + 跨线程 set_value ──
TEST(StressRaceTest, MultiWaiterCrossThread) {
    std::atomic<int> sum{0};
    test_util::run_task([&] { return multi_waiter_cross_thread(&sum, 10); });
    EXPECT_EQ(sum.load(), 420); // 10 等待者 × 42
}

// ── 高并发 cancel + sleep 竞争 ──
TEST(StressRaceTest, CancelDuringSleep) {
    std::atomic<int> completed{0};
    auto worker = cancel_race_worker(&completed);
    worker.start();
    // 让 worker 挂到 sleep 上
    std::this_thread::sleep_for(20ms);
    coro::EventLoop::get().run(); // 驱动一下

    // 从另一个线程 cancel
    std::thread canceler([&worker] {
        std::this_thread::sleep_for(10ms);
        worker.cancel();
    });

    coro::EventLoop::get().run(); // 驱动到完成
    canceler.join();
    EXPECT_EQ(completed.load(), 1); // 取消后协程正常结束
}

// ── 大量并发 Promise/Future 跨线程 ──
TEST(StressRaceTest, ManyCrossThreadPromises) {
    std::atomic<int> total{0};
    constexpr int N = 50;

    auto scenario = [&]() -> coro::Task<> {
        std::vector<std::thread> threads;
        std::vector<coro::Promise<int>> promises(N);
        std::vector<coro::Future<int>> futures;
        futures.reserve(N);

        for (int i = 0; i < N; ++i)
            futures.push_back(promises[i].get_future());

        // 从 N 个线程同时 set_value
        for (int i = 0; i < N; ++i) {
            threads.emplace_back([p = std::move(promises[i]), i]() mutable {
                std::this_thread::sleep_for(std::chrono::milliseconds(i % 5));
                p.set_value(i + 1);
            });
        }

        for (int i = 0; i < N; ++i)
            total += co_await futures[i];

        for (auto& t : threads)
            t.join();
    };

    test_util::run_task(scenario);
    EXPECT_EQ(total.load(), N * (N + 1) / 2); // 1+2+...+50 = 1275
}
