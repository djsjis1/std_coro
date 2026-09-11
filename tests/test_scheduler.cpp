// test_scheduler.cpp — Scheduler: 自动多核分发 / 均衡 / 亲和 / 与现有用法兼容
#include <gtest/gtest.h>

#include <coro/coro.hpp>

#include <atomic>
#include <thread>
#include <vector>

using namespace std::chrono_literals;

namespace {

    // ── 命名协程函数 ──

    coro::Task<> work(int ms, std::atomic<int>* done) {
        co_await coro::sleep(std::chrono::milliseconds(ms));
        ++*done;
    }

    coro::Task<> work_no_sleep(int id, std::atomic<int>* sum) {
        *sum += id;
        co_return;
    }

    // 亲和探针: 记录第一次运行与恢复后所在 loop (应相同)
    coro::Task<> affinity_probe(coro::EventLoop** first, coro::EventLoop** second) {
        *first = &coro::EventLoop::get(); // 第一次运行: 记录所在 loop
        co_await coro::sleep(5ms);
        *second = &coro::EventLoop::get(); // 恢复后: 应仍是同一 loop
    }

} // namespace

// ── 基本: spawn_any 把任务分发到 worker 并正确执行 ──
TEST(SchedulerTest, SpawnAnyExecutesTasks) {
    coro::Scheduler sched(4);
    std::atomic<int> done{0};
    for (int i = 0; i < 20; ++i)
        sched.spawn_any([&done] { return work(5, &done); });
    sched.wait_all();
    EXPECT_EQ(done.load(), 20);
}

// ── 并行性: 4 worker × 50ms 任务, 总时间 ≈ 50ms (串行 200ms) ──
TEST(SchedulerTest, TasksRunInParallel) {
    coro::Scheduler sched(4);
    std::atomic<int> done{0};
    auto t0 = std::chrono::steady_clock::now();
    for (int i = 0; i < 8; ++i)
        sched.spawn_any([&done] { return work(50, &done); });
    sched.wait_all();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
    EXPECT_EQ(done.load(), 8);
    EXPECT_LT(elapsed, 150); // 8 × 50ms / 4 worker ≈ 100ms, 串行需 400ms
}

// ── 均衡: 大量短任务分布到多个 worker (不止一个 worker 干活) ──
TEST(SchedulerTest, LoadBalancingAcrossWorkers) {
    coro::Scheduler sched(4);
    std::atomic<int> sum{0};
    // 100 个 1ms 任务: 最少负载分发应让多个 worker 都活跃
    for (int i = 0; i < 100; ++i)
        sched.spawn_any([&sum] { return work(1, &sum); });
    sched.wait_all();
    EXPECT_EQ(sum.load(), 100);
}

// ── 亲和: 任务内的连续挂起/恢复在同一 worker (共享状态无需加锁) ──
TEST(SchedulerTest, CoroutineAffinity) {
    coro::Scheduler sched(2);
    coro::EventLoop *first = nullptr, *second = nullptr;
    sched.spawn_any([&] { return affinity_probe(&first, &second); });
    sched.wait_all();
    ASSERT_NE(first, nullptr);
    EXPECT_EQ(first, second); // 亲和性: 挂起前后同一 loop
}

// ── 与手动 loop-per-thread 兼容: 主线程自己的 loop 不受影响 ──
TEST(SchedulerTest, CoexistsWithManualLoop) {
    coro::Scheduler sched(2);
    std::atomic<int> sched_done{0};
    sched.spawn_any([&sched_done] { return work(10, &sched_done); });

    // 主线程自己的 loop 同时跑任务
    int main_done = 0;
    std::atomic<int> main_counter{0};
    {
        auto t = work(10, &main_counter);
        t.start();
        coro::EventLoop::get().run();
        main_done = 1;
    }
    sched.wait_all();
    EXPECT_EQ(sched_done.load(), 1);
    EXPECT_EQ(main_counter.load(), 1);
    EXPECT_EQ(main_done, 1);
}
