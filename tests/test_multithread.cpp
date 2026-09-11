// test_multithread.cpp — 每线程独立事件循环: 多核并行 / 互不干扰 / 跨线程 Future 路由
#include <gtest/gtest.h>

#include <coro/coro.hpp>

#include "test_util.h"

#include <atomic>
#include <thread>
#include <vector>

using namespace std::chrono_literals;

namespace
{

    // ── 命名协程函数 ──

    coro::Task<> sleeper(int ms, std::atomic<int> *done)
    {
        co_await coro::sleep(std::chrono::milliseconds(ms));
        ++*done;
    }

    coro::Task<> spawn_one(std::atomic<int> *completed)
    {
        co_await coro::sleep(1ms);
        ++*completed;
    }

    coro::Task<> await_all(std::vector<coro::Task<>> *tasks)
    {
        for (auto &t : *tasks)
            co_await std::move(t);
    }

    // 每线程跑 N 个并发子任务, 全部完成
    coro::Task<> spawn_n(int n, std::atomic<int> *completed)
    {
        std::vector<coro::Task<>> tasks;
        tasks.reserve(n);
        for (int i = 0; i < n; ++i)
            tasks.push_back(spawn_one(completed));
        co_await await_all(&tasks);
    }

    // 线程工作函数 (普通函数, 非协程): 在当前线程自己的 loop 里跑任务
    void thread_sleep_and_run(int ms, std::atomic<int> *done)
    {
        auto t = sleeper(ms, done);
        t.start();
        coro::EventLoop::get().run(); // 本线程的 loop
    }

    void thread_spawn_and_run(int n, std::atomic<int> *completed)
    {
        auto t = spawn_n(n, completed);
        t.start();
        coro::EventLoop::get().run();
    }

    // 跨线程 Future: 主线程 loop 等待, 工作线程 (无 loop) set_value
    coro::Task<> cross_loop_future_scenario(int *out)
    {
        coro::Promise<int> p;
        auto f = p.get_future();
        std::thread worker([promise = std::move(p)]() mutable
                           {
            std::this_thread::sleep_for(20ms);
            promise.set_value(77); });
        *out = co_await f; // 等待者在本线程 loop; set_value 从 worker 线程路由回来
        worker.join();
    }

    // 主 loop 跑 20ms 任务并等待 30ms (期间工作线程 loop 也在跑)
    coro::Task<> main_loop_work(std::atomic<int> *other, int *out)
    {
        auto t = sleeper(20, other); // 主 loop 的 20ms 任务
        t.start();
        co_await coro::sleep(30ms);
        *out = 1;
    }

} // namespace

// ── 核心验证: 两个线程各跑一个 60ms 任务, 总时间 ≈ 60ms (并行) 而非 120ms ──
TEST(MultiThreadTest, TwoLoopsRunInParallel)
{
    std::atomic<int> a{0}, b{0};
    auto t0 = std::chrono::steady_clock::now();
    std::thread t1(thread_sleep_and_run, 60, &a);
    std::thread t2(thread_sleep_and_run, 60, &b);
    t1.join();
    t2.join();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - t0)
                       .count();
    EXPECT_EQ(a.load(), 1);
    EXPECT_EQ(b.load(), 1);
    EXPECT_LT(elapsed, 110); // 并行: 两个 60ms 合计 < 110ms (串行会 ≥ 120ms)
}

// ── 每线程独立 loop: 各自跑大量协程互不干扰 ──
TEST(MultiThreadTest, IndependentLoopsDoNotInterfere)
{
    std::atomic<int> c1{0}, c2{0};
    std::thread t1(thread_spawn_and_run, 50, &c1);
    std::thread t2(thread_spawn_and_run, 50, &c2);
    t1.join();
    t2.join();
    EXPECT_EQ(c1.load(), 50);
    EXPECT_EQ(c2.load(), 50);
}

// ── 四线程并行: 4 × 50ms ≈ 50ms ──
TEST(MultiThreadTest, FourLoopsScaleToCores)
{
    constexpr int N = 4;
    std::atomic<int> done[N]{};
    std::thread threads[N];
    auto t0 = std::chrono::steady_clock::now();
    for (int i = 0; i < N; ++i)
        threads[i] = std::thread(thread_sleep_and_run, 50, &done[i]);
    for (auto &t : threads)
        t.join();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - t0)
                       .count();
    for (int i = 0; i < N; ++i)
        EXPECT_EQ(done[i].load(), 1);
    EXPECT_LT(elapsed, 100); // 4 × 50ms 并行 < 100ms (串行 200ms)
}

// ── 跨线程 Future 在多 loop 模型下正确路由 ──
TEST(MultiThreadTest, CrossThreadFutureRoutesToOwnerLoop)
{
    int out = 0;
    test_util::run_task([&]
                        { return cross_loop_future_scenario(&out); });
    EXPECT_EQ(out, 77);
}

// ── 主线程 loop 与其他线程 loop 同时活跃互不阻塞 ──
TEST(MultiThreadTest, MainLoopRunsWhileOtherLoopBusy)
{
    std::atomic<int> other{0};
    std::thread worker(thread_sleep_and_run, 40, &other);
    // 主线程 loop 同时跑自己的任务
    int out = 0;
    test_util::run_task([&]
                        { return main_loop_work(&other, &out); });
    worker.join();
    EXPECT_EQ(out, 1);
    EXPECT_EQ(other.load(), 2); // 20ms 主 loop + 40ms 工作线程各 +1
}
