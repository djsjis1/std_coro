// stress.cpp — 高并发压力测试 (独立程序, 计时输出)
// 运行: build/Debug/coro_stress.exe
#include <coro/coro.hpp>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>
#include <vector>

using namespace std::chrono_literals;

namespace
{

    struct Timer
    {
        std::chrono::steady_clock::time_point t0;
        explicit Timer(const char *name) : t0(std::chrono::steady_clock::now())
        {
            std::printf("[RUN ] %s\n", name);
        }
        ~Timer()
        {
            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::steady_clock::now() - t0)
                          .count();
            std::printf("[OK  ] %6lld ms\n", (long long)ms);
        }
    };

    // ── 命名协程函数 ──

    coro::Task<> tiny_sleep(int ms, std::atomic<int64_t> *done)
    {
        co_await coro::sleep(std::chrono::milliseconds(ms));
        ++*done;
    }

    coro::Task<> yield_n(int n, std::atomic<int64_t> *count)
    {
        for (int i = 0; i < n; ++i)
        {
            co_await coro::yield();
            ++*count;
        }
    }

    coro::Task<> q_producer(coro::Queue<int> *q, int n)
    {
        for (int i = 0; i < n; ++i)
            co_await q->put(i);
    }

    // 记录"当前 loop 是哪个 worker" (通过地址匹配 Scheduler::loop_at)
    coro::Task<> count_by_loop(coro::Scheduler *sched, std::atomic<int64_t> (*per)[4])
    {
        auto *cur = &coro::EventLoop::get();
        for (int i = 0; i < 4; ++i)
        {
            if (sched->loop_at(i) == cur)
            {
                ++(*per)[i];
                break;
            }
        }
        co_return;
    }

    coro::Task<> q_consumer(coro::Queue<int> *q, int n, int64_t *sum)
    {
        for (int i = 0; i < n; ++i)
            *sum += co_await q->get();
    }

    coro::Task<> spawn_roundtrip(int n, int64_t *sum)
    {
        for (int i = 0; i < n; ++i)
        {
            auto t = coro::spawn([](int v) -> coro::Task<int>
                                 { co_return v; }(i));
            *sum += co_await std::move(t);
        }
    }

    // 场景主协程们

    coro::Task<> scene_100k_sleeps(std::atomic<int64_t> *done, int n)
    {
        std::vector<coro::Task<>> tasks;
        tasks.reserve(n);
        for (int i = 0; i < n; ++i)
            tasks.push_back(tiny_sleep(1, done));
        for (auto &t : tasks)
            t.start();
        co_await coro::sleep(50ms); // 等待全部完成 (约 1ms + 调度开销)
        for (auto &t : tasks)
        {
            if (!t.is_ready())
                co_await std::move(t); // 兜底: 未完成则等待
        }
    }

    coro::Task<> scene_100k_timers(std::atomic<int64_t> *done, int n)
    {
        std::vector<coro::Task<>> tasks;
        tasks.reserve(n);
        for (int i = 0; i < n; ++i)
            tasks.push_back(tiny_sleep((i % 10) + 1, done)); // 1~10ms 分散 deadline
        for (auto &t : tasks)
            t.start();
        co_await coro::sleep(100ms);
        for (auto &t : tasks)
        {
            if (!t.is_ready())
                co_await std::move(t);
        }
    }

    coro::Task<> scene_yield_storm(std::atomic<int64_t> *count, int n)
    {
        auto a = coro::spawn(yield_n(n, count));
        auto b = coro::spawn(yield_n(n, count));
        auto c = coro::spawn(yield_n(n, count));
        auto d = coro::spawn(yield_n(n, count));
        co_await std::move(a);
        co_await std::move(b);
        co_await std::move(c);
        co_await std::move(d);
    }

    coro::Task<> scene_queue_throughput(int n, int64_t *sum)
    {
        coro::Queue<int> q;
        auto p = coro::spawn(q_producer(&q, n));
        auto c = coro::spawn(q_consumer(&q, n, sum));
        co_await std::move(p);
        co_await std::move(c);
    }

    coro::Task<> scene_spawn_roundtrip(int n, int64_t *sum)
    {
        *sum = 0;
        auto t = spawn_roundtrip(n, sum);
        // 直接用惰性
        co_await std::move(t);
    }

    void run_single()
    {
        {
            Timer t("场景 1: 10 万协程同时 sleep(1ms)");
            std::atomic<int64_t> done{0};
            auto task = scene_100k_sleeps(&done, 100000);
            task.start();
            coro::EventLoop::get().run();
            std::printf("      完成数: %lld\n", (long long)done.load());
        }
        {
            Timer t("场景 2: 10 万定时器 (1~10ms 分散)");
            std::atomic<int64_t> done{0};
            auto task = scene_100k_timers(&done, 100000);
            task.start();
            coro::EventLoop::get().run();
            std::printf("      完成数: %lld\n", (long long)done.load());
        }
        {
            Timer t("场景 3: yield 风暴 (4 协程 × 100 万次)");
            std::atomic<int64_t> count{0};
            auto task = scene_yield_storm(&count, 1000000);
            task.start();
            coro::EventLoop::get().run();
            std::printf("      yield 数: %lld\n", (long long)count.load());
        }
        {
            Timer t("场景 4: 队列吞吐 (100 万 put/get)");
            int64_t sum = 0;
            auto task = scene_queue_throughput(1000000, &sum);
            task.start();
            coro::EventLoop::get().run();
            std::printf("      sum: %lld\n", (long long)sum);
        }
        {
            Timer t("场景 5: spawn+await 往返 (10 万次)");
            int64_t sum = 0;
            auto task = scene_spawn_roundtrip(100000, &sum);
            task.start();
            coro::EventLoop::get().run();
            std::printf("      sum: %lld\n", (long long)sum);
        }
    }

    void run_multi_thread()
    {
        {
            Timer t("场景 6: 4 线程 × 10 万协程 (每线程独立 loop)");
            std::atomic<int64_t> done{0};
            std::thread ts[4];
            for (auto &t : ts)
            {
                t = std::thread([&done]
                                {
                    std::vector<coro::Task<>> tasks;
                    for (int i = 0; i < 100000; ++i)
                        tasks.push_back(tiny_sleep(1, &done));
                    for (auto &tk : tasks)
                        tk.start();
                    coro::EventLoop::get().run();
                    for (auto &tk : tasks)
                    {
                        if (!tk.is_ready())
                        {
                            auto m = std::move(tk);
                            m.start();
                            // 极少数未完成: 继续驱动
                            coro::EventLoop::get().run();
                        }
                    } });
            }
            for (auto &t : ts)
                t.join();
            std::printf("      完成数: %lld\n", (long long)done.load());
        }
        {
            Timer t("场景 7: Scheduler 自动分发 40 万协程 (4 worker)");
            coro::Scheduler sched(4);
            std::atomic<int64_t> done{0};
            for (int i = 0; i < 400000; ++i)
                sched.spawn_any([&done]
                                { return tiny_sleep(1, &done); });
            sched.wait_all();
            std::printf("      完成数: %lld\n", (long long)done.load());
        }
        {
            Timer t("场景 8: Scheduler 均衡性 (4 worker 各收到任务)");
            coro::Scheduler sched(4);
            std::atomic<int64_t> per_worker[4]{};
            for (int i = 0; i < 4000; ++i)
            {
                sched.spawn_any([&sched, &per_worker]
                                { return count_by_loop(&sched, &per_worker); });
            }
            sched.wait_all();
            std::printf("      各 worker 计数: %lld %lld %lld %lld\n",
                        (long long)per_worker[0].load(), (long long)per_worker[1].load(),
                        (long long)per_worker[2].load(), (long long)per_worker[3].load());
        }
    }

} // namespace

int main()
{
    std::printf("=== coro 高并发压力测试 ===\n");
    run_single();
    run_multi_thread();
    std::printf("=== 全部场景完成 ===\n");
    return 0;
}
