// benchmarks/bench_runtime.cpp — 事件循环/定时器/取消的测量基线 (F03)
//
// 目的: 任何性能改动之前先有可复现的基线。这里只记录**稳定可观测**的指标:
//   - 调度吞吐: 每秒完成的协程唤醒次数 (yield 往返)
//   - 定时器延迟: sleep(1ms) 的实际延迟 p50/p95/p99/最大
//   - 取消延迟: cancel 请求到任务真正结束的时间
//   - 堆分配次数: 覆盖全局 operator new/delete 计数 (仅本可执行文件)
//
// 刻意不做的事:
//   - 不统计系统调用数 (需 strace/ETW, 不适合每个平台都跑的基准)
//   - 不设硬性阈值: 共享机器上微秒级门槛不可靠; 用前后对比判断回归
//
// 用法: coro_bench [scale]   (scale 默认 1; 2 表示每项翻倍)

#include <coro/coro.hpp>
#include <coro/scheduler.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <new>
#include <string>
#include <vector>

// ---- 全局分配计数 (只影响本可执行文件) ----
namespace {
std::atomic<long long> g_allocations{0};
}

void* operator new(std::size_t size) {
    g_allocations.fetch_add(1, std::memory_order_relaxed);
    if (void* p = std::malloc(size ? size : 1))
        return p;
    throw std::bad_alloc{};
}
void operator delete(void* p) noexcept {
    std::free(p);
}
void operator delete(void* p, std::size_t) noexcept {
    std::free(p);
}

namespace {

using clk = std::chrono::steady_clock;

    struct stats {
        double p50 = 0, p95 = 0, p99 = 0, max = 0, mean = 0;
    };

    stats summarize(std::vector<double> samples) {
        stats s;
        if (samples.empty())
            return s;
        std::sort(samples.begin(), samples.end());
        const auto pick = [&](double q) {
            const std::size_t index = static_cast<std::size_t>(q * static_cast<double>(samples.size() - 1));
            return samples[index];
        };
        double sum = 0;
        for (double v : samples)
            sum += v;
        s.mean = sum / static_cast<double>(samples.size());
        s.p50 = pick(0.50);
        s.p95 = pick(0.95);
        s.p99 = pick(0.99);
        s.max = samples.back();
        return s;
    }

    coro::Task<> yield_rounds(int rounds) {
        for (int i = 0; i < rounds; ++i)
            co_await coro::yield();
    }

    coro::Task<> one_millisecond(std::vector<double>* samples) {
        const auto begin = clk::now();
        co_await coro::sleep(std::chrono::milliseconds(1));
        samples->push_back(std::chrono::duration<double, std::micro>(clk::now() - begin).count());
    }

    coro::Task<> long_sleep() {
        co_await coro::sleep(std::chrono::hours(1));
    }

    void bench_scheduling(int scale) {
        const int tasks = 2000 * scale;
        const int rounds = 20;
        auto& loop = coro::EventLoop::get();
        const auto allocs_before = g_allocations.load();

        std::vector<coro::Task<>> pending;
        pending.reserve(static_cast<std::size_t>(tasks));
        for (int i = 0; i < tasks; ++i)
            pending.push_back(yield_rounds(rounds));

        const auto begin = clk::now();
        for (auto& task : pending) {
            task.start();
            task.detach(); // 完成即自释放; 循环在全部帧结束后退出
        }
        loop.run();
        const auto elapsed = std::chrono::duration<double, std::nano>(clk::now() - begin).count();
        const auto wakeups = static_cast<double>(tasks) * rounds;
        const auto allocs = g_allocations.load() - allocs_before;
        std::printf("[sched]  tasks=%d rounds=%d  %.1f ns/wakeup  %.2f M wakeup/s  allocs=%lld\n", tasks, rounds,
                    elapsed / wakeups, wakeups / (elapsed / 1e9) / 1e6, static_cast<long long>(allocs));
    }

    void bench_timer_latency(int scale) {
        const int count = 200 * scale;
        auto& loop = coro::EventLoop::get();
        std::vector<double> samples;
        samples.reserve(static_cast<std::size_t>(count));
        std::vector<coro::Task<>> pending;
        pending.reserve(static_cast<std::size_t>(count));
        for (int i = 0; i < count; ++i)
            pending.push_back(one_millisecond(&samples));
        for (auto& task : pending) {
            task.start();
            task.detach();
        }
        loop.run();
        const auto s = summarize(samples);
        std::printf("[timer] sleep(1ms) n=%zu  p50=%.0f p95=%.0f p99=%.0f max=%.0f us (mean %.0f)\n", samples.size(),
                    s.p50, s.p95, s.p99, s.max, s.mean);
    }

    /// 定时器在“持续 yield 风暴”下的实际延迟。
    /// 这是 F01 的判定依据: ready 队列永不为空时, 定时器/IO 是否仍能推进。
    coro::Task<> yield_storm(std::atomic<bool>* stop) {
        while (!stop->load(std::memory_order_acquire))
            co_await coro::yield();
    }

    coro::Task<> timed_under_load(std::atomic<bool>* stop, std::vector<double>* samples) {
        for (int i = 0; i < 50; ++i) {
            const auto begin = clk::now();
            co_await coro::sleep(std::chrono::milliseconds(1));
            samples->push_back(std::chrono::duration<double, std::micro>(clk::now() - begin).count());
        }
        stop->store(true, std::memory_order_release);
    }

    void bench_timer_under_load(int scale) {
        std::atomic<bool> stop{false};
        std::vector<double> samples;
        samples.reserve(50);
        auto storm = yield_storm(&stop);
        storm.start();
        storm.detach();
        auto driver = timed_under_load(&stop, &samples);
        driver.start();
        coro::EventLoop::get().run();
        driver.take_result();
        const auto s = summarize(samples);
        std::printf("[load]   sleep(1ms) under yield storm n=%zu  p50=%.0f p95=%.0f p99=%.0f max=%.0f us\n",
                    samples.size(), s.p50, s.p95, s.p99, s.max);
        (void)scale;
    }

    /// 取消延迟测量必须在事件循环内完成: 外部无法把“进到循环直到全部挂起”
    /// 与“取消后等它真正结束”拆开 (loop.run 遇到 1h 定时器不会返回)。
    coro::Task<> drive_cancel(int count, std::vector<double>* samples) {
        std::vector<coro::Task<>> tasks;
        tasks.reserve(static_cast<std::size_t>(count));
        for (int i = 0; i < count; ++i)
            tasks.push_back(long_sleep());
        for (auto& task : tasks)
            task.start();
        co_await coro::sleep(std::chrono::milliseconds(10)); // 让全部任务真正挂到定时器上

        for (auto& task : tasks) {
            const auto begin = clk::now();
            task.cancel();
            while (!task.is_ready())
                co_await coro::yield();
            samples->push_back(std::chrono::duration<double, std::micro>(clk::now() - begin).count());
        }
    }

    void bench_cancel_latency(int scale) {
        const int count = 200 * scale;
        std::vector<double> samples;
        samples.reserve(static_cast<std::size_t>(count));
        auto driver = drive_cancel(count, &samples);
        driver.start();
        coro::EventLoop::get().run();
        driver.take_result();
        const auto s = summarize(samples);
        std::printf("[cancel] long-sleep n=%zu  p50=%.0f p95=%.0f p99=%.0f max=%.0f us\n", samples.size(), s.p50,
                    s.p95, s.p99, s.max);
    }

    /// 分发路径采样 (F03): 不同 worker 数下的分发吞吐与扩展比。
    /// 计划要求“先采样再改选 worker 策略”, 这里给出 1/2/4 worker 的对照。
    coro::Task<> small_work(int iterations) {
        volatile int sink = 0;
        for (int i = 0; i < iterations; ++i)
            sink = sink + i;
        (void)sink;
        co_return;
    }

    void bench_scheduler_dispatch() {
        const int tasks = 20000;
        const int iterations = 200;
        double baseline = 0;
        for (int workers : {1, 2, 4}) {
            coro::Scheduler sched(static_cast<std::size_t>(workers));
            const auto begin = clk::now();
            for (int i = 0; i < tasks; ++i)
                sched.spawn_any([iterations] { return small_work(iterations); });
            sched.wait_all();
            const auto elapsed = std::chrono::duration<double, std::milli>(clk::now() - begin).count();
            if (workers == 1)
                baseline = elapsed;
            std::printf("[sched*] workers=%d tasks=%d  %.2f ms  %.0f task/s  speedup=%.2fx\n", workers, tasks,
                        elapsed, static_cast<double>(tasks) / (elapsed / 1000.0), baseline / elapsed);
        }
    }

    /// 分配热点采样 (F03): 每种基础模式平均多少次堆分配。
    /// 目的不是追求零分配, 而是找出“每单位工作”的真实成本在哪里。
    coro::Task<> immediate() { co_return; }

    coro::Task<> one_yield_once() { co_await coro::yield(); }

    void bench_alloc_profile() {
        const int n = 2000;
        const auto measure = [n](const char* label, auto factory) {
            const auto before = g_allocations.load();
            {
                std::vector<coro::Task<>> tasks;
                tasks.reserve(static_cast<std::size_t>(n));
                for (int i = 0; i < n; ++i)
                    tasks.push_back(factory());
                for (auto& task : tasks) {
                    task.start();
                    task.detach();
                }
                coro::EventLoop::get().run();
            }
            const auto used = g_allocations.load() - before;
            std::printf("[alloc]  %-14s %5.2f allocs/unit (%lld total)\n", label, static_cast<double>(used) / n,
                        static_cast<long long>(used));
        };
        measure("task:return", [] { return immediate(); });
        measure("task:yield", [] { return one_yield_once(); });
    }

} // namespace

int main(int argc, char** argv) {
    const int scale = argc > 1 ? std::max(1, std::atoi(argv[1])) : 1;
    std::printf("coro runtime baseline (scale=%d)\n", scale);
    const auto allocs_before = g_allocations.load();
    bench_scheduling(scale);
    bench_timer_latency(scale);
    bench_timer_under_load(scale);
    bench_cancel_latency(scale);
    bench_scheduler_dispatch();
    bench_alloc_profile();
    std::printf("[mem]   total allocations during run = %lld\n", static_cast<long long>(g_allocations.load() - allocs_before));
    return 0;
}
