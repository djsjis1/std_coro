// stress.cpp — 高并发压力测试 (独立程序, 计时 + 结果判定)
// 运行: build/<config>/coro_stress
//
// 每个场景都校验完成数量: 计数不符或根协程抛异常时打印原因并返回非零。
// 退出码 0 = 全部场景通过, 1 = 至少一个场景失败。
#include <coro/coro.hpp>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <exception>
#include <string>
#include <thread>
#include <vector>

namespace {

    int g_failures = 0;

    std::chrono::steady_clock::time_point now() { return std::chrono::steady_clock::now(); }

    // 场景计时 + 判定: 构造时打印 [RUN], finish() 只在无异常且计数等于期望时打印 [OK]。
    // 计时与判定分开, 避免"跑完就报成功"掩盖漏调度或未取出的异常。
    struct Scenario {
        const char* name;
        std::chrono::steady_clock::time_point started = now();
        long long expected = 0;
        long long actual = 0;
        std::string note;
        std::string error;

        explicit Scenario(const char* scenario_name) : name(scenario_name) { std::printf("[RUN ] %s\n", name); }

        void fail(std::string why) {
            if (error.empty())
                error = std::move(why);
        }

        void finish() {
            const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now() - started).count();
            if (error.empty() && actual == expected)
                std::printf("[OK  ] %6lld ms\n", static_cast<long long>(ms));
            else {
                ++g_failures;
                std::printf("[FAIL] %6lld ms\n", static_cast<long long>(ms));
                if (!error.empty())
                    std::printf("       原因: %s\n", error.c_str());
                else
                    std::printf("       期望 %lld, 实际 %lld\n", expected, actual);
            }
            if (!note.empty())
                std::printf("       %s\n", note.c_str());
        }
    };

    // 启动根协程并驱动事件循环; 循环停止后根协程必须已完成, 异常在此取回。
    // 等价 tests/test_util.h 的 run_task 模式: 不要只验证"事件循环停了"。
    template <typename T> void drive(coro::Task<T>& task, Scenario& s) {
        task.start();
        coro::EventLoop::get().run();
        if (!task.is_ready()) {
            s.fail("根协程未完成: 事件循环已无工作, 但任务仍挂起");
            return;
        }
        try {
            task.take_result();
        } catch (const std::exception& e) {
            s.fail(std::string("根协程异常: ") + e.what());
        } catch (...) {
            s.fail("根协程抛出非标准异常");
        }
    }

    // ── 命名协程函数 ──

    coro::Task<> tiny_sleep(int ms, std::atomic<int64_t>* done) {
        co_await coro::sleep(std::chrono::milliseconds(ms));
        ++*done;
    }

    coro::Task<> yield_n(int n, std::atomic<int64_t>* count) {
        for (int i = 0; i < n; ++i) {
            co_await coro::yield();
            ++*count;
        }
    }

    coro::Task<> q_producer(coro::Queue<int>* q, int n) {
        for (int i = 0; i < n; ++i)
            co_await q->put(i);
    }

    coro::Task<> q_consumer(coro::Queue<int>* q, int n, int64_t* sum) {
        for (int i = 0; i < n; ++i)
            *sum += co_await q->get();
    }

    coro::Task<int> roundtrip_value(int value) { co_return value; }

    coro::Task<> spawn_roundtrip(int n, int64_t* sum) {
        for (int i = 0; i < n; ++i) {
            auto t = coro::spawn(roundtrip_value(i));
            *sum += co_await std::move(t);
        }
    }

    // 记录"当前 loop 是哪个 worker" (通过地址匹配 Scheduler::loop_at)
    coro::Task<> count_by_loop(coro::Scheduler* sched, std::atomic<int64_t> (*per)[4]) {
        auto* cur = &coro::EventLoop::get();
        for (int i = 0; i < 4; ++i) {
            if (sched->loop_at(i) == cur) {
                ++(*per)[i];
                break;
            }
        }
        co_return;
    }

    // ── 场景主协程 ──
    //
    // 先全部启动, 再逐个等待: 已就绪的任务同样经过 await_resume, 结果与异常都不会被跳过。
    // 旧版用"固定 sleep 一段时间再猜都跑完了", 既干扰计时又可能漏掉未完成任务。

    coro::Task<> scene_100k_sleeps(std::atomic<int64_t>* done, int n) {
        std::vector<coro::Task<>> tasks;
        tasks.reserve(static_cast<std::size_t>(n));
        for (int i = 0; i < n; ++i)
            tasks.push_back(tiny_sleep(1, done));
        for (auto& t : tasks)
            t.start();
        for (auto& t : tasks)
            co_await std::move(t);
    }

    coro::Task<> scene_100k_timers(std::atomic<int64_t>* done, int n) {
        std::vector<coro::Task<>> tasks;
        tasks.reserve(static_cast<std::size_t>(n));
        for (int i = 0; i < n; ++i)
            tasks.push_back(tiny_sleep((i % 10) + 1, done)); // 1~10ms 分散 deadline
        for (auto& t : tasks)
            t.start();
        for (auto& t : tasks)
            co_await std::move(t);
    }

    coro::Task<> scene_yield_storm(std::atomic<int64_t>* count, int n) {
        auto a = coro::spawn(yield_n(n, count));
        auto b = coro::spawn(yield_n(n, count));
        auto c = coro::spawn(yield_n(n, count));
        auto d = coro::spawn(yield_n(n, count));
        co_await std::move(a);
        co_await std::move(b);
        co_await std::move(c);
        co_await std::move(d);
    }

    coro::Task<> scene_queue_throughput(int n, int64_t* sum) {
        coro::Queue<int> q;
        auto p = coro::spawn(q_producer(&q, n));
        auto c = coro::spawn(q_consumer(&q, n, sum));
        co_await std::move(p);
        co_await std::move(c);
    }

    coro::Task<> scene_spawn_roundtrip(int n, int64_t* sum) {
        *sum = 0;
        auto t = spawn_roundtrip(n, sum);
        co_await std::move(t);
    }

    void run_single() {
        {
            Scenario s("场景 1: 10 万协程同时 sleep(1ms)");
            std::atomic<int64_t> done{0};
            auto task = scene_100k_sleeps(&done, 100000);
            drive(task, s);
            s.expected = 100000;
            s.actual = done.load();
            s.note = "完成数: " + std::to_string(s.actual);
            s.finish();
        }
        {
            Scenario s("场景 2: 10 万定时器 (1~10ms 分散)");
            std::atomic<int64_t> done{0};
            auto task = scene_100k_timers(&done, 100000);
            drive(task, s);
            s.expected = 100000;
            s.actual = done.load();
            s.note = "完成数: " + std::to_string(s.actual);
            s.finish();
        }
        {
            Scenario s("场景 3: yield 风暴 (4 协程 × 100 万次)");
            std::atomic<int64_t> count{0};
            auto task = scene_yield_storm(&count, 1000000);
            drive(task, s);
            s.expected = 4000000;
            s.actual = count.load();
            s.note = "yield 数: " + std::to_string(s.actual);
            s.finish();
        }
        {
            Scenario s("场景 4: 队列吞吐 (100 万 put/get)");
            int64_t sum = 0;
            auto task = scene_queue_throughput(1000000, &sum);
            drive(task, s);
            s.expected = 499999500000LL; // 0 + 1 + ... + 999999
            s.actual = sum;
            s.note = "sum: " + std::to_string(s.actual);
            s.finish();
        }
        {
            Scenario s("场景 5: spawn+await 往返 (10 万次)");
            int64_t sum = 0;
            auto task = scene_spawn_roundtrip(100000, &sum);
            drive(task, s);
            s.expected = 4999950000LL; // 0 + 1 + ... + 99999
            s.actual = sum;
            s.note = "sum: " + std::to_string(s.actual);
            s.finish();
        }
    }

    void run_multi_thread() {
        {
            Scenario s("场景 6: 4 线程 × 10 万协程 (每线程独立 loop)");
            constexpr int k_threads = 4;
            constexpr int k_per_thread = 100000;
            std::atomic<int64_t> done{0};
            std::atomic<int> stuck{0};
            // 异常记录在各自线程的槽位里, join 之后再展开 —— 不在线程存活期间抛出。
            std::vector<std::exception_ptr> errors(static_cast<std::size_t>(k_threads));
            std::vector<std::thread> threads;
            threads.reserve(static_cast<std::size_t>(k_threads));
            for (int i = 0; i < k_threads; ++i) {
                threads.emplace_back([&done, &stuck, &errors, i] {
                    try {
                        std::vector<coro::Task<>> tasks;
                        tasks.reserve(static_cast<std::size_t>(k_per_thread));
                        for (int k = 0; k < k_per_thread; ++k)
                            tasks.push_back(tiny_sleep(1, &done));
                        for (auto& t : tasks)
                            t.start();
                        coro::EventLoop::get().run();
                        // 事件循环退出后逐个消费: 未就绪说明卡住, 已就绪也要取回异常。
                        for (auto& t : tasks) {
                            if (!t.is_ready()) {
                                ++stuck;
                                continue;
                            }
                            t.take_result();
                        }
                    } catch (...) {
                        errors[static_cast<std::size_t>(i)] = std::current_exception();
                    }
                });
            }
            for (auto& t : threads)
                t.join();

            s.expected = static_cast<long long>(k_threads) * k_per_thread;
            s.actual = done.load();
            s.note = "完成数: " + std::to_string(s.actual);
            if (stuck.load() != 0)
                s.fail("有 " + std::to_string(stuck.load()) + " 个协程在事件循环退出时仍未完成");
            for (const auto& e : errors) {
                if (!e)
                    continue;
                try {
                    std::rethrow_exception(e);
                } catch (const std::exception& ex) {
                    s.fail(std::string("工作线程异常: ") + ex.what());
                } catch (...) {
                    s.fail("工作线程抛出非标准异常");
                }
                break;
            }
            s.finish();
        }
        {
            Scenario s("场景 7: Scheduler 自动分发 40 万协程 (4 worker)");
            std::atomic<int64_t> done{0};
            coro::Scheduler sched(4);
            for (int i = 0; i < 400000; ++i)
                sched.spawn_any([&done] { return tiny_sleep(1, &done); });
            sched.wait_all();
            s.expected = 400000;
            s.actual = done.load();
            s.note = "完成数: " + std::to_string(s.actual);
            s.finish();
        }
        {
            Scenario s("场景 8: Scheduler 均衡性 (4 worker 各收到任务)");
            coro::Scheduler sched(4);
            std::atomic<int64_t> per_worker[4]{};
            for (int i = 0; i < 4000; ++i)
                sched.spawn_any([&sched, &per_worker] { return count_by_loop(&sched, &per_worker); });
            sched.wait_all();

            long long total = 0;
            std::string note = "各 worker 计数:";
            for (auto& n : per_worker) {
                total += n.load();
                note += " " + std::to_string(n.load());
            }
            // 只要求总数正确; 具体分布由调度决定, 不作为阈值。
            s.expected = 4000;
            s.actual = total;
            s.note = std::move(note);
            s.finish();
        }
    }

} // namespace

int main() {
    std::printf("=== coro 高并发压力测试 ===\n");
    run_single();
    run_multi_thread();
    if (g_failures != 0) {
        std::printf("=== %d 个场景失败 ===\n", g_failures);
        return 1;
    }
    std::printf("=== 全部场景通过 ===\n");
    return 0;
}
