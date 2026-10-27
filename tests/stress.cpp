// stress.cpp — 高并发压力测试 (独立程序, 分组调度 + 计时 + 判定)
//
// 运行: build/<config>/coro_stress [group] [--scale=N] [--soak=SEC] [--list]
//   不带 group 时跑全部场景; group 取 core / sync / channel / net / process / threads / soak
//   --scale 缩放每个场景的规模 (默认 1.0, 压得动就调大), --soak 设定混合负载时长 (秒)
//
// 每个场景都校验完成数量: 计数不符或根协程抛异常时打印原因并返回非零。
// 退出码 0 = 全部场景通过, 1 = 至少一个场景失败。
#include <coro/channel.hpp>
#include <coro/coro.hpp>
#include <coro/fs.hpp>
#include <coro/pool.hpp>
#include <coro/process.hpp>
#include <coro/rate_limit.hpp>
#include <coro/select.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <string>
#include <thread>
#include <vector>

// 值检查而非 defined(): 能力宏总是被注入 (关闭时 =0), defined() 恒真会
// 让 no-uring 配置编进 net 场景 (coro::net::TcpStream 不存在, 编译失败)
#if defined(_WIN32) || (defined(CORO_HAS_URING) && CORO_HAS_URING)
#include <coro/net.hpp>
#define CORO_STRESS_HAVE_NET 1
// 进程 (coro::process) 与异步文件 (coro::fs::read_all/write_all) 与 net 同源,
// 只在原生 I/O 后端下存在 —— no-uring 配置下 process.hpp/fs.hpp 编译为空壳
#define CORO_STRESS_HAVE_NATIVE_IO 1
#endif

// HTTP 客户端只在开启该组件时编入 (它在原生 I/O 之上, 依赖前面的 net 部分)
#if defined(CORO_HAS_HTTP_CLIENT) && CORO_HAS_HTTP_CLIENT && defined(CORO_STRESS_HAVE_NET)
#include <coro/http_client.hpp>
#define CORO_STRESS_HAVE_HTTP 1
#endif

#if defined(_WIN32)
#include <windows.h>
// psapi.h 的 K32GetProcessMemoryInfo 在 kernel32 里导出, 不必额外链接 psapi.lib
#include <psapi.h>
#endif

namespace {

    int g_failures = 0;
    double g_scale = 1.0;
    int g_soak_seconds = 3;
    int g_watchdog_seconds = 0; // 0 = 关闭; >0 时某场景超时未完成就断定挂死并终止
    bool g_pin_cores = false;   // 多 loop 场景是否把每个 loop 绑到固定核 (只用于实验对比)
    int g_net_conns = 32;       // 4KB echo 的连接数 (扫描用: 看吞吐在哪饱和)
    std::atomic<const char*> g_running{nullptr};
    std::atomic<long long> g_running_since_ms{0};

    long long now_ms() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::steady_clock::now().time_since_epoch())
            .count();
    }

    /// 看门狗: 压测程序的失败模式里最难定位的是“卡住不动”。
    /// 开启后只要某场景超过阈值没结束, 就直接报出场景名并终止, 不靠人工猜。
    void start_watchdog() {
        const int limit = g_watchdog_seconds;
        std::thread([limit] {
            for (;;) {
                std::this_thread::sleep_for(std::chrono::milliseconds(200));
                const char* name = g_running.load(std::memory_order_acquire);
                if (name == nullptr)
                    continue;
                const long long spent = now_ms() - g_running_since_ms.load(std::memory_order_acquire);
                if (spent > static_cast<long long>(limit) * 1000) {
                    std::printf("[HANG] 场景 \"%s\" 已运行 %lld ms, 超过看门狗 %d s\n", name, spent, limit);
                    std::fflush(stdout);
                    std::abort();
                }
            }
        }).detach();
    }

    using clock_t_ = std::chrono::steady_clock;

    clock_t_::time_point now() {
        return clock_t_::now();
    }

    // 规模缩放: 场景里所有"多少次"都过一遍, 保证 --scale 整体生效且不会缩到 0
    long long scaled(long long n) {
        const auto v = static_cast<long long>(std::llround(static_cast<double>(n) * g_scale));
        return v < 1 ? 1 : v;
    }

    // 延迟采样: 只记录单个场景内的操作耗时, 用于 p50/p95/p99 ——
    // 平均值会把长尾抹掉, 而长尾正是调度/唤醒问题最先暴露的地方。
    struct Latency {
        std::vector<double> us;

        void reserve(std::size_t n) { us.reserve(n); }

        void add(clock_t_::duration d) { us.push_back(std::chrono::duration<double, std::micro>(d).count()); }

        double pct(double p) const {
            if (us.empty())
                return 0.0;
            std::vector<double> sorted = us;
            const auto idx = static_cast<std::size_t>(p * static_cast<double>(sorted.size() - 1));
            std::nth_element(sorted.begin(), sorted.begin() + static_cast<std::ptrdiff_t>(idx), sorted.end());
            return sorted[idx];
        }

        std::string report() const {
            if (us.empty())
                return "无样本";
            char buf[160];
            std::snprintf(buf, sizeof(buf), "延迟 us: p50=%.0f p95=%.0f p99=%.0f max=%.0f (n=%zu)", pct(0.50),
                          pct(0.95), pct(0.99), pct(1.0), us.size());
            return buf;
        }
    };

    std::string fmt_num(long long v) {
        return std::to_string(v);
    }

    // 句柄 / 常驻内存基线: 泄漏类问题只能靠"跑完再量"发现, 计数本身不依赖被测模块
#if defined(_WIN32)
    unsigned long handle_count() {
        DWORD count = 0;
        GetProcessHandleCount(GetCurrentProcess(), &count);
        return static_cast<unsigned long>(count);
    }

    unsigned long long rss_bytes() {
        PROCESS_MEMORY_COUNTERS_EX pmc{};
        pmc.cb = sizeof(pmc);
        if (!K32GetProcessMemoryInfo(GetCurrentProcess(), reinterpret_cast<PROCESS_MEMORY_COUNTERS*>(&pmc),
                                     sizeof(pmc)))
            return 0;
        return static_cast<unsigned long long>(pmc.WorkingSetSize);
    }
#else
    unsigned long handle_count() {
        return 0;
    }

    unsigned long long rss_bytes() {
        return 0;
    }
#endif

    std::string fmt_mb(unsigned long long bytes) {
        char buf[64];
        std::snprintf(buf, sizeof(buf), "%.1f MB", static_cast<double>(bytes) / (1024.0 * 1024.0));
        return buf;
    }

    // 场景计时 + 判定: 构造时打印 [RUN], finish() 只在无异常且计数等于期望时打印 [OK]。
    // 计时与判定分开, 避免"跑完就报成功"掩盖漏调度或未取出的异常。
    struct Scenario {
        const char* name;
        clock_t_::time_point started = now();
        long long expected = 0;
        long long actual = 0;
        long long ops = 0; // 用于吞吐换算 (0 = 不计)
        std::string note;
        std::string error;

        explicit Scenario(const char* scenario_name) : name(scenario_name) {
            std::printf("[RUN ] %s\n", name);
            std::fflush(stdout);
            g_running_since_ms.store(now_ms(), std::memory_order_release);
            g_running.store(name, std::memory_order_release);
        }

        void fail(std::string why) {
            if (error.empty())
                error = std::move(why);
        }

        void finish() {
            const auto elapsed = now() - started;
            const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();
            if (error.empty() && actual == expected)
                std::printf("[OK  ] %8lld ms%s\n", static_cast<long long>(ms), throughput(elapsed).c_str());
            else {
                ++g_failures;
                std::printf("[FAIL] %8lld ms\n", static_cast<long long>(ms));
                if (!error.empty())
                    std::printf("       原因: %s\n", error.c_str());
                else
                    std::printf("       期望 %lld, 实际 %lld\n", expected, actual);
            }
            if (!note.empty())
                std::printf("       %s\n", note.c_str());
            // 逐场景 flush: 重定向到文件时 stdout 是全缓冲, 一旦后续场景挂住,
            // 文件里看不到"挂在哪里"的最后一行。
            g_running.store(nullptr, std::memory_order_release);
            std::fflush(stdout);
        }

      private:
        std::string throughput(clock_t_::duration elapsed) const {
            if (ops <= 0)
                return {};
            const double secs = std::chrono::duration<double>(elapsed).count();
            if (secs <= 0.0)
                return {};
            char buf[64];
            std::snprintf(buf, sizeof(buf), "  %8.0f ops/s", static_cast<double>(ops) / secs);
            return buf;
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

    // ── 资源池 / 有界队列: 配额与背压 ──

    coro::Task<int> pool_make(int v) {
        co_return v;
    }

    coro::Task<> pool_worker(coro::Pool<int>* pool, int iters, std::atomic<int>* in_flight, std::atomic<int>* peak,
                             std::atomic<int>* bad) {
        for (int i = 0; i < iters; ++i) {
            auto lease = co_await pool->acquire();
            if (!lease.valid()) {
                ++*bad;
                co_return;
            }
            const int cur = ++*in_flight;
            int prev = peak->load();
            while (cur > prev && !peak->compare_exchange_weak(prev, cur)) {
            }
            co_await coro::yield(); // 放大并发窗口: 超过 max_agents 就会在这里看到峰值拔高
            --*in_flight;
        } // lease 析构即归还
    }

    coro::Task<> scene_pool(int workers, int iters, std::size_t max_agents, std::atomic<int>* peak,
                            std::atomic<int>* bad) {
        coro::Pool<int> pool([]() -> coro::Task<int> { return pool_make(1); },
                             coro::Pool<int>::options{max_agents, max_agents});
        std::atomic<int> in_flight{0};
        std::vector<coro::Task<>> tasks;
        tasks.reserve(static_cast<std::size_t>(workers));
        for (int i = 0; i < workers; ++i)
            tasks.push_back(pool_worker(&pool, iters, &in_flight, peak, bad));
        for (auto& t : tasks)
            t.start();
        for (auto& t : tasks)
            co_await std::move(t);
    }

    coro::Task<> q_consumer_slow(coro::Queue<int>* q, int n, int64_t* sum) {
        for (int i = 0; i < n; ++i) {
            *sum += co_await q->get();
            if ((i & 15) == 0)
                co_await coro::yield(); // 周期性放慢消费者, 逼出"队列满 → put 挂起"
        }
    }

    coro::Task<> scene_queue_backpressure(int n, int capacity, int64_t* sum) {
        coro::Queue<int> q(static_cast<std::size_t>(capacity));
        auto p = coro::spawn(q_producer(&q, n));
        auto c = coro::spawn(q_consumer_slow(&q, n, sum));
        co_await std::move(p);
        co_await std::move(c);
    }

    coro::Task<int> roundtrip_value(int value) {
        co_return value;
    }

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

    // ── core: 定时器 / 让出 / 队列 / spawn 往返 ──

    coro::Task<> scene_sleeps(std::atomic<int64_t>* done, int n) {
        std::vector<coro::Task<>> tasks;
        tasks.reserve(static_cast<std::size_t>(n));
        for (int i = 0; i < n; ++i)
            tasks.push_back(tiny_sleep(1, done));
        for (auto& t : tasks)
            t.start();
        for (auto& t : tasks)
            co_await std::move(t);
    }

    coro::Task<> scene_spread_timers(std::atomic<int64_t>* done, int n) {
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

    // 多生产者/多消费者: 单队列上的唤醒竞争 + 总量守恒 (旧场景只有 1P1C)
    coro::Task<> mp_producer(coro::Queue<int>* q, int per) {
        for (int i = 0; i < per; ++i)
            co_await q->put(1);
    }

    coro::Task<> mp_consumer(coro::Queue<int>* q, int per, std::atomic<int64_t>* got) {
        for (int i = 0; i < per; ++i) {
            (void)co_await q->get();
            ++*got;
        }
    }

    coro::Task<> scene_queue_mpmc(int producers, int consumers, int per, std::atomic<int64_t>* got) {
        coro::Queue<int> q;
        std::vector<coro::Task<>> tasks;
        tasks.reserve(static_cast<std::size_t>(producers + consumers));
        for (int i = 0; i < producers; ++i)
            tasks.push_back(mp_producer(&q, per));
        for (int i = 0; i < consumers; ++i)
            tasks.push_back(mp_consumer(&q, per, got));
        for (auto& t : tasks)
            t.start();
        for (auto& t : tasks)
            co_await std::move(t);
    }

    coro::Task<> scene_spawn_roundtrip(int n, int64_t* sum) {
        *sum = 0;
        auto t = spawn_roundtrip(n, sum);
        co_await std::move(t);
    }

    // 1ms 定时器的实际抖动: 定时器堆/等待轮询做错时这里会明显超时
    coro::Task<> scene_sleep_drift(int rounds, double* worst_ms, double* total_ms) {
        const auto t0 = now();
        double worst = 0.0;
        for (int i = 0; i < rounds; ++i) {
            const auto s = now();
            co_await coro::sleep(std::chrono::milliseconds(1));
            const auto took = std::chrono::duration<double, std::milli>(now() - s).count();
            worst = std::max(worst, took);
        }
        *worst_ms = worst;
        *total_ms = std::chrono::duration<double, std::milli>(now() - t0).count();
    }

    // 取消风暴: 大量长睡眠被组取消, 必须全部观察到取消并收尾 (漏一个就是泄漏/挂死)
    coro::Task<> cancel_victim(std::atomic<int64_t>* cancelled) {
        try {
            co_await coro::sleep(std::chrono::seconds(5));
        } catch (const coro::CancelledError&) {
            ++*cancelled;
        }
    }

    coro::Task<> scene_cancel_storm(int n, std::atomic<int64_t>* cancelled, std::size_t* pending_after) {
        coro::TaskGroup group;
        for (int i = 0; i < n; ++i)
            group.spawn(cancel_victim(cancelled));
        co_await coro::yield(); // 让子任务真正挂起到定时器上再取消
        group.cancel();
        try {
            co_await group.wait();
        } catch (...) {
            // 取消是预期路径; 组只在子任务抛出非取消异常时才抛 ExceptionGroup, 由计数区分。
        }
        *pending_after = group.pending_count();
    }

    // ── sync: Lock / Semaphore / Event / rate_limiter ──

    coro::Task<> lock_worker(coro::Lock* lock, int iters, int64_t* counter) {
        for (int i = 0; i < iters; ++i) {
            co_await lock->acquire();
            ++*counter; // 临界区: 单 loop 下不带锁也不会错, 这里验证的是 acquire/release 路径本身
            lock->release();
        }
    }

    coro::Task<> scene_lock_contention(int workers, int iters, int64_t* counter) {
        coro::Lock lock;
        std::vector<coro::Task<>> tasks;
        tasks.reserve(static_cast<std::size_t>(workers));
        for (int i = 0; i < workers; ++i)
            tasks.push_back(lock_worker(&lock, iters, counter));
        for (auto& t : tasks)
            t.start();
        for (auto& t : tasks)
            co_await std::move(t);
    }

    coro::Task<> sem_worker(coro::Semaphore* sem, int iters, std::atomic<int>* in_flight, std::atomic<int>* peak) {
        for (int i = 0; i < iters; ++i) {
            co_await sem->acquire();
            const int cur = ++*in_flight;
            int prev = peak->load();
            while (cur > prev && !peak->compare_exchange_weak(prev, cur)) {
            }
            co_await coro::yield(); // 放大并发窗口: 上限若失效这里必然超发
            --*in_flight;
            sem->release();
        }
    }

    coro::Task<> scene_semaphore(int workers, int iters, int permits, std::atomic<int>* peak) {
        coro::Semaphore sem(permits);
        std::atomic<int> in_flight{0};
        std::vector<coro::Task<>> tasks;
        tasks.reserve(static_cast<std::size_t>(workers));
        for (int i = 0; i < workers; ++i)
            tasks.push_back(sem_worker(&sem, iters, &in_flight, peak));
        for (auto& t : tasks)
            t.start();
        for (auto& t : tasks)
            co_await std::move(t);
    }

    coro::Task<> event_waiter(coro::Event* ev, std::atomic<int64_t>* woken) {
        co_await ev->wait();
        ++*woken;
    }

    coro::Task<> scene_event_broadcast(int waiters, std::atomic<int64_t>* woken, std::atomic<bool>* fired) {
        coro::Event ev;
        std::vector<coro::Task<>> tasks;
        tasks.reserve(static_cast<std::size_t>(waiters));
        for (int i = 0; i < waiters; ++i)
            tasks.push_back(event_waiter(&ev, woken));
        for (auto& t : tasks)
            t.start();
        for (int i = 0; i < 4; ++i)
            co_await coro::yield(); // 让等待者全部入队, 否则测的是"set 后立刻返回"
        ev.set();
        *fired = true;
        for (auto& t : tasks)
            co_await std::move(t);
    }

    coro::Task<> scene_rate_limit(int tokens, int capacity, int period_ms, double* elapsed_ms) {
        coro::rate_limiter rl(static_cast<std::uint64_t>(capacity),
                              std::chrono::milliseconds(period_ms)); // 每 period 补满一桶
        const auto t0 = now();
        for (int i = 0; i < tokens; ++i)
            co_await rl.acquire();
        *elapsed_ms = std::chrono::duration<double, std::milli>(now() - t0).count();
    }

    // ── channel / select ──

    coro::Task<> ch_producer(coro::channel<int>::sender tx, int n) {
        for (int i = 0; i < n; ++i)
            co_await tx.send(i);
    }

    coro::Task<> ch_consumer(coro::channel<int>::receiver rx, int n, int64_t* sum) {
        for (int i = 0; i < n; ++i) {
            auto v = co_await rx.recv();
            if (!v)
                co_return;
            *sum += *v;
        }
    }

    coro::Task<> scene_channel_throughput(int n, int capacity, int64_t* sum) {
        auto ch = coro::channel<int>::bounded(capacity);
        auto p = coro::spawn(ch_producer(ch.make_sender(), n));
        auto c = coro::spawn(ch_consumer(ch.make_receiver(), n, sum));
        co_await std::move(p);
        co_await std::move(c);
    }

    // 多接收者竞争: 每个值只能被取一次 (重复或丢失都是交接逻辑的 bug)
    coro::Task<> ch_race_consumer(coro::channel<int>::receiver rx, int n, std::vector<std::atomic<int>>* seen) {
        for (int i = 0; i < n; ++i) {
            auto v = co_await rx.recv();
            if (!v)
                co_return;
            ++(*seen)[static_cast<std::size_t>(*v)];
        }
    }

    coro::Task<> scene_channel_race(int receivers, int total, int* bad) {
        auto ch = coro::channel<int>::bounded(8);
        std::vector<std::atomic<int>> seen(static_cast<std::size_t>(total));
        for (auto& s : seen)
            s.store(0);
        const int per = total / receivers;
        std::vector<coro::Task<>> tasks;
        tasks.reserve(static_cast<std::size_t>(receivers) + 1);
        for (int i = 0; i < receivers; ++i)
            tasks.push_back(ch_race_consumer(ch.make_receiver(), per, &seen));
        for (auto& t : tasks)
            t.start();
        auto p = coro::spawn(ch_producer(ch.make_sender(), per * receivers));
        for (auto& t : tasks)
            co_await std::move(t);
        co_await std::move(p);

        int dup = 0;
        for (const auto& s : seen)
            if (s.load() > 1)
                ++dup;
        *bad = dup;
    }

    coro::Task<> select_loop(coro::channel<int>::receiver a, coro::channel<int>::receiver b, int rounds, int* from_a,
                             int* from_b) {
        int ca = 0, cb = 0;
        int empty_streak = 0;
        // 两个通道各发 rounds 个值, 必须全部收到 (只收 rounds 个会漏掉一半, 发送方随后撞上关闭通道)
        while (ca + cb < rounds * 2) {
            auto r =
                co_await coro::select(coro::recv_of(a), coro::recv_of(b), coro::after(std::chrono::milliseconds(50)));
            if (r.timed_out)
                break;
            if (r.value)
                empty_streak = 0;
            else if (++empty_streak > 1000)
                break; // 通道已 EOF 且无新值: 退出去让计数不匹配报失败, 而不是空转挂死
            if (r.index == 0 && r.value)
                ++ca;
            else if (r.index == 1 && r.value)
                ++cb;
        }
        *from_a = ca;
        *from_b = cb;
    }

    coro::Task<> select_feeder(coro::channel<int>::sender a, coro::channel<int>::sender b, int rounds) {
        for (int i = 0; i < rounds; ++i) {
            co_await a.send(i);
            co_await b.send(i);
        }
    }

    coro::Task<> scene_select_storm(int rounds, int* from_a, int* from_b) {
        auto cha = coro::channel<int>::bounded(4);
        auto chb = coro::channel<int>::bounded(4);
        // 故意多留一个发送端: 否则 feeder 一结束, 最后一个 sender 释放就会关闭通道,
        // 循环会在 EOF 分支上空转 (这曾经是压测程序挂死的根因)。
        auto keep_a = cha.make_sender();
        auto keep_b = chb.make_sender();
        auto feed = coro::spawn(select_feeder(cha.make_sender(), chb.make_sender(), rounds));
        co_await select_loop(cha.make_receiver(), chb.make_receiver(), rounds, from_a, from_b);
        // 收尾必须是确定性的: select 因超时提前退出时, 发送端可能还挂着,
        // 直接 co_await 会永久阻塞。
        feed.cancel();
        try {
            co_await std::move(feed);
        } catch (...) {
            // 取消/关闭通道都是预期结束方式
        }
    }

#ifdef CORO_STRESS_HAVE_NET
    // ── net: 回环 TCP 吞吐 / accept 风暴 (IOCP & io_uring) ──

    coro::Task<> echo_session(coro::net::TcpStream conn, std::atomic<int>* errs, std::atomic<int64_t>* served) {
        std::vector<char> buf(16 * 1024);
        for (;;) {
            const int r = co_await conn.read(buf.data(), buf.size());
            if (r < 0) {
                ++*errs;
                break;
            }
            if (r == 0)
                break; // 对端关闭
            const int w = co_await conn.write(buf.data(), static_cast<std::size_t>(r));
            if (w != r) {
                ++*errs;
                break;
            }
            *served += r;
        }
        conn.close();
    }

    coro::Task<> echo_server(coro::net::TcpListener* listener, int clients, std::atomic<int>* errs,
                             std::atomic<int64_t>* served) {
        coro::TaskGroup sessions;
        for (int i = 0; i < clients; ++i) {
            auto conn = co_await listener->accept();
            if (!conn.valid()) {
                ++*errs;
                continue;
            }
            sessions.spawn(echo_session(std::move(conn), errs, served));
        }
        try {
            co_await sessions.wait();
        } catch (...) {
            ++*errs;
        }
    }

    coro::Task<> echo_client(unsigned short port, int rounds, int payload, std::vector<double>* lat,
                             std::atomic<int>* errs, std::atomic<int64_t>* moved) {
        auto conn = co_await coro::net::TcpStream::connect("127.0.0.1", port);
        if (!conn.valid()) {
            ++*errs;
            co_return;
        }
        std::vector<char> out(static_cast<std::size_t>(payload), 'x');
        std::vector<char> in(static_cast<std::size_t>(payload));
        for (int i = 0; i < rounds; ++i) {
            const auto t0 = now();
            const int w = co_await conn.write(out.data(), out.size());
            if (w != payload) {
                ++*errs;
                break;
            }
            std::size_t got = 0;
            while (got < in.size()) {
                const int r = co_await conn.read(in.data() + got, in.size() - got);
                if (r <= 0) {
                    ++*errs;
                    got = 0;
                    break;
                }
                got += static_cast<std::size_t>(r);
            }
            if (got != in.size())
                break;
            lat->push_back(std::chrono::duration<double, std::micro>(now() - t0).count());
            *moved += 2 * static_cast<int64_t>(payload);
        }
        conn.close();
    }

    coro::Task<> scene_tcp_echo(int clients, int rounds, int payload, std::atomic<int>* errs,
                                std::atomic<int64_t>* moved, std::vector<double>* lat) {
        coro::net::TcpListener listener;
        if (!listener.bind_listen("127.0.0.1", 0)) { // 端口 0: 交给系统分配, 避免和并行测试抢端口
            ++*errs;
            co_return;
        }
        const unsigned short port = listener.local_port();
        auto srv = coro::spawn(echo_server(&listener, clients, errs, moved));

        std::vector<coro::Task<>> clients_tasks;
        clients_tasks.reserve(static_cast<std::size_t>(clients));
        for (int i = 0; i < clients; ++i)
            clients_tasks.push_back(echo_client(port, rounds, payload, lat, errs, moved));
        for (auto& t : clients_tasks)
            t.start();
        for (auto& t : clients_tasks)
            co_await std::move(t);
        co_await std::move(srv);
        listener.close();
    }

    coro::Task<> accept_all(coro::net::TcpListener* listener, int conns, std::atomic<int>* accepted,
                            std::atomic<int>* errs) {
        for (int i = 0; i < conns; ++i) {
            auto conn = co_await listener->accept();
            if (!conn.valid()) {
                ++*errs;
                continue;
            }
            ++*accepted;
            conn.close(); // 立刻关闭: 压的是 accept 队列与句柄回收
        }
    }

    coro::Task<> connect_and_close(unsigned short port, std::atomic<int>* errs) {
        auto conn = co_await coro::net::TcpStream::connect("127.0.0.1", port);
        if (!conn.valid())
            ++*errs;
        conn.close();
    }

#if defined(_WIN32) // 仅场景 32 使用 (见其门控注释)
    coro::Task<> accept_one_of(coro::net::TcpListener* listener, int conns, std::atomic<int>* accepted,
                               std::atomic<int>* errs) {
        // 无固定份额: 多个 acceptor 抢同一个连接数, 避免整除余数丢掉尾部的连接
        try {
            while (accepted->load(std::memory_order_acquire) < conns) {
                auto conn = co_await listener->accept();
                if (!conn.valid()) {
                    ++*errs;
                    continue;
                }
                ++*accepted;
                conn.close();
            }
        } catch (const coro::CancelledError&) {
            // 连接已经收完, 剩下的 acceptor 还在阻塞 accept 上: 由调用方取消
        }
    }

    // 同时挂 N 个 accept: 验证"预提交多个 AcceptEx"能否把串行往返摊开
    coro::Task<> scene_accept_parallel(int conns, int inflight, std::atomic<int>* accepted, std::atomic<int>* errs) {
        coro::net::TcpListener listener;
        if (!listener.bind_listen("127.0.0.1", 0)) {
            ++*errs;
            co_return;
        }
        const unsigned short port = listener.local_port();
        // acceptor 用 TaskGroup 管: 收满后还有人在阻塞 accept 上, 必须显式取消才能退出
        coro::TaskGroup group;
        for (int i = 0; i < inflight; ++i)
            group.spawn(accept_one_of(&listener, conns, accepted, errs));

        std::vector<coro::Task<>> clients;
        clients.reserve(static_cast<std::size_t>(conns));
        for (int i = 0; i < conns; ++i)
            clients.push_back(connect_and_close(port, errs));
        for (auto& t : clients)
            t.start();
        for (auto& t : clients)
            co_await std::move(t);
        group.cancel();
        try {
            co_await group.wait();
        } catch (...) {
            ++*errs;
        }
        listener.close();
    }
#endif // _WIN32 (accept_one_of / scene_accept_parallel)

    coro::Task<> connect_close_counted(unsigned short port, std::atomic<int>* ok, std::atomic<int>* errs) {
        auto conn = co_await coro::net::TcpStream::connect("127.0.0.1", port);
        if (!conn.valid()) {
            ++*errs;
            co_return;
        }
        ++*ok;
        conn.close();
    }

    // 只测客户端建连+关闭, 服务端不 accept (连接堆在监听 backlog 里):
    // 用来把"每连接 1ms"归因到 connect 侧还是 accept 侧。
    coro::Task<> scene_connect_only(int conns, std::atomic<int>* ok, std::atomic<int>* errs) {
        coro::net::TcpListener listener;
        if (!listener.bind_listen("127.0.0.1", 0, 1024)) {
            ++*errs;
            co_return;
        }
        const unsigned short port = listener.local_port();
        std::vector<coro::Task<>> clients;
        clients.reserve(static_cast<std::size_t>(conns));
        for (int i = 0; i < conns; ++i)
            clients.push_back(connect_close_counted(port, ok, errs));
        for (auto& t : clients)
            t.start();
        for (auto& t : clients)
            co_await std::move(t);
        listener.close();
    }

    // 前置声明: accept_keep 定义在本节靠后的位置, 场景 35 要先用到
    coro::Task<> accept_keep(coro::net::TcpListener* listener, int conns, std::vector<coro::net::TcpStream>* keep,
                             std::atomic<int>* errs);

    coro::Task<> connect_hold(unsigned short port, std::vector<coro::net::TcpStream>* keep, std::atomic<int>* ok,
                              std::atomic<int>* errs) {
        auto conn = co_await coro::net::TcpStream::connect("127.0.0.1", port);
        if (!conn.valid()) {
            ++*errs;
            co_return;
        }
        ++*ok;
        keep->push_back(std::move(conn)); // 不关闭: 把"关闭/收尾"从测量里拿掉
    }

    // 建连+accept 但全部保持不关: 区分"握手/accept"与"socket 收尾"的开销
    coro::Task<> scene_connect_hold(int conns, int wave, std::atomic<int>* ok, std::atomic<int>* errs) {
        coro::net::TcpListener listener;
        if (!listener.bind_listen("127.0.0.1", 0, 1024)) {
            ++*errs;
            co_return;
        }
        const unsigned short port = listener.local_port();
        std::vector<coro::net::TcpStream> server_side;
        std::vector<coro::net::TcpStream> client_side;
        server_side.reserve(static_cast<std::size_t>(conns));
        client_side.reserve(static_cast<std::size_t>(conns));
        auto srv = coro::spawn(accept_keep(&listener, conns, &server_side, errs));
        for (int done = 0; done < conns; done += wave) {
            const int batch_n = conns - done < wave ? conns - done : wave;
            std::vector<coro::Task<>> batch;
            batch.reserve(static_cast<std::size_t>(batch_n));
            for (int i = 0; i < batch_n; ++i)
                batch.push_back(connect_hold(port, &client_side, ok, errs));
            for (auto& t : batch)
                t.start();
            for (auto& t : batch)
                co_await std::move(t);
        }
        co_await std::move(srv);
        // 统一在最后关闭: 收尾成本不计入上面的建连耗时
        server_side.clear();
        client_side.clear();
        listener.close();
    }

    coro::Task<> scene_accept_waves(int conns, int wave, std::atomic<int>* accepted, std::atomic<int>* errs) {
        coro::net::TcpListener listener;
        if (!listener.bind_listen("127.0.0.1", 0)) {
            ++*errs;
            co_return;
        }
        const unsigned short port = listener.local_port();
        auto srv = coro::spawn(accept_all(&listener, conns, accepted, errs));
        for (int done = 0; done < conns; done += wave) {
            const int lower = conns - done < wave ? conns - done : wave;
            std::vector<coro::Task<>> batch;
            batch.reserve(static_cast<std::size_t>(lower));
            for (int i = 0; i < lower; ++i)
                batch.push_back(connect_and_close(port, errs));
            for (auto& t : batch)
                t.start();
            for (auto& t : batch)
                co_await std::move(t);
        }
        co_await std::move(srv);
        listener.close();
    }

    coro::Task<> scene_accept_storm(int conns, std::atomic<int>* accepted, std::atomic<int>* errs) {
        coro::net::TcpListener listener;
        if (!listener.bind_listen("127.0.0.1", 0)) {
            ++*errs;
            co_return;
        }
        const unsigned short port = listener.local_port();
        auto srv = coro::spawn(accept_all(&listener, conns, accepted, errs));
        std::vector<coro::Task<>> clients;
        clients.reserve(static_cast<std::size_t>(conns));
        for (int i = 0; i < conns; ++i)
            clients.push_back(connect_and_close(port, errs));
        for (auto& t : clients)
            t.start();
        for (auto& t : clients)
            co_await std::move(t);
        co_await std::move(srv);
        listener.close();
    }
#endif // CORO_STRESS_HAVE_NET

#ifdef CORO_STRESS_HAVE_NATIVE_IO
    // ── process: 进程 churn + 句柄回收 ──

    coro::Task<> process_batch(int n, int* failures) {
        for (int i = 0; i < n; ++i) {
#if defined(_WIN32)
            auto [code, out] = co_await coro::process::run_capture({"cmd", "/c", "echo coro-stress"});
#else
            auto [code, out] = co_await coro::process::run_capture({"sh", "-c", "echo coro-stress"});
#endif
            if (code != 0 || out.find("coro-stress") == std::string::npos)
                ++*failures;
        }
    }

    // 两批对比: 第一批包含线程池/等待注册的一次性预热, 第二批才是稳态。
    // 只有第二批仍在增长才能判定为"每轮泄漏", 否则会误报预热开销。
    coro::Task<> scene_process_churn(int n, int* bad, std::atomic<unsigned long>* h0, std::atomic<unsigned long>* h1,
                                     std::atomic<unsigned long>* h2) {
#if defined(_WIN32)
        h0->store(handle_count());
#endif
        int failures = 0;
        co_await process_batch(n, &failures);
#if defined(_WIN32)
        h1->store(handle_count());
#endif
        co_await process_batch(n, &failures);
#if defined(_WIN32)
        h2->store(handle_count());
#endif
        *bad = failures;
    }
#endif // CORO_STRESS_HAVE_NATIVE_IO

    // ── threads: 多线程 / Scheduler ──

    // 已就绪任务的批次调度: 全部先 start 再统一 await, 压的是就绪队列的批量 entry/exit
    coro::Task<> scene_ready_storm(int n, int64_t* sum) {
        std::vector<coro::Task<int>> tasks;
        tasks.reserve(static_cast<std::size_t>(n));
        for (int i = 0; i < n; ++i)
            tasks.push_back(roundtrip_value(i));
        for (auto& t : tasks)
            t.start();
        for (auto& t : tasks)
            *sum += co_await std::move(t);
    }

    // 嵌套 TaskGroup: 取消必须逐层传播到最深处的等待者, 每层都要正常收尾
    coro::Task<> nested_group(int depth, std::atomic<int64_t>* cancelled, std::atomic<bool>* armed) {
        if (depth == 0) {
            // 先声明"已就位"再挂起: 外层才能确定性地等到底层真正挂在定时器上, 而不是靠猜 yield 次数
            armed->store(true, std::memory_order_release);
            try {
                co_await coro::sleep(std::chrono::seconds(5));
            } catch (const coro::CancelledError&) {
                ++*cancelled;
            }
            co_return;
        }
        coro::TaskGroup inner;
        inner.spawn(nested_group(depth - 1, cancelled, armed));
        try {
            co_await inner.wait();
        } catch (...) {
            // 深层取消逐层展开, 这里只负责不让异常穿出嵌套层
        }
    }

    coro::Task<> scene_nested_cancel(int depth, std::atomic<int64_t>* cancelled, std::atomic<bool>* armed,
                                     std::atomic<int>* errs) {
        coro::TaskGroup root;
        root.spawn(nested_group(depth, cancelled, armed));
        // 每层 spawn+wait 至少占一拍调度, 等 enough 拍直到底层就位 (上限防死循环)
        for (int i = 0; i < depth * 8 + 64 && !armed->load(std::memory_order_acquire); ++i)
            co_await coro::yield();
        co_await coro::yield(); // 就位后还要一拍才真正提交为定时器等待
        root.cancel();
        try {
            co_await root.wait();
        } catch (...) {
            ++*errs;
        }
    }

    // move-only 元素过通道: 重复交付会直接 double free, 比 int 通道更能暴露所有权 bug
    coro::Task<> move_producer(coro::channel<std::unique_ptr<int>>::sender tx, int n) {
        for (int i = 0; i < n; ++i)
            co_await tx.send(std::make_unique<int>(i));
    }

    coro::Task<> move_consumer(coro::channel<std::unique_ptr<int>>::receiver rx, int n, int64_t* sum, int* bad) {
        for (int i = 0; i < n; ++i) {
            auto v = co_await rx.recv();
            if (!v || !*v) {
                ++*bad;
                co_return;
            }
            *sum += **v;
        }
    }

    coro::Task<> scene_channel_move(int n, int64_t* sum, int* bad) {
        auto ch = coro::channel<std::unique_ptr<int>>::bounded(16);
        auto p = coro::spawn(move_producer(ch.make_sender(), n));
        auto c = coro::spawn(move_consumer(ch.make_receiver(), n, sum, bad));
        co_await std::move(p);
        co_await std::move(c);
    }

#ifdef CORO_STRESS_HAVE_NATIVE_IO
    // ── io: 文件读写 (IOCP / io_uring 异步文件路径) ──

    coro::Task<> fs_file_roundtrip(int id, int rounds, int payload, std::atomic<int>* bad,
                                   std::atomic<int64_t>* bytes) {
        const std::string data(static_cast<std::size_t>(payload), 'c');
        const std::string path = "coro_stress_fs_tmp_" + std::to_string(id) + ".bin";
        for (int i = 0; i < rounds; ++i) {
            const bool wrote = co_await coro::fs::write_all(path, data);
            auto back = co_await coro::fs::read_all(path);
            if (!wrote || back.size() != data.size())
                ++*bad;
            *bytes += static_cast<int64_t>(back.size());
        }
        std::remove(path.c_str());
    }

    coro::Task<> scene_fs_io(int writers, int rounds, int payload, std::atomic<int>* bad, std::atomic<int64_t>* bytes) {
        std::vector<coro::Task<>> tasks;
        tasks.reserve(static_cast<std::size_t>(writers));
        for (int i = 0; i < writers; ++i)
            tasks.push_back(fs_file_roundtrip(i, rounds, payload, bad, bytes));
        for (auto& t : tasks)
            t.start();
        for (auto& t : tasks)
            co_await std::move(t);
    }

    void run_fsio() {
        {
            Scenario s("场景 29: 文件读写往返 (8 路并发 × 200 轮 × 64KB)");
            std::atomic<int> bad{0};
            std::atomic<int64_t> bytes{0};
            const int writers = 8;
            const int rounds = static_cast<int>(scaled(200));
            auto task = scene_fs_io(writers, rounds, 64 * 1024, &bad, &bytes);
            drive(task, s);
            s.expected = 0;
            s.actual = bad.load();
            s.ops = static_cast<long long>(writers) * rounds;
            s.note = "校验失败轮次: " + fmt_num(bad.load()) + ", 字节: " + fmt_num(bytes.load());
            s.finish();
        }
    }
#endif // CORO_STRESS_HAVE_NATIVE_IO

#ifdef CORO_STRESS_HAVE_NET
    // 把 echo 场景包一层计数器: 线程里拿不到 vec, 只汇总往返次数与字节
    coro::Task<> echo_counted(int clients, int rounds, int payload, std::atomic<int>* errs, std::atomic<int64_t>* bytes,
                              std::atomic<int64_t>* rtts) {
        std::vector<double> lat;
        lat.reserve(static_cast<std::size_t>(clients) * static_cast<std::size_t>(rounds));
        co_await scene_tcp_echo(clients, rounds, payload, errs, bytes, &lat);
        *rtts += static_cast<int64_t>(lat.size());
    }

    // 每个线程自己的 EventLoop + 自己的监听端口: 证明吞吐能随线程数扩展 (单 loop 饱和点对照)
    void run_echo_threads(int threads, int clients, int rounds, int payload, std::atomic<int>* errs,
                          std::atomic<int64_t>* bytes, std::atomic<int64_t>* rtts) {
        std::vector<std::thread> pool;
        pool.reserve(static_cast<std::size_t>(threads));
        for (int i = 0; i < threads; ++i) {
            pool.emplace_back([errs, bytes, rtts, clients, rounds, payload, i, pinned = g_pin_cores] {
#if defined(_WIN32)
                // 绑核: 验证 4 loop 折损是调度迁移还是共享内存带宽
                if (pinned)
                    SetThreadAffinityMask(GetCurrentThread(), static_cast<DWORD_PTR>(1) << (i % 8));
#else
                (void)pinned;
#endif
                auto task = echo_counted(clients, rounds, payload, errs, bytes, rtts);
                task.start();
                coro::EventLoop::get().run();
                if (!task.is_ready()) {
                    ++*errs;
                    return;
                }
                try {
                    task.take_result();
                } catch (...) {
                    ++*errs;
                }
            });
        }
        for (auto& t : pool)
            t.join();
    }

    coro::Task<> hanging_read(coro::net::TcpStream conn, std::atomic<int64_t>* cancelled, std::atomic<int>* errs) {
        char buf[64];
        try {
            const int r = co_await conn.read(buf, sizeof(buf)); // 对端不发数据 → 永久挂起
            (void)r;
            ++*errs; // 不应被正常唤醒
        } catch (const coro::CancelledError&) {
            ++*cancelled;
        }
        conn.close();
    }

    coro::Task<> accept_keep(coro::net::TcpListener* listener, int conns, std::vector<coro::net::TcpStream>* keep,
                             std::atomic<int>* errs) {
        for (int i = 0; i < conns; ++i) {
            auto conn = co_await listener->accept();
            if (!conn.valid()) {
                ++*errs;
                continue;
            }
            keep->push_back(std::move(conn));
        }
    }

    coro::Task<> connect_silent(unsigned short port, std::atomic<int>* errs, std::vector<coro::net::TcpStream>* keep) {
        auto conn = co_await coro::net::TcpStream::connect("127.0.0.1", port);
        if (!conn.valid()) {
            ++*errs;
            co_return;
        }
        keep->push_back(std::move(conn)); // 只建立连接, 不发数据: 把服务端 read 留在挂起态
    }

    // 挂起中的 IOCP read 被批量取消: 压 CancelIoEx 路径 + 句柄回收
    coro::Task<> scene_io_cancel_storm(int conns, std::atomic<int64_t>* cancelled, std::atomic<int>* errs,
                                       std::atomic<unsigned long>* h0, std::atomic<unsigned long>* h1) {
        coro::net::TcpListener listener;
        if (!listener.bind_listen("127.0.0.1", 0)) {
            ++*errs;
            co_return;
        }
        const unsigned short port = listener.local_port();
        std::vector<coro::net::TcpStream> server_side, client_side;
        server_side.reserve(static_cast<std::size_t>(conns));
        client_side.reserve(static_cast<std::size_t>(conns));
        h0->store(handle_count());

        auto srv = coro::spawn(accept_keep(&listener, conns, &server_side, errs));
        std::vector<coro::Task<>> clients;
        clients.reserve(static_cast<std::size_t>(conns));
        for (int i = 0; i < conns; ++i)
            clients.push_back(connect_silent(port, errs, &client_side));
        for (auto& t : clients)
            t.start();
        for (auto& t : clients)
            co_await std::move(t);
        co_await std::move(srv);

        coro::TaskGroup readers;
        for (auto& conn : server_side)
            readers.spawn(hanging_read(std::move(conn), cancelled, errs));
        co_await coro::yield(); // 让所有 read 真正提交到 IOCP
        readers.cancel();
        try {
            co_await readers.wait();
        } catch (...) {
            ++*errs;
        }
        client_side.clear();
        listener.close();
        h1->store(handle_count());
    }

    void run_net() {
        {
            Scenario s("场景 9: TCP 回环 echo 吞吐 (N 连接 × 300 轮 × 4KB)");
            std::atomic<int> errs{0};
            std::atomic<int64_t> moved{0};
            Latency lat;
            const int conns = g_net_conns;
            lat.reserve(static_cast<std::size_t>(conns) * 300);
            auto task = scene_tcp_echo(conns, static_cast<int>(scaled(300)), 4096, &errs, &moved, &lat.us);
            drive(task, s);
            s.expected = 0;
            s.actual = errs.load();
            s.ops = lat.us.size();
            s.note = "往返: " + fmt_num(static_cast<long long>(lat.us.size())) + ", 字节: " + fmt_num(moved.load()) +
                     ", " + lat.report();
            s.finish();
        }
        {
            Scenario s("场景 10: accept 风暴 (1000 连接快速建立/关闭)");
            std::atomic<int> accepted{0};
            std::atomic<int> errs{0};
            const int conns = static_cast<int>(scaled(1000));
            auto task = scene_accept_storm(conns, &accepted, &errs);
            drive(task, s);
            s.expected = conns;
            s.actual = accepted.load();
            s.ops = conns;
            s.note = "错误数: " + fmt_num(errs.load());
            if (errs.load() != 0)
                s.fail("accept/connect 出现错误: " + fmt_num(errs.load()));
            s.finish();
        }
#if defined(_WIN32)
        // 观测性场景 (不因收不满判定失败): "同 fd 多挂起 accept 的完成包
        // 会全部投递"这一前提在两个平台的高负载下都不可依赖 ——
        // Linux/io_uring (ubuntu-24.04, 内核 6.8) 每挂起 acceptor 只完成首个;
        // Windows/IOCP 在前序场景遗留大量 TIME_WAIT/句柄时同样间歇性只完成
        // 首个 (约 1/3 复现, 零错误)。accept 功能的可靠覆盖是串行重提交
        // (场景 9/10); 本场景只记录多挂起在当前系统上的实际完成量。
        // 详见 docs/quality-status.md "尚未宣称的能力"。
        {
            Scenario s("场景 32: accept 预提交并发 (32 个 accept 同时挂着, 1000 连接) [观测]");
            std::atomic<int> accepted{0};
            std::atomic<int> errs{0};
            const int conns = static_cast<int>(scaled(1000));
            auto task = scene_accept_parallel(conns, 32, &accepted, &errs);
            drive(task, s);
            s.expected = 0; // 只报错误数; 完成量受内核/负载影响, 不作合同
            s.actual = errs.load();
            s.ops = conns;
            s.note = "accepted " + fmt_num(accepted.load()) + "/" + fmt_num(conns) + ", 错误数: " +
                     fmt_num(errs.load());
            if (errs.load() != 0)
                s.fail("并发 accept 出现错误: " + fmt_num(errs.load()));
            s.finish();
        }
#endif
        {
            // Windows 的 listen backlog 实际被内核限制在 ~200 (AFD\Params\Backlog),
            // 所以这里只用 150 个并发建连: 超过 backlog 的连接会直接被内核丢弃,
            // 测到的是 SYN 重传等待而不是本库的开销。
            Scenario s("场景 34: 纯建连+关闭 (150 连接, 服务端不 accept)");
            std::atomic<int> ok{0};
            std::atomic<int> errs{0};
            const int conns = static_cast<int>(scaled(150));
            auto task = scene_connect_only(conns, &ok, &errs);
            drive(task, s);
            s.expected = conns;
            s.actual = ok.load();
            s.ops = conns;
            s.note = "错误数: " + fmt_num(errs.load());
            s.finish();
        }
        {
            Scenario s("场景 33: accept 波浪式建连 (同时最多 64 个, 1000 连接)");
            std::atomic<int> accepted{0};
            std::atomic<int> errs{0};
            const int conns = static_cast<int>(scaled(1000));
            auto task = scene_accept_waves(conns, 64, &accepted, &errs);
            drive(task, s);
            s.expected = conns;
            s.actual = accepted.load();
            s.ops = conns;
            s.note = "错误数: " + fmt_num(errs.load());
            if (errs.load() != 0)
                s.fail("波浪建连出现错误: " + fmt_num(errs.load()));
            s.finish();
        }
        {
            Scenario s("场景 35: 建连+accept 且保持在线 (1000 条, 每批 150 并发)");
            std::atomic<int> ok{0};
            std::atomic<int> errs{0};
            const int conns = static_cast<int>(scaled(1000));
            auto task = scene_connect_hold(conns, 150, &ok, &errs);
            drive(task, s);
            s.expected = conns;
            s.actual = ok.load();
            s.ops = conns;
            s.note = "错误数: " + fmt_num(errs.load());
            s.finish();
        }
        {
            Scenario s("场景 27: 挂起 IOCP read 批量取消 (200 连接)");
            std::atomic<int64_t> cancelled{0};
            std::atomic<int> errs{0};
            std::atomic<unsigned long> h0{0}, h1{0};
            const int conns = static_cast<int>(scaled(200));
            auto task = scene_io_cancel_storm(conns, &cancelled, &errs, &h0, &h1);
            drive(task, s);
            s.expected = conns;
            s.actual = cancelled.load();
            s.ops = conns;
            const long leak = static_cast<long>(h1.load()) - static_cast<long>(h0.load());
            s.note = "句柄 " + fmt_num(static_cast<long long>(h0.load())) + " → " +
                     fmt_num(static_cast<long long>(h1.load())) + " (" + fmt_num(leak) +
                     "), 异常: " + fmt_num(errs.load());
            // 连接句柄必须随取消/关闭全部归还; 留 16 个余量吸收运行时缓存
            if (leak > 16)
                s.fail("取消后套接字句柄未回收: " + fmt_num(leak));
            s.finish();
        }
        {
            Scenario s("场景 36: 小包 echo (32 连接 × 300 轮 × 64B, 验证 Nagle 影响)");
            std::atomic<int> errs{0};
            std::atomic<int64_t> moved{0};
            Latency lat;
            lat.reserve(32 * 300);
            auto task = scene_tcp_echo(32, static_cast<int>(scaled(300)), 64, &errs, &moved, &lat.us);
            drive(task, s);
            s.expected = 0;
            s.actual = errs.load();
            s.ops = static_cast<long long>(lat.us.size());
            s.note = "往返: " + fmt_num(static_cast<long long>(lat.us.size())) + ", " + lat.report();
            s.finish();
        }
        {
            // 每 loop 的并发量与场景 9 完全一致 (32 连接 × 300 轮):
            // 只有同负载才能把"4 loop 聚合吞吐 / 单 loop 吞吐"当作扩展比来读。
            Scenario s("场景 28: 4 loop 并行 echo 扩展性 (4 线程 × 32 连接 × 300 轮)");
            std::atomic<int> errs{0};
            std::atomic<int64_t> bytes{0};
            std::atomic<int64_t> rtts{0};
            const int threads = 4;
            const int clients = 32;
            const int rounds = static_cast<int>(scaled(300));
            run_echo_threads(threads, clients, rounds, 4096, &errs, &bytes, &rtts);
            s.expected = static_cast<long long>(threads) * clients * rounds;
            s.actual = rtts.load();
            s.ops = rtts.load();
            s.note = "字节: " + fmt_num(bytes.load()) + ", 错误: " + fmt_num(errs.load()) +
                     (g_pin_cores ? ", 已绑核" : ", 未绑核");
            if (errs.load() != 0)
                s.fail("多 loop echo 出现错误: " + fmt_num(errs.load()));
            s.finish();
        }
    }
#endif

    void run_core() {
        {
            Scenario s("场景 1: 10 万协程同时 sleep(1ms)");
            std::atomic<int64_t> done{0};
            const int n = static_cast<int>(scaled(100000));
            auto task = scene_sleeps(&done, n);
            drive(task, s);
            s.expected = n;
            s.actual = done.load();
            s.ops = n;
            s.finish();
        }
        {
            Scenario s("场景 2: 10 万定时器 (1~10ms 分散)");
            std::atomic<int64_t> done{0};
            const int n = static_cast<int>(scaled(100000));
            auto task = scene_spread_timers(&done, n);
            drive(task, s);
            s.expected = n;
            s.actual = done.load();
            s.ops = n;
            s.finish();
        }
        {
            Scenario s("场景 3: yield 风暴 (4 协程 × 100 万次)");
            std::atomic<int64_t> count{0};
            const int n = static_cast<int>(scaled(1000000));
            auto task = scene_yield_storm(&count, n);
            drive(task, s);
            s.expected = 4LL * n;
            s.actual = count.load();
            s.ops = count.load();
            s.finish();
        }
        {
            Scenario s("场景 4: 队列吞吐 (100 万 put/get, 1P1C)");
            int64_t sum = 0;
            const int n = static_cast<int>(scaled(1000000));
            auto task = scene_queue_throughput(n, &sum);
            drive(task, s);
            s.expected = static_cast<long long>(n) * (n - 1) / 2;
            s.actual = sum;
            s.ops = n;
            s.note = "sum: " + fmt_num(sum);
            s.finish();
        }
        {
            Scenario s("场景 5: 队列竞争 (8P/8C, 每个 5 万)");
            std::atomic<int64_t> got{0};
            const int per = static_cast<int>(scaled(50000));
            auto task = scene_queue_mpmc(8, 8, per, &got);
            drive(task, s);
            s.expected = 8LL * per;
            s.actual = got.load();
            s.ops = got.load();
            s.finish();
        }
        {
            Scenario s("场景 6: spawn+await 往返 (10 万次)");
            int64_t sum = 0;
            const int n = static_cast<int>(scaled(100000));
            auto task = scene_spawn_roundtrip(n, &sum);
            drive(task, s);
            s.expected = static_cast<long long>(n) * (n - 1) / 2;
            s.actual = sum;
            s.ops = n;
            s.note = "sum: " + fmt_num(sum);
            s.finish();
        }
        {
            Scenario s("场景 7: 取消风暴 (2 万个长睡眠被组取消)");
            std::atomic<int64_t> cancelled{0};
            std::size_t pending = 1;
            const int n = static_cast<int>(scaled(20000));
            const auto rss0 = rss_bytes();
            auto task = scene_cancel_storm(n, &cancelled, &pending);
            drive(task, s);
            const auto rss1 = rss_bytes();
            const long long grow = static_cast<long long>(rss1) - static_cast<long long>(rss0);
            s.expected = n;
            s.actual = cancelled.load();
            s.note = "组内残留: " + fmt_num(static_cast<long long>(pending)) + ", 常驻内存 " + fmt_mb(rss0) + " → " +
                     fmt_mb(rss1);
            if (pending != 0)
                s.fail("取消后组内仍有未收尾的子任务: " + fmt_num(static_cast<long long>(pending)));
            // 定时器堆里被取消的条目必须随帧销毁回收; 20 万个框全泄也只几 MB,
            // 所以这里用 128MB 当"结构性问题"阈值, 避免共享 runner 上的噪声误报。
            if (grow > 128LL * 1024 * 1024)
                s.fail("取消后常驻内存未回落: +" + fmt_mb(static_cast<unsigned long long>(grow)));
            s.finish();
        }
        {
            Scenario s("场景 8: 1ms 定时器抖动 (2000 次串行 sleep)");
            double worst = 0.0, total = 0.0;
            const int n = static_cast<int>(scaled(2000));
            auto task = scene_sleep_drift(n, &worst, &total);
            drive(task, s);
            s.expected = 1;
            s.actual = 1;
            s.ops = n;
            char buf[200];
            std::snprintf(buf, sizeof(buf), "最差 %.2f ms, 总 %.1f ms, 平均 %.3f ms/次", worst, total,
                          total / static_cast<double>(n));
            s.note = buf;
            // 串行 1ms 睡眠的最差样本超过 100ms 说明定时器唤醒被饿死, 属于真实缺陷
            if (worst > 100.0)
                s.fail("单次 1ms 睡眠抖动超过 100ms: " + std::to_string(worst));
            s.finish();
        }
        {
            Scenario s("场景 24: 已就绪任务批次调度 (20 万 start + await)");
            int64_t sum = 0;
            const int n = static_cast<int>(scaled(200000));
            auto task = scene_ready_storm(n, &sum);
            drive(task, s);
            s.expected = static_cast<long long>(n) * (n - 1) / 2;
            s.actual = sum;
            s.ops = n;
            s.finish();
        }
        {
            Scenario s("场景 25: 嵌套 TaskGroup 取消传播 (深度 500)");
            std::atomic<int64_t> cancelled{0};
            std::atomic<int> errs{0};
            std::atomic<bool> armed{false};
            const int depth = static_cast<int>(scaled(500));
            auto task = scene_nested_cancel(depth, &cancelled, &armed, &errs);
            drive(task, s);
            s.expected = 1; // 只有一个最深层等待者, 取消必须送达且只送达一次
            s.actual = cancelled.load();
            s.note = "取消送达: " + fmt_num(cancelled.load()) + " / 深度 " + fmt_num(depth) +
                     ", 异常: " + fmt_num(errs.load());
            s.finish();
        }
        {
            Scenario s("场景 31: 有界队列背压 (容量 4, 1P1C 10 万)");
            int64_t sum = 0;
            const int n = static_cast<int>(scaled(100000));
            auto task = scene_queue_backpressure(n, 4, &sum);
            drive(task, s);
            s.expected = static_cast<long long>(n) * (n - 1) / 2;
            s.actual = sum;
            s.ops = n;
            s.note = "sum: " + fmt_num(sum);
            s.finish();
        }
    }

    void run_sync() {
        {
            Scenario s("场景 11: Lock 争用 (16 协程 × 2 万次获取/释放)");
            int64_t counter = 0;
            const int iters = static_cast<int>(scaled(20000));
            auto task = scene_lock_contention(16, iters, &counter);
            drive(task, s);
            s.expected = 16LL * iters;
            s.actual = counter;
            s.ops = counter;
            s.finish();
        }
        {
            Scenario s("场景 12: Semaphore 竞争 (16 协程 × 5000, 许可 4)");
            std::atomic<int> peak{0};
            const int iters = static_cast<int>(scaled(5000));
            auto task = scene_semaphore(16, iters, 4, &peak);
            drive(task, s);
            s.expected = 1;
            s.actual = 1;
            s.ops = 16LL * iters;
            s.note = "并发峰值: " + fmt_num(peak.load()) + " (上限 4)";
            // 超额许可说明 acquire/release 计数有竞态, 会直接把连接池/限流打穿
            if (peak.load() > 4)
                s.fail("Semaphore 许可上限被突破: 峰值 " + fmt_num(peak.load()));
            s.finish();
        }
        {
            Scenario s("场景 13: Event 广播唤醒 (5000 等待者)");
            std::atomic<int64_t> woken{0};
            std::atomic<bool> fired{false};
            const int n = static_cast<int>(scaled(5000));
            auto task = scene_event_broadcast(n, &woken, &fired);
            drive(task, s);
            s.expected = n;
            s.actual = woken.load();
            s.ops = woken.load();
            s.finish();
        }
        {
            Scenario s("场景 14: rate_limiter 令牌桶 (5000 令牌, 500/50ms)");
            double elapsed = 0.0;
            const int tokens = static_cast<int>(scaled(5000));
            auto task = scene_rate_limit(tokens, 500, 50, &elapsed);
            drive(task, s);
            s.expected = 1;
            s.actual = 1;
            s.ops = tokens;
            char buf[200];
            std::snprintf(buf, sizeof(buf), "耗时 %.1f ms (理论下限 %.0f ms)", elapsed,
                          static_cast<double>(tokens) / 500.0 * 50.0 - 50.0);
            s.note = buf;
            // 桶漏了 (发多了) 会显著快于理论下限; 这里留 20% 余量吸收最后一个周期
            const double floor_ms = static_cast<double>(tokens) / 500.0 * 50.0 * 0.8 - 50.0;
            if (elapsed < floor_ms)
                s.fail("限流未生效 (发得太快): " + std::to_string(elapsed) + " ms");
            s.finish();
        }
        {
            Scenario s("场景 30: 资源池配额争用 (16 协程 × 200 次 acquire, 上限 8)");
            std::atomic<int> peak{0};
            std::atomic<int> bad{0};
            const int workers = 16;
            const int iters = static_cast<int>(scaled(200));
            auto task = scene_pool(workers, iters, 8, &peak, &bad);
            drive(task, s);
            s.expected = 0;
            s.actual = bad.load();
            s.ops = static_cast<long long>(workers) * iters;
            s.note = "并发持有峰值: " + fmt_num(peak.load()) + " (上限 8), 无效句柄: " + fmt_num(bad.load());
            // 超过 max_agents 说明池的配额账算错了, 会直接把下游连接数打穿
            if (peak.load() > 8)
                s.fail("Pool 超出 max_agents: 峰值 " + fmt_num(peak.load()));
            s.finish();
        }
    }

    void run_channel() {
        {
            Scenario s("场景 15: channel 吞吐 (20 万, 容量 64, 1P1C)");
            int64_t sum = 0;
            const int n = static_cast<int>(scaled(200000));
            auto task = scene_channel_throughput(n, 64, &sum);
            drive(task, s);
            s.expected = static_cast<long long>(n) * (n - 1) / 2;
            s.actual = sum;
            s.ops = n;
            s.note = "sum: " + fmt_num(sum);
            s.finish();
        }
        {
            Scenario s("场景 16: channel rendezvous 交接 (5 万, 容量 1)");
            int64_t sum = 0;
            const int n = static_cast<int>(scaled(50000));
            auto task = scene_channel_throughput(n, 1, &sum);
            drive(task, s);
            s.expected = static_cast<long long>(n) * (n - 1) / 2;
            s.actual = sum;
            s.ops = n;
            s.finish();
        }
        {
            Scenario s("场景 17: channel 多接收者竞争 (8 接收者, 8 万值, 不得重复交付)");
            int dup = -1;
            const int total = static_cast<int>(scaled(80000));
            auto task = scene_channel_race(8, total, &dup);
            drive(task, s);
            s.expected = 0;
            s.actual = dup;
            s.ops = total;
            s.note = "重复交付值: " + fmt_num(dup);
            s.finish();
        }
        {
            Scenario s("场景 18: select 双通道 + 超时分支 (2 万轮)");
            int from_a = 0, from_b = 0;
            const int rounds = static_cast<int>(scaled(20000));
            auto task = scene_select_storm(rounds, &from_a, &from_b);
            drive(task, s);
            s.expected = 2LL * rounds; // 两条通道各 rounds 个值, 全部要收到
            s.actual = from_a + from_b;
            s.ops = from_a + from_b;
            s.note = "来自 A: " + fmt_num(from_a) + ", 来自 B: " + fmt_num(from_b);
            // 两条分支都该被选到; 全落在同一条说明选择逻辑退化成固定优先级
            if (from_a == 0 || from_b == 0)
                s.fail("select 只走了单分支 (A=" + fmt_num(from_a) + ", B=" + fmt_num(from_b) + ")");
            s.finish();
        }
        {
            Scenario s("场景 26: channel move-only 元素 (10 万个 unique_ptr)");
            int64_t sum = 0;
            int bad = 0;
            const int n = static_cast<int>(scaled(100000));
            auto task = scene_channel_move(n, &sum, &bad);
            drive(task, s);
            s.expected = static_cast<long long>(n) * (n - 1) / 2;
            s.actual = sum;
            s.ops = n;
            s.note = "空值/丢失: " + fmt_num(bad);
            if (bad != 0)
                s.fail("move-only 元素丢失: " + fmt_num(bad));
            s.finish();
        }
    }

#ifdef CORO_STRESS_HAVE_NATIVE_IO
    void run_process() {
        {
            Scenario s("场景 19: 进程 churn ( 2 批 × 25 次 run_capture )");
            int bad = 0;
            std::atomic<unsigned long> h0{0}, h1{0}, h2{0};
            const int n = static_cast<int>(scaled(25));
            auto task = scene_process_churn(n, &bad, &h0, &h1, &h2);
            drive(task, s);
            s.expected = 0;
            s.actual = bad;
            s.ops = 2LL * n;
            const long warmup = static_cast<long>(h1.load()) - static_cast<long>(h0.load());
            const long steady = static_cast<long>(h2.load()) - static_cast<long>(h1.load());
            s.note = "失败: " + fmt_num(bad) + ", 句柄 " + fmt_num(static_cast<long long>(h0.load())) + " → " +
                     fmt_num(static_cast<long long>(h1.load())) + " → " + fmt_num(static_cast<long long>(h2.load())) +
                     " (首批 +" + fmt_num(warmup) + ", 稳态 +" + fmt_num(steady) + ")";
            // 首批增长含线程池/等待注册的一次性预热; 稳态还涨就是每轮泄漏
            if (steady > 4)
                s.fail("稳态句柄持续增长 (每轮泄漏): " + fmt_num(steady));
            s.finish();
        }
    }
#endif // CORO_STRESS_HAVE_NATIVE_IO

    void run_threads() {
        {
            Scenario s("场景 20: 4 线程 × 10 万协程 (每线程独立 loop)");
            constexpr int k_threads = 4;
            const int k_per_thread = static_cast<int>(scaled(100000));
            std::atomic<int64_t> done{0};
            std::atomic<int> stuck{0};
            // 异常记录在各自线程的槽位里, join 之后再展开 —— 不在线程存活期间抛出。
            std::vector<std::exception_ptr> errors(static_cast<std::size_t>(k_threads));
            std::vector<std::thread> threads;
            threads.reserve(static_cast<std::size_t>(k_threads));
            for (int i = 0; i < k_threads; ++i) {
                threads.emplace_back([&done, &stuck, &errors, k_per_thread, i] {
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
            s.ops = done.load();
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
            Scenario s("场景 21: Scheduler 自动分发 40 万协程 (4 worker)");
            std::atomic<int64_t> done{0};
            const int n = static_cast<int>(scaled(400000));
            coro::Scheduler sched(4);
            for (int i = 0; i < n; ++i)
                sched.spawn_any([&done] { return tiny_sleep(1, &done); });
            sched.wait_all();
            s.expected = n;
            s.actual = done.load();
            s.ops = n;
            s.finish();
        }
        {
            Scenario s("场景 22: Scheduler 均衡性 (4 worker 各收到任务)");
            coro::Scheduler sched(4);
            std::atomic<int64_t> per_worker[4]{};
            const int n = static_cast<int>(scaled(4000));
            for (int i = 0; i < n; ++i)
                sched.spawn_any([&sched, &per_worker] { return count_by_loop(&sched, &per_worker); });
            sched.wait_all();

            long long total = 0;
            std::string note = "各 worker 计数:";
            for (auto& c : per_worker) {
                total += c.load();
                note += " " + std::to_string(c.load());
            }
            // 只要求总数正确; 具体分布由调度决定, 不作为阈值。
            s.expected = n;
            s.actual = total;
            s.ops = n;
            s.note = std::move(note);
            s.finish();
        }
    }

    // ── soak: 混合负载长跑, 暴露计时器/队列/通道互相踩踏的问题 ──

    coro::Task<> soak_worker(coro::Queue<int>* q, coro::channel<int>* ch, std::chrono::milliseconds duration,
                             std::atomic<int64_t>* ops, std::atomic<int64_t>* sum, int id) {
        const auto deadline = now() + duration;
        auto tx = ch->make_sender();
        auto rx = ch->make_receiver();
        while (now() < deadline) {
            co_await coro::sleep(std::chrono::milliseconds(1));
            co_await q->put(id);
            const int v = co_await q->get();
            *sum += v;
            co_await tx.send(id);
            auto got = co_await rx.recv();
            if (got)
                *sum += *got;
            co_await coro::yield();
            ++*ops;
        }
    }

    coro::Task<> scene_soak(int workers, std::chrono::milliseconds duration, std::atomic<int64_t>* ops,
                            std::atomic<int64_t>* sum) {
        coro::Queue<int> q;
        coro::channel<int> ch = coro::channel<int>::bounded(64);
        std::vector<coro::Task<>> tasks;
        tasks.reserve(static_cast<std::size_t>(workers));
        for (int i = 0; i < workers; ++i)
            tasks.push_back(soak_worker(&q, &ch, duration, ops, sum, i));
        for (auto& t : tasks)
            t.start();
        for (auto& t : tasks)
            co_await std::move(t);
    }

    void run_soak() {
        Scenario s("场景 23: 混合负载 soak (16 协程 × N 秒: sleep+queue+channel+yield)");
        std::atomic<int64_t> ops{0};
        std::atomic<int64_t> sum{0};
        const auto duration = std::chrono::seconds(g_soak_seconds);
        auto task = scene_soak(16, duration, &ops, &sum);
        drive(task, s);
        s.expected = 1;
        s.actual = 1;
        s.ops = ops.load();
        s.note = "轮次: " + fmt_num(ops.load()) + ", 校验和: " + fmt_num(sum.load());
        if (ops.load() == 0)
            s.fail("soak 未产生任何轮次");
        s.finish();
    }

#ifdef CORO_STRESS_HAVE_HTTP
    // ── http: 客户端吞吐 / 连接复用 / 延迟 ──

    // 最小 HTTP/1.1 对端: 固定响应 + keep-alive。不引第三方服务器, 不影响客户端本身的判定。
    coro::Task<> http_serve_conn(coro::net::TcpStream conn, std::atomic<int>* errs, std::atomic<int64_t>* served) {
        static constexpr char k_body[] = "coro-stress-ok";
        const std::string response = std::string("HTTP/1.1 200 OK\r\nContent-Length: ") +
                                     std::to_string(sizeof(k_body) - 1) + "\r\nConnection: keep-alive\r\n\r\n" + k_body;
        char buf[4096];
        std::string pending;
        for (;;) {
            const int n = co_await conn.read(buf, sizeof(buf));
            if (n < 0) {
                ++*errs;
                break;
            }
            if (n == 0)
                break; // 客户端关闭
            pending.append(buf, static_cast<std::size_t>(n));
            std::size_t head_end;
            while ((head_end = pending.find("\r\n\r\n")) != std::string::npos) {
                pending.erase(0, head_end + 4); // GET 无请求体, 按头部边界切分即可
                const int w = co_await conn.write(response.data(), response.size());
                if (w != static_cast<int>(response.size())) {
                    ++*errs;
                    conn.close();
                    co_return;
                }
                ++*served;
            }
        }
        conn.close();
    }

    coro::Task<> http_accept_all(coro::net::TcpListener* listener, int conns, std::atomic<int>* errs,
                                 std::atomic<int64_t>* served) {
        coro::TaskGroup sessions;
        for (int i = 0; i < conns; ++i) {
            auto conn = co_await listener->accept();
            if (!conn.valid()) {
                ++*errs;
                continue;
            }
            sessions.spawn(http_serve_conn(std::move(conn), errs, served));
        }
        try {
            co_await sessions.wait();
        } catch (...) {
            ++*errs;
        }
    }

    coro::Task<> http_client_worker(coro::HttpClient* client, std::string url, int rounds, std::atomic<int>* bad,
                                    std::vector<double>* lat) {
        for (int i = 0; i < rounds; ++i) {
            const auto t0 = now();
            try {
                auto resp = co_await client->get(url); // keep-alive: 连接应被池复用
                if (resp.status != 200 || resp.body != "coro-stress-ok")
                    ++*bad;
            } catch (...) {
                ++*bad;
            }
            lat->push_back(std::chrono::duration<double, std::micro>(now() - t0).count());
        }
    }

    coro::Task<> scene_http(int conns, int rounds, std::atomic<int>* bad, std::atomic<int>* errs,
                            std::atomic<int64_t>* served, std::vector<double>* lat) {
        coro::net::TcpListener listener;
        if (!listener.bind_listen("127.0.0.1", 0)) {
            ++*errs;
            co_return;
        }
        const unsigned short port = listener.local_port();
        auto srv = coro::spawn(http_accept_all(&listener, conns, errs, served));

        coro::HttpClient::Options opts;
        opts.max_connections_per_origin = static_cast<std::size_t>(conns) + 4;
        opts.max_idle_per_origin = static_cast<std::size_t>(conns) + 4;
        opts.max_total_requests = static_cast<std::size_t>(conns) + 8;
        coro::HttpClient client(opts);
        const std::string url = "http://127.0.0.1:" + std::to_string(port) + "/health";

        std::vector<coro::Task<>> workers;
        workers.reserve(static_cast<std::size_t>(conns));
        for (int i = 0; i < conns; ++i)
            workers.push_back(http_client_worker(&client, url, rounds, bad, lat));
        for (auto& t : workers)
            t.start();
        for (auto& t : workers)
            co_await std::move(t);

        co_await client.aclose();
        listener.close();
        co_await std::move(srv); // 连接被关闭后 serve 循环自然退出
    }

    void run_http() {
        {
            Scenario s("场景 37: HTTP client keep-alive 吞吐 (32 并发 × 200 请求)");
            std::atomic<int> bad{0};
            std::atomic<int> errs{0};
            std::atomic<int64_t> served{0};
            Latency lat;
            const int conns = 32;
            const int rounds = static_cast<int>(scaled(200));
            lat.reserve(static_cast<std::size_t>(conns) * static_cast<std::size_t>(rounds));
            auto task = scene_http(conns, rounds, &bad, &errs, &served, &lat.us);
            drive(task, s);
            s.expected = 0;
            s.actual = bad.load();
            s.ops = lat.us.size();
            s.note = "成功: " + fmt_num(static_cast<long long>(lat.us.size())) +
                     ", 服务端响应: " + fmt_num(served.load()) + ", 错误: " + fmt_num(errs.load()) + ", " +
                     lat.report();
            if (errs.load() != 0)
                s.fail("HTTP 服务端/客户端出现错误: " + fmt_num(errs.load()));
            s.finish();
        }
    }
#endif // CORO_STRESS_HAVE_HTTP

    struct Group {
        const char* name;
        void (*run)();
    };

    const Group k_groups[] = {
        {"core", run_core},       {"sync", run_sync},       {"channel", run_channel},
#ifdef CORO_STRESS_HAVE_NATIVE_IO
        {"io", run_fsio},
#endif
#ifdef CORO_STRESS_HAVE_NET
        {"net", run_net},
#endif
#ifdef CORO_STRESS_HAVE_NATIVE_IO
        {"process", run_process},
#endif
        {"threads", run_threads}, {"soak", run_soak},
#ifdef CORO_STRESS_HAVE_HTTP
        {"http", run_http},
#endif
    };

    void print_groups() {
        std::printf("可用分组:");
        for (const auto& g : k_groups)
            std::printf(" %s", g.name);
        std::printf(" (缺省=全部)\n");
    }

} // namespace

int main(int argc, char** argv) {
    const char* filter = nullptr;
    for (int i = 1; i < argc; ++i) {
        const char* a = argv[i];
        if (std::strncmp(a, "--scale=", 8) == 0) {
            g_scale = std::atof(a + 8);
            if (g_scale <= 0.0)
                g_scale = 1.0;
        } else if (std::strncmp(a, "--soak=", 7) == 0) {
            g_soak_seconds = std::atoi(a + 7);
            if (g_soak_seconds <= 0)
                g_soak_seconds = 3;
        } else if (std::strncmp(a, "--watchdog=", 11) == 0) {
            g_watchdog_seconds = std::atoi(a + 11);
        } else if (std::strcmp(a, "--pin") == 0) {
            g_pin_cores = true;
        } else if (std::strncmp(a, "--net-conns=", 12) == 0) {
            g_net_conns = std::atoi(a + 12);
            if (g_net_conns <= 0)
                g_net_conns = 32;
        } else if (std::strcmp(a, "--list") == 0) {
            print_groups();
            return 0;
        } else if (a[0] != '-') {
            filter = a;
        } else {
            std::printf("未知参数: %s\n", a);
            print_groups();
            return 2;
        }
    }

    std::printf("=== coro 高并发压力测试 (scale=%.2f, soak=%ds) ===\n", g_scale, g_soak_seconds);
    if (g_watchdog_seconds > 0)
        start_watchdog();
    const auto t0 = now();
    bool matched = false;
    for (const auto& g : k_groups) {
        if (filter != nullptr && std::strcmp(filter, g.name) != 0)
            continue;
        matched = true;
        g.run();
    }
    if (!matched) {
        std::printf("没有匹配的分组: %s\n", filter);
        print_groups();
        return 2;
    }
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now() - t0).count();
    if (g_failures != 0) {
        std::printf("=== %d 个场景失败 (总耗时 %lld ms) ===\n", g_failures, static_cast<long long>(elapsed));
        return 1;
    }
    std::printf("=== 全部场景通过 (总耗时 %lld ms) ===\n", static_cast<long long>(elapsed));
    return 0;
}
