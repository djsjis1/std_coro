# 第 10 讲 — 综合实战：并发文件处理服务

> 本讲把前 9 讲的所有组件拼成一个完整、可靠、可扩展的程序：
> **一个并发文件处理服务** —— 监视目录中的新文件 → 限并发处理 →
> 汇总落盘 → 支持 TCP 查询进度 → Ctrl+C 优雅停机。
> 全部代码可整体复制到 `main.cpp`（协程练习场）直接运行。

涉及技术点清单：

| 技术点 | 来自 |
|---|---|
| Task / spawn / TaskGroup | 第 1-3 讲 |
| 超时（wait_for）与取消 | 第 3 讲 |
| Queue 流水线 / Semaphore | 第 4 讲 |
| to_thread（阻塞内核出桥） | 第 5 讲 |
| TCP 查询端点 | 第 6 讲 |
| fs / fs_watch / signal | 第 7 讲 |
| 优雅停机编排 | 第 7、9 讲 |

---

## 10.1 需求与架构

**需求**

1. 监视 `./incoming` 目录：出现新 `.txt` 文件就处理；
2. 处理 = 异步读文件 → 统计行数/字节数（CPU 工作，进线程池）→
   结果追加到 `summary.jsonl`；
3. 最多 4 个文件并发处理（Semaphore）；
4. TCP 端口 7777 提供 `stats` 查询；
5. Ctrl+C：停止接收新文件 → 排空队列处理完手头的 → 干净退出。

**架构**

```
fs::watch("incoming") ──事件──> 收集协程 ──路径──> Queue(容量 8)
                                                    │
                                    worker × 4 (Semaphore 限并发)
                                    read_all → to_thread(统计) → 追加写入
                                                    │
                              Stats (atomic 计数) <──更新──┘
                                                    │
TCP :7777 查询协程 <──读 Stats       Ctrl+C ──> signal::wait ──> 停产+排空+收割
```

## 10.2 完整代码（可整体复制运行）

```cpp
// project.cpp — 并发文件处理服务
#include <coro/coro.hpp>
#include <coro/fs.hpp>
#include <coro/fs_watch.hpp>
#include <coro/net.hpp>
#include <coro/signal.hpp>

#include <atomic>
#include <chrono>
#include <iostream>
#include <string>
#include <vector>

using namespace std::chrono_literals;

// ---------------------------------------------------------------------------
// 共享状态: 原子计数 (worker/查询端点/主协程都会读写)
// ---------------------------------------------------------------------------
struct Stats
{
    std::atomic<int> processed{0};
    std::atomic<int> in_flight{0};
    std::atomic<long long> total_bytes{0};
};
static Stats g_stats;

// ---------------------------------------------------------------------------
// 单文件处理: 异步读 → to_thread 统计 → 追加写结果   (第 5 + 7 讲)
// ---------------------------------------------------------------------------
coro::Task<> process_one(std::string path)
{
    g_stats.in_flight++;

    std::string data = co_await coro::fs::read_all(path);       // 异步读
    if (data.empty() && coro::io::last_error() != 0)
        throw std::runtime_error("读取失败: " + path);

    // CPU 统计放线程池, 不卡事件循环 (第 5 讲)
    auto stat = co_await coro::to_thread([data] {
        int lines = 1;
        for (char c : data) if (c == '\n') ++lines;
        return std::make_pair(lines, (long long)data.size());
    });

    std::string record = "{\"file\": \"" + path + "\", \"lines\": " +
                         std::to_string(stat.first) + ", \"bytes\": " +
                         std::to_string(stat.second) + "}\n";

    // 追加写: append 模式下 write_at 的 offset 被忽略 (OS 原子追加)
    coro::fs::File out = co_await coro::fs::open(
        "summary.jsonl",
        coro::fs::mode::append | coro::fs::mode::create);
    if (out.valid()) {
        co_await out.write_at(record.data(), record.size(), 0);
        out.close();
    }

    g_stats.processed++;
    g_stats.total_bytes += stat.second;
    g_stats.in_flight--;
}

// ---------------------------------------------------------------------------
// 常驻 worker: 从队列取路径 → 限并发 → 处理   (第 3 + 4 讲)
// ---------------------------------------------------------------------------
coro::Task<> worker_loop(coro::Queue<std::string>& q, coro::Semaphore& sem)
{
    try {
        while (true) {
            std::string path = co_await q.get();               // 空则挂起
            {
                auto g = co_await sem.guard();                 // 最多 4 并发
                try {
                    co_await coro::wait_for(process_one(path), 5s);  // 单文件超时
                } catch (const coro::TimeoutError&) {
                    std::cout << "[worker] 超时: " << path << std::endl;
                } catch (const std::exception& e) {
                    std::cout << "[worker] 失败: " << path
                              << " (" << e.what() << ")" << std::endl;
                }
            }
            q.task_done();                                     // join 协议记账
        }
    } catch (const coro::CancelledError&) {
        // 停机: TaskGroup 析构取消 worker 时走到这里, 无需清理 (栈自动展开)
    }
}

// ---------------------------------------------------------------------------
// TCP 查询端点 :7777   (第 6 讲)
// ---------------------------------------------------------------------------
coro::Task<> handle_query(coro::net::TcpStream conn)
{
    char buf[64];
    int n = co_await conn.read(buf, sizeof(buf) - 1);
    if (n > 0) {
        std::string cmd(buf, n);
        std::string reply;
        if (cmd.find("stats") != std::string::npos)
            reply = "processed=" + std::to_string(g_stats.processed.load()) +
                    " in_flight=" + std::to_string(g_stats.in_flight.load()) +
                    " bytes=" + std::to_string(g_stats.total_bytes.load()) + "\n";
        else
            reply = "usage: stats\n";
        co_await conn.write(reply.data(), (int)reply.size());
    }
    conn.close();
}

coro::Task<> serve_stats()
{
    try {
        coro::net::TcpListener listener;
        if (!listener.bind_listen("127.0.0.1", 7777)) co_return;
        while (true) {
            auto conn = co_await listener.accept();
            if (!conn.valid()) break;
            co_await handle_query(std::move(conn));            // 查询端点串行即可
        }
    } catch (const coro::CancelledError&) {
        // 停机取消
    }
}

// ---------------------------------------------------------------------------
// 目录收集协程: watch 事件 → 过滤 .txt → 入队   (第 7 讲)
// ---------------------------------------------------------------------------
coro::Task<> collector_loop(coro::fs::DirectoryWatcher& w,
                            coro::Queue<std::string>& q,
                            coro::Event& stop)
{
    try {
        while (!stop.is_set()) {
            coro::fs::watch_event ev;
            try {
                // 200ms 窗口: 既响应事件又周期性检查停止标志
                ev = co_await coro::wait_for(w.next(), 200ms);
            } catch (const coro::TimeoutError&) {
                continue;
            }
            if (ev.type == coro::fs::watch_event_type::created &&
                ev.path.size() > 4 &&
                ev.path.compare(ev.path.size() - 4, 4, ".txt") == 0)
            {
                co_await q.put("incoming/" + ev.path);         // 满则背压挂起
                std::cout << "[收集] " << ev.path << " 入队" << std::endl;
            }
        }
    } catch (const coro::CancelledError&) {
    }
}

// ---------------------------------------------------------------------------
// 主编排: 组装流水线 + 优雅停机   (第 3 + 7 讲)
// ---------------------------------------------------------------------------
coro::Task<> main_flow()
{
    co_await coro::fs::write_all("incoming/.keep", "");        // 确保目录存在

    coro::Queue<std::string> q(8);                             // 有界队列 = 背压
    coro::Semaphore sem(4);
    coro::Event stop;

    // ① worker × 4 装进 TaskGroup: 作用域收敛, 析构自动取消
    coro::TaskGroup workers;
    for (int i = 0; i < 4; ++i)
        workers.spawn(worker_loop(q, sem));

    // ② 查询端点 + 收集协程
    auto stats_srv = coro::spawn(serve_stats());
    auto watcher   = co_await coro::fs::watch("incoming", false);
    auto collector = coro::spawn(collector_loop(watcher, q, stop));

    std::cout << "服务运行中: 监视 incoming/, 查询端口 7777, Ctrl+C 停机"
              << std::endl;

    // ③ 主流程挂起, 直到 Ctrl+C (Linux 亦可 SIGTERM)
    co_await coro::signal::wait(SIGINT);
    std::cout << "\n[停机] 收到信号, 停止接收, 排空队列..." << std::endl;

    // ④ 优雅停机四步: 停产 → 等收尾排空 → 收割常驻协程 → 关资源
    stop.set();                                 // 停产: collector 下个周期退出
    co_await std::move(collector);
    co_await q.join();                          // 排空: 已入队文件全部处理完
    stats_srv.cancel();                         // 收割: 查询端点退出
    co_await std::move(stats_srv);
    watcher.close();
    std::cout << "[停机] 共处理 " << g_stats.processed.load()
              << " 个文件, " << g_stats.total_bytes.load()
              << " 字节, 再见" << std::endl;
    // workers: main_flow 返回时 TaskGroup 析构自动取消 4 个常驻 worker
}

int main()
{
    try {
        coro::run(main_flow());
    } catch (const std::exception& e) {
        std::cerr << "fatal: " << e.what() << std::endl;
        return 1;
    }
    return 0;
}
```

## 10.3 逐段复盘

**单文件处理 `process_one`**：IO（read_all/write_at）留在事件循环，
CPU（统计）进 `to_thread`——这是 5.2 节"异步外壳 + 阻塞内核"的
直接应用。追加写用 `append` 模式的句柄：并发/多次写入都是原子追加，
不用自己管理写 offset。

**worker 循环**：三个保护层层叠加——

1. `q.get()` 空则挂起（不忙等）；
2. `sem.guard()` 限制全局并发 ≤ 4（队列里再多的文件也不会压垮磁盘）；
3. `wait_for(..., 5s)` 单文件超时，卡死的文件不拖垮整条流水线。

单文件的失败（异常）被 catch 住记日志，**不影响其他文件**。

**收集协程**：`wait_for(w.next(), 200ms)` 的"事件 + 轮询"混合模式——
事件来了立刻处理，200ms 没事件就检查一次停止标志。
`q.put` 满则背压：队列容量 8 限制了未处理文件的内存占用。

**停机四步**（本讲核心）：

| 步骤 | 工具 | 谁 |
|---|---|---|
| 停产 | `stop.set()`（Event） | collector 200ms 内退出 |
| 排空 | `q.join()` | 等 in-flight 元素全部 task_done |
| 收割 | `cancel()` + `co_await` | stats 服务退出 |
| 兜底 | `TaskGroup` 析构 | 4 个 worker 自动取消 |

每一步用的是哪个讲次里的哪个工具，全部对得上——这就是
"生命周期全部收敛"的含义：每个组件的生与死都有唯一负责人。

**为什么主流程靠 `signal::wait` 返回而不是被取消？**
`signal::wait` 是正常完成的 awaitable（返回信号编号），
Ctrl+C 让 `main_flow` 从挂起点继续往下走停机逻辑；
`main` 里的 `catch` 只兜底"worker 取消传播"等异常路径。

## 10.4 运行与验收

```bash
cmake --build build --target coro_practice
./build/Debug/coro_practice.exe
# 另开终端:
echo data1 > incoming/a.txt
echo more data2 > incoming/b.txt
# Windows (PowerShell):  借助 curl 查询
curl http://127.0.0.1:7777 --data "stats"     # 或用任意 TCP 工具发 stats
# 查看结果
cat summary.jsonl
# Ctrl+C 主程序 → 观察优雅停机输出
```

预期行为：

- 文件一落盘就被处理（毫秒级延迟）；
- 一次放 20 个文件：日志显示始终约 4 个在处理（Semaphore 生效）；
- `stats` 返回累计计数；
- Ctrl+C 后打印排空信息、退出码 0、无残留线程。

## 10.5 进阶方向（自己动手扩展）

- worker 数与 Semaphore 容度参数化，接命令行参数；
- 查询端点换成第 9 讲的 Web 框架，加 `/stats` JSON API；
- 处理逻辑接第 8 讲的 `Scheduler`：多核并行统计大文件；
- 失败文件进重试队列（二次 Queue + `call_later` 指数退避）；
- 给 watcher 加去抖：同路径 100ms 内多条事件合并成一次处理。

## 10.6 结语

到这里你已经能独立用 coro 写出产品级异步程序。接下来：

- 想知其所以然 → [C++20 协程课程](../cpp20-coroutines-course/README.md)
  + [架构与源码剖析](../architecture.md)；
- 忘 API → [API 参考手册](../api-reference.md)；
- 程序不对劲 → [FAQ 与故障排查](../faq.md)。
