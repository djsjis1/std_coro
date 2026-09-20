# coro 协程库完全指南

> C++20 协程事件循环框架 — 类似 Python asyncio 的使用体验。
> 本文档是 `USAGE.md`（API 参考）与 `cpp20-coroutines-guide.md`（入门教程）的合并总结版。
>
> 更多文档：[文档中心](README.md) ·
> [使用教程（10 讲）](tutorial/README.md) ·
> [API 参考手册](api-reference.md) ·
> [架构与源码剖析](architecture.md) ·
> [FAQ 与故障排查](faq.md)

- **零配置**：Windows 默认 IOCP，Linux 默认 io_uring，其他平台退化为纯标准库
- **Header-only**：`#include <coro/coro.hpp>` 即可；网络需额外 `#include <coro/net.hpp>`
- **依赖**：CMake 3.20+，MSVC 2022 / GCC 11+（<14 加 `-fcoroutines`）/ Clang 14+；Linux 网络层需 `liburing-dev`
- **模型**：单线程事件循环（类 asyncio 默认模式）+ 惰性启动的 `Task<T>`

---

## 一、协程解决什么问题（一分钟版）

| 方案 | 代码可读性 | 并发能力 | 线程开销 |
|---|---|---|---|
| 同步阻塞 | 好 | 差 | 1 线程/请求 |
| 回调 | 差（回调地狱） | 好 | 极少 |
| **协程** | **好（同步写法）** | **好** | **极少** |

核心思想：**挂起不占线程，恢复后接着往下写**。一个函数体出现 `co_await` / `co_yield` / `co_return` 任一关键字即为协程；其返回类型必须内嵌 `promise_type`。

```cpp
coro::Task<> fetch_all() {
    auto a = co_await download(url1);   // 挂起，线程去干别的
    auto b = co_await download(url2);   // 恢复后继续写，像同步代码
}
```

---

## 二、底层机制速览（帧 / 句柄 / promise / awaiter）

协程被调用时，编译器在堆上分配**协程帧**：

```
┌──────────────────────────────┐
│ 协程帧 (堆上)                 │
│  ├ 参数 & 跨挂起存活的局部变量  │
│  ├ 挂起点指针 (状态机状态)     │
│  └ promise 对象 (控制台)      │
└──────────────▲───────────────┘
               │ coroutine_handle (遥控器: resume / done / destroy)
        ┌──────┴──────┐
        │ Task 外壳对象 │ ← 你代码里拿到的
        └─────────────┘
```

**promise_type 五件套**（编译器按固定时机调用）：

| 成员 | 时机 | 作用 |
|---|---|---|
| `get_return_object()` | 创建时 | 用 `from_promise(*this)` 把句柄装进外壳返回 |
| `initial_suspend()` | 协程体前 | `suspend_never`=立即执行 / `suspend_always`=惰性等启动 |
| `final_suspend()` | 协程体后 | `suspend_never`=自动销毁帧 / `suspend_always`=帧保留待外部销毁 |
| `return_void()` / `return_value(v)` | `co_return` | 接收结果 |
| `unhandled_exception()` | 抛异常时 | `exception_ = std::current_exception()` 存起来 |

**awaiter 三件套**（`co_await` 任意实现这三者的对象）：

```cpp
struct Aw {
    bool await_ready();                       // ① 好了没？true 则不挂起
    void await_suspend(coroutine_handle<> h); // ② 挂起，拿到"被挂起协程"的句柄
    void await_resume();                      // ③ 恢复后取结果
};
```

- `await_suspend` 返回 `void`=挂起等别人 resume；`bool`=false 可继续；`coroutine_handle`=对称传输直接切换
- `co_yield v` 等价于 `co_await promise.yield_value(v)`（生成器机制）
- 异常不会直接飞到调用方：先入 `unhandled_exception()`，等待者在 `await_resume()` 时重新抛出

---

## 三、手写最小协程（教学示例）

```cpp
// 编译: g++ -std=c++20 x.cpp && ./a.out
#include <coroutine>
#include <iostream>

struct my_coro {
    struct promise_type {
        my_coro get_return_object() {
            return my_coro{std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        std::suspend_always initial_suspend() { return {}; }   // 惰性启动
        std::suspend_never final_suspend() noexcept { return {}; }
        void return_void() {}
        void unhandled_exception() {}
    };
    std::coroutine_handle<promise_type> h_;
    explicit my_coro(std::coroutine_handle<promise_type> h) : h_(h) {}
    ~my_coro() { if (h_) h_.destroy(); }
    void resume() { if (h_ && !h_.done()) h_.resume(); }
    bool done() const { return !h_ || h_.done(); }
};

my_coro counter() {
    for (int i = 1; i <= 3; ++i) {
        std::cout << i << " ";
        co_await std::suspend_always{};   // 每轮暂停
    }
}

int main() {
    my_coro c = counter();                // 创建即挂起
    while (!c.done()) c.resume();         // 手动驱动 → 1 2 3
}
```

生命周期四原则：**拷贝禁止**（双 destroy）、**移动转移**、**析构销毁帧**、**帧只能销毁一次**。

---

## 四、快速开始

```cpp
#include <coro/coro.hpp>
#include <iostream>
using namespace std::chrono_literals;

coro::Task<int> compute() {
    co_await coro::sleep(1s);       // 挂起 1 秒，不阻塞线程
    co_return 42;
}

coro::Task<> main_task() {
    int result = co_await compute();
    std::cout << result << std::endl;
}

int main() { coro::run(main_task()); }   // 启动事件循环，跑完自动退出
```

构建：

```bash
cmake -B build -A x64                 # Windows
cmake --build build --config Debug
# Linux: sudo apt install liburing-dev 后 cmake -B build && cmake --build build

# 运行单元测试 (googletest, 集成于 thirdparty/googletest-main)
ctest --test-dir build -C Debug --output-on-failure
# 或直接运行: build/Debug/coro_tests.exe
# 测试覆盖: Task/spawn/异常、取消语义、并发原语、同步原语、Future、调度、TCP、
#           文件 IO、管道、信号、目录监视、子进程 (128 用例)

# 高并发压力测试 (独立程序):
cmake --build build --config Release --target coro_stress
build/Release/coro_stress.exe
# 覆盖: 10 万协程并发 / 10 万定时器 / 400 万次 yield / 100 万队列吞吐
#       10 万 spawn 往返 / 4 线程 × 10 万协程并行
```

---

## 五、核心 API

### Task 与启动

| API | 说明 |
|---|---|
| `coro::Task<T>` | 协程返回类型；**惰性启动**（`co_await` 或 `start()` 才运行）；仅可移动 |
| `co_await task` | 串行等待，异常在此重新抛出 |
| `coro::spawn(task)` | 后台启动（等价 `asyncio.create_task`），⚠️ **必须保存返回值** |
| `t.start()` / `t.detach()` | 显式启动 / 放弃所有权（detach 任务异常会打印警告，回调可替换） |
| `coro::run(task)` | 启动事件循环直到任务完成，**返回主协程结果**（对标 `asyncio.run`；异常在 run 处重新抛出） |

### 时间与取消

```cpp
co_await coro::sleep(500ms);     // 挂起 500ms
co_await coro::yield();          // 让出 CPU（等价 sleep(0)）

auto t = coro::spawn(slow_task());
t.cancel();                      // 取消：协程在下一个 await 点收到 CancelledError
try {
    co_await std::move(t);
} catch (const coro::CancelledError&) {
    // 可在这里收尾；被取消协程体内的栈对象析构函数也已执行完毕
}
```

**取消语义（对标 Python）**：`cancel()` 后协程在下一个 await 点抛出 `CancelledError` 终止协程体，`finally` 风格的清理逻辑正常执行；无限循环任务也能被终止；挂起在 IOCP/io_uring 上的任务会先取消底层 I/O 再终止。请在事件循环线程调用。

---

## 六、并发 API

### TaskGroup — 结构化并发 (对标 Python 3.11)

```cpp
coro::Task<> main_task() {
    coro::TaskGroup group;
    group.spawn(fetch_a());   // 添加并启动子任务 (任意返回类型)
    group.spawn(fetch_b());
    group.spawn(fetch_c());
    try {
        co_await group.wait();       // 等待全部完成
    } catch (const coro::ExceptionGroup& eg) {
        // 任一失败 → 其余被自动取消; eg.exceptions() 逐个检查
    }
}
```

语义 (与 Python `asyncio.TaskGroup` 一致)：任一子任务失败 → 自动取消其余 → 全部结束后抛 `ExceptionGroup`（单个异常也打包）；组内取消的 `CancelledError` 不聚合；忘记 `wait()` 时析构自动取消残留子任务。

```cpp
// 静态 gather：编译期数量，返回 tuple，总耗时 ≈ 最慢的那个
auto [users, posts] = co_await coro::gather(fetch_users(), fetch_posts());

// 动态 gather：运行时数量，同类型
std::vector<coro::Task<int>> tasks;
auto results = co_await coro::gather_all(std::move(tasks));   // vector<int>

// void 任务并发
co_await coro::gather_void(t1(), t2(), t3());

// 超时等待
try {
    auto r = co_await coro::wait_for(slow_task(), 500ms);
} catch (const coro::TimeoutError&) { /* 超时, 任务已被取消 */ }

// 两路竞速 (FIRST_COMPLETED)
int r = co_await coro::wait_any(fast_task(), slow_task());

// N 路 wait (对标 asyncio.wait 的 return_when):
std::vector<coro::Task<int>> tasks = {t1(), t2(), t3()};
auto first = co_await coro::wait_tasks(std::move(tasks), coro::WaitMode::FirstCompleted);
// WaitMode::FirstException — 任一失败立即抛 (其余后台继续)
// WaitMode::AllCompleted   — 全部完成后返回全部结果

// 线程池桥接 (对标 asyncio.to_thread):
int v = co_await coro::to_thread([] { return blocking_read(); });
```

> `gather` 不支持 `Task<void>`（void 不能进 tuple），用 `gather_void`。

---

## 七、同步原语与队列

```cpp
coro::Lock lock;                 // 互斥锁（FIFO 公平，支持递归获取）
co_await lock.acquire();
/* 临界区 */
lock.release();

// RAII 守卫 (对标 async with lock:, 异常路径也自动释放):
{
    auto g = co_await lock.guard();
    /* 临界区 */
}

coro::Semaphore sem(3);          // 信号量：最多 3 个协程并发
co_await sem.acquire();  /* ... */  sem.release();

coro::Event ev;                  // 一次性事件通知
co_await ev.wait();              // A 等待
ev.set();                        // B 触发（唤醒全部等待者）

coro::Condition cond(&lock);     // 条件变量 (对标 asyncio.Condition)
// 等待方 (必须先持锁; wait 返回时重新持锁):
{
    auto g = co_await lock.guard();
    while (!ready) co_await cond.wait();
}
// 通知方:
{
    auto g = co_await lock.guard();
    ready = true;
    cond.notify();               // 或 notify_all()
}

coro::Queue<int> q(10);          // 有界队列（默认无界）
co_await q.put(42);              // 满则挂起生产者
int v = co_await q.get();        // 空则挂起消费者
auto nv = q.get_nowait();        // 非阻塞取: 空 → nullopt
bool ok = q.put_nowait(42);      // 非阻塞放: 满 → false
// 生产者-消费者收尾 (对标 q.task_done / q.join):
q.task_done();                   // 处理完一个任务
co_await q.join();               // 挂起直到所有任务处理完

// 调试/监控 (对标 asyncio.all_tasks / current_task):
size_t n = coro::EventLoop::get().active_task_count();
auto cur = coro::EventLoop::current_task();   // 协程内非空
```

---

## 八、Promise / Future 桥接回调

```cpp
coro::Task<std::string> download_async(const std::string& url) {
    coro::Promise<std::string> promise;
    auto future = promise.get_future();
    legacy_api.download(url, [&promise](const std::string& data) {
        promise.set_value(data);            // 回调中完成
    });
    co_return co_await future;              // 挂起直到回调触发
}
```

- 提前 `set_value` 后 `co_await` 不挂起（快速路径）
- **多个协程可等待同一个 Future**（全部被唤醒）
- `set_value` / `set_exception` **可跨线程调用**（内部加锁 + 自动唤醒事件循环）
- 重复 set 抛 `std::logic_error`

---

## 九、TCP 网络（Windows IOCP / Linux io_uring）

```cpp
// 服务器
coro::Task<> echo_handler(coro::net::TcpStream conn) {
    char buf[1024];
    while (true) {
        int n = co_await conn.read(buf, sizeof(buf));
        if (n <= 0) break;                   // 对端关闭
        co_await conn.write(buf, n);         // 回显
    }
}

coro::Task<> main_task() {
    coro::net::TcpListener listener;
    if (!listener.bind_listen("127.0.0.1", 8888)) co_return;
    while (true) {
        auto conn = co_await listener.accept();
        // fire-and-forget: start + detach, 协程帧自持有到完成
        auto t = echo_handler(std::move(conn));  // 命名协程函数 (参数进帧)
        t.start();
        t.detach();
    }
}

// 客户端
auto conn = co_await coro::net::TcpStream::connect("127.0.0.1", 8888);
if (!conn.valid()) { /* 连接失败 */ }
co_await conn.write("hello", 5);
int n = co_await conn.read(buf, sizeof(buf));
conn.close();
```

错误约定：`read`/`write` 返回 -1 表示出错（`errno` 存平台错误码）；`read` 返回 0 表示对端关闭；`accept`/`connect` 失败返回 `valid() == false`。

---

## 十、IO 扩展：文件 / 管道 / 信号 / 目录监视 / 子进程

除 TCP 外, 库还提供五个 IO 模块, 全部构建在同一套平台事件源上
(Windows IOCP / Linux io_uring), 零配置、可取消、错误模型统一。
使用方式与 net.hpp 相同 —— 单独 include 对应头文件:

```cpp
#include <coro/fs.hpp>       // 异步文件 IO
#include <coro/pipe.hpp>     // 异步管道 (+ Linux fd 轮询)
#include <coro/signal.hpp>   // 信号事件
#include <coro/fs_watch.hpp> // 目录监视
#include <coro/process.hpp>  // 子进程
```

**统一错误约定** (所有 IO awaiter 一致): 返回 `int` 时 `>=0` 为字节数、
`-1` 为失败; 此时 `errno` 已转换为标准 errno 值 (strerror 可直接用),
平台原生码在 `coro::io::last_error()` (thread_local, co_await 返回后立即读)。
`read` 类返回 `0` 一律表示「对端关闭 / 文件尾」。

### 10.1 异步文件 IO — coro::fs

核心是**定位读写** (`read_at`/`write_at` 显式传 offset, 不共享文件游标),
因此同一个文件可以被多个协程并发分块读写, 这是异步文件 IO 最自然的形态:

```cpp
coro::Task<> demo() {
    // 便捷接口: 一次读写整个文件
    co_await coro::fs::write_all("data.bin", "hello");     // "w" 语义: 创建/截断
    std::string s = co_await coro::fs::read_all("data.bin");

    // 元信息 (同步: 元数据操作微秒级, 非热路径)
    auto st = coro::fs::stat("data.bin");
    // st.size / st.mtime_sec / st.is_dir / st.exists

    // 句柄式: 打开 → 定位读写 → 刷盘
    coro::fs::File f = co_await coro::fs::open(
        "data.bin", coro::fs::mode::write | coro::fs::mode::create);
    if (!f.valid()) { /* 失败: coro::io::last_error() */ }

    int n = co_await f.write_at("XYZ", 3, /*offset=*/100); // 写字节 100~102
    char buf[16];
    int m = co_await f.read_at(buf, sizeof(buf), 100);     // 读回
    co_await f.fsync();                                    // 刷盘
    f.close();

    // EOF 语义: 读到/越过文件尾返回 0 (不报错)
    int eof = co_await f.read_at(buf, sizeof(buf), st.size); // eof == 0

    // 并发分块读: 四个协程各读 1/4, gather 汇合 (定位读无游标竞争)
    auto&& [a, b, c, d] = co_await coro::gather(
        read_slice("data.bin", 0, st.size / 4),
        read_slice("data.bin", st.size / 4, st.size / 4), ...);
}
```

打开模式组合 (对标 fopen): `read`("r") / `write`("w", 隐含 create+truncate) /
`create` / `truncate` / `append`("a", write_at 的 offset 被忽略, 原子追加) /
`exclusive`(与 create 组合, 已存在则报错 —— 原子创建锁)。

平台说明: Windows 上 open/stat 是同步调用 (元数据操作, OS 已缓存),
read/write/fsync 全异步 (ReadFile/WriteFile + IOCP 完成包);
Linux 上 open/read/write/fsync 全部走 io_uring。挂起中的读写可被
`Task::cancel()` 取消 (与网络 IO 同一机制)。

### 10.2 异步管道 — coro::pipe

POSIX pipe 的协程化: 一条单向字节通道, 满写/空读自动挂起 (天然背压):

```cpp
coro::Task<> demo() {
    auto [rd, wr] = coro::pipe::pair();          // {读端, 写端}, 默认 64KB 缓冲

    // 典型分工: 写端在任意协程, 读端并发消费
    // 捕获型协程 lambda 可以使用；这里 producer_fn 的闭包一直活到
    // producer 被 await 完成，因此对 wr 的引用有效。
    auto producer_fn = [&wr]() -> coro::Task<> {
        for (int i = 0; i < 1000; i++)
            co_await wr.write("chunk;", 6);
        wr.close();                               // 写完关闭 → 读者收到 EOF
    };
    auto producer = coro::spawn(producer_fn());

    char buf[4096];
    while (true) {
        int n = co_await rd.read(buf, sizeof(buf));
        if (n == 0) break;                        // 写端已全部关闭
        if (n < 0)  break;                        // 错误 (io::last_error())
        process(buf, n);
    }
    co_await std::move(producer);
}
```

背压实测: 4KB 缓冲的管道连续写 16KB, 写满即挂起, 读者排空后写者被唤醒 ——
不需要任何手动流控。Windows 实现为命名管道对 (匿名管道不支持 OVERLAPPED,
无法接入 IOCP), Linux 为 `pipe2(O_NONBLOCK)` + io_uring。

Linux 专属: 任意 fd 的就绪轮询 (Windows socket 是完成制, 无此概念):

```cpp
uint32_t revents = co_await coro::io::poll(fd, POLLIN);  // 挂起到可读
```

### 10.3 信号事件 — coro::signal

对标 `asyncio` 的 `add_signal_handler`, 支持 SIGINT / SIGTERM / SIGBREAK / SIGHUP:

```cpp
coro::Task<> main_task() {
    // 1) 单次等待: 挂起直到信号到达, 多个协程可同时等
    int sig = co_await coro::signal::wait(SIGINT);

    // 2) 持续处理: 每次信号到达执行 factory() 返回的协程 (串行, 不重入)
    //    返回 RAII 注册对象, 析构/stop() 时注销
    coro::signal::handler h = coro::signal::handle(
        SIGTERM, [] { return shutdown_task(); });
}
```

Web 服务器优雅关停的完整写法 (examples 见 Web/main.cpp):

```cpp
coro::run([&](web_server& s) -> coro::Task<> {
    auto srv = coro::spawn(s.serve());            // accept 循环
    auto sig  = coro::signal::handle(SIGINT,  [&s] { return shutdown_task(s); });
    auto sig2 = coro::signal::handle(SIGBREAK, [&s] { return shutdown_task(s); });
    co_await std::move(srv);                      // 信号处理器调 s.stop() 后退出
    s.wait_all();                                 // 等连接协程收尾
}(server));
```

平台说明: Windows 无真信号 —— 控制台事件 (Ctrl+C/Ctrl+Break/关窗) 与
CRT `raise()` 双路桥接; SIGHUP 仅来自「控制台窗口关闭」事件, 无法经
`raise()` 触发。Linux 用 `sigaction + self-pipe + reader 线程`，
处理器只写管道，reader 再将通知路由到注册时的事件循环。

### 10.4 目录监视 — coro::fs::watch

对标 watchfiles / inotify 工具:

```cpp
coro::Task<> demo() {
    // Windows 和 Linux 都支持递归子目录
    auto w = co_await coro::fs::watch("src", /*recursive=*/true);
    while (true) {
        coro::fs::watch_event ev = co_await w.next();   // 挂起到下一个事件
        switch (ev.type) {
        case coro::fs::watch_event_type::created:  /* ev.path 新建 */ break;
        case coro::fs::watch_event_type::removed:  /* ev.path 删除 */ break;
        case coro::fs::watch_event_type::modified: /* ev.path 内容变化 */ break;
        case coro::fs::watch_event_type::renamed:
            // ev.old_path → ev.path
            break;
        case coro::fs::watch_event_type::overflow:   // 内核缓冲溢出, 可能丢事件
            break;
        }
    }
}
```

注意: 「修改」的粒度由内核决定 (编辑器保存常触发多条 created/modified),
需要去抖请在应用层收集同路径事件 (如 100ms 窗口)。Windows 用
`ReadDirectoryChangesW` 原生递归；Linux 用 inotify 的 wd 映射跟踪整棵目录树。

### 10.5 子进程 — coro::process

对标 `asyncio.subprocess`:

```cpp
coro::Task<> demo() {
    // 便捷: 跑到退出并收集 stdout
    auto [code, out] = co_await coro::process::run_capture({"cmd", "/c", "echo hi"});

    // 完整接口: stdio 管道可选捕获
    coro::process::options opt;
    opt.capture_stdin = true;     // 需要向子进程写数据时
    opt.capture_stdout = true;
    coro::process::Process p = co_await coro::process::spawn(
        {"cmd", "/c", "findstr x"}, opt);
    if (!p.valid()) { /* 启动失败: io::last_error() */ }

    co_await p.stdin_pipe()->write("xxx
", 4);
    p.stdin_pipe()->close();                  // EOF → 子进程读到输入结束

    char buf[256];
    int n = co_await p.stdout_pipe()->read(buf, sizeof(buf)); // 读子进程输出

    int exit_code = co_await p.wait();        // 挂起等退出 (多协程可同时 wait)
    p.terminate();                            // SIGTERM / TerminateProcess
}
```

生命周期约定: `Process` 析构**不杀进程也不等待** —— 需要明确 `wait()`
或 `terminate()`。退出通知经跨线程 Promise 路由 (等待者所在的 loop 被精确
唤醒)。Linux 实现用专用收割线程 waitpid, 不触碰全局信号掩码。

---

## 十一、调度辅助

```cpp
coro::call_soon([] { /* 下一轮执行 */ });
coro::call_later(500ms, [] { /* 500ms 后 */ });
coro::call_later(1s, []() -> coro::Task<> { co_await do_something(); });  // 支持协程回调
coro::call_at(deadline, callback);   // 指定时间点
```

---

## 十二、生命周期规则与经典坑

| 规则 | 说明 |
|---|---|
| **Task 所有权** | 协程完成前销毁 Task 会安全终止/放弃该任务；若要继续运行，必须持有 Task 或 `detach()` |
| **spawn 返回值要保存** | 丢弃返回值会终止任务，不会形成后台任务 |
| **fire-and-forget** | `start()` 后 `detach()`，协程帧自持有运行到完成（见网络示例）；异常会打印警告。需后续 `cancel()` 则自己持有 Task（成员/`shared_ptr`），不要 detach |
| **单线程模型** | 不要在多线程同时 resume 同一协程 |
| **嵌套 run() 不支持** | 运行中再次 `run()` 直接返回 |
| **lambda 协程** | 支持；捕获属于闭包，闭包必须活到任务结束；逃逸任务优先用按值参数 |

其余经典坑：

1. **悬垂外部对象**：协程参数/捕获引用的对象先析构，恢复后访问会产生 UB
2. **final_suspend 忘挂起**：`suspend_never` 结束后帧自动销毁，再碰句柄即 UB
3. **重复 destroy**：拷贝被禁止就是为了防双重释放

---

## 十三、多线程并行（每线程一个事件循环）

对标 Python asyncio 的"一个线程一个 loop"模型——**多核并行的正确打开方式**：

```cpp
// 每个线程跑自己的事件循环, 各占一个核, 互不干扰
void worker(int id) {
    auto t = my_task(id);
    t.start();
    coro::EventLoop::get().run();   // 本线程的 loop (惰性创建)
}

int main() {
    std::thread t1(worker, 1);
    std::thread t2(worker, 2);
    // ... N 个线程 = N 核并行; 各自内部仍是单线程语义
}
```

| 要点 | 说明 |
|---|---|
| `EventLoop::get()` | 返回**当前线程**的 loop（不存在则惰性创建），不再是全局单例 |
| 内部原语路由 | sleep/sync/queue/gather/task_group 全部经当前线程 loop——同一线程内语义与之前完全一致 |
| 跨线程唤醒 | `Promise::set_value` 从任意线程调用时，自动路由到等待者所在的 loop（`owner_loop`） |
| 单线程语义不变 | 同一线程内仍是"协作式单线程"——共享状态无需加锁 |

> 与 Go 的区别：Go 是 work-stealing 多核调度（任务自动迁移），本库是"每线程一个 loop"（线程亲缘，对标 asyncio）。后者实现简单、行为可预测，配合 `to_thread` 可覆盖绝大多数并行场景。

### 13.1 Scheduler — 自动多核分发（像 goroutine 一样丢任务）

```cpp
coro::Scheduler sched;                              // 默认 = CPU 核数个 worker
for (auto& req : requests)
    sched.spawn_any([&req] { return handle(req); }); // 自动分发到最闲的 worker
sched.wait_all();                                   // 阻塞直到全部完成
```

要点：

- **工厂模式**：`spawn_any` 接受返回 `Task<T>` 的工厂——协程帧在 worker 线程创建/销毁（跨线程传递已创建的 Task 会破坏帧内存归属，Debug CRT 会断言）
- **协程亲和**：任务一旦绑定 worker 不迁移 → 每 worker 内仍是单线程语义（共享状态无需加锁）
- 与手动 loop-per-thread 完全兼容，可混合使用

## 十四、Python asyncio 映射速查

| Python asyncio | coro 本库 |
|---|---|
| `await asyncio.sleep(1)` | `co_await coro::sleep(1s)` |
| `await asyncio.sleep(0)` | `co_await coro::yield()` |
| `await coro()` | `co_await task()` |
| `asyncio.create_task(coro())` | `coro::spawn(task())` |
| `await asyncio.gather(a, b)` | `co_await coro::gather(a(), b())` |
| `asyncio.wait_for(coro, t)` | `co_await coro::wait_for(task, t)` |
| `f = asyncio.Future(); f.set_result(x)` | `coro::Promise<T> p; p.set_value(x)` |
| `asyncio.run(main())` | `coro::run(main_task())` |
| `task.cancel()` | `t.cancel()`（协程体内收到 `CancelledError`） |
| `asyncio.Lock/Semaphore/Event/Queue` | `coro::Lock/Semaphore/Event/Queue` |
| `open()+read()` / aiofiles | `co_await coro::fs::open(...)` + `co_await f.read_at(...)` |
| `pathlib.Path.read_bytes()`（异步场景） | `co_await coro::fs::read_all(path)` |
| `os.pipe()` | `auto [rd, wr] = coro::pipe::pair();` |
| `loop.add_signal_handler(SIGINT, cb)` | `coro::signal::handle(SIGINT, [] { return cb_task(); })` |
| `watchfiles.awatch(path)` | `co_await coro::fs::watch(path)` + `co_await w.next()` |
| `asyncio.create_subprocess_exec(...)` | `co_await coro::process::spawn({...}, opts)` |
| `proc.wait()` | `co_await p.wait()` |
| `proc.terminate()` | `p.terminate()` |

---

## 十五、已知限制

- **网络协议范围**：已有 TCP/UDP；尚无 Unix socket、TLS 和域名解析
- **文件 IO（Windows）**：`fs::open`/`stat` 是同步调用（元数据操作，微秒级）；
  读写/刷盘全异步。Linux 上 open 走 io_uring 全异步
- **`fs::watch`（Linux）**：递归模式依赖每个子目录的 inotify watch，受
  `/proc/sys/fs/inotify/max_user_watches` 限制；收到 `overflow` 后应全量扫描兜底
- **信号（Windows）**：无真信号，控制台事件桥接（SIGINT/SIGBREAK/SIGHUP 关窗/
  SIGTERM 注销）；SIGHUP 无法经 `raise()` 触发
- **子进程**：Windows 侧 stdio 用命名管道（匿名管道不支持 OVERLAPPED），
  通过 `PROC_THREAD_ATTRIBUTE_HANDLE_LIST` 仅继承 stdin/stdout/stderr 白名单
- **`gather` 为编译期数量**：动态数量用 `gather_all` / `wait_tasks`（仅同类型）
- **`wait_any`**：支持两路和动态 N 路；需要完成任务集合分组时使用
  `wait_tasks(..., WaitMode::FirstCompleted)`
- **`gather_void` 传播第一个异常**（全部完成后，对标 `asyncio.gather` 语义）
- **TaskGroup 的 `spawn` 不返回句柄**：子任务效果通过参数/共享状态传递
- **`Condition` 需显式关联 `Lock`**：不自动创建锁（asyncio 默认自建）

## 附：示例与阅读顺序

| 示例 | 覆盖功能 |
|---|---|
| `examples/basic.cpp` | sleep + 串行 + spawn |
| `examples/gather.cpp` | gather 并发 + 异常 |
| `examples/future.cpp` | Promise/Future 桥接 |
| `examples/full_test.cpp` | 10 项功能综合测试 |
| `examples/echo_server.cpp` | IOCP/io_uring TCP echo |
| `examples/file_io.cpp` | coro::fs 定位读写 / 并发分块读 / fsync |
| `examples/dir_watch.cpp` | coro::fs::watch 目录事件 |
| `examples/process_demo.cpp` | coro::process 捕获输出 / 退出码 / stdin |

源码阅读顺序：`sleep.hpp` → `event_loop.hpp` → `task.hpp` → `sync.hpp` / `queue.hpp` → `future.hpp` → `gather.hpp` / `wait.hpp` → `net.hpp` → `io.hpp` → `fs.hpp` / `pipe.hpp` / `signal.hpp` / `fs_watch.hpp` / `process.hpp`。

## 术语速查

| 术语 | 一句话解释 |
|---|---|
| 协程函数 | 函数体含 `co_await`/`co_yield`/`co_return` 的函数 |
| 协程帧 | 堆上存状态的内存块（局部变量 + 挂起点 + promise） |
| `coroutine_handle` | 帧的遥控器：`resume`/`done`/`destroy` |
| `promise_type` | 帧内控制台（协议五件套） |
| awaitable | 实现 `await_ready`/`await_suspend`/`await_resume` 的类型 |
| `co_await` | 挂起当前协程，等 awaitable 就绪后恢复 |
| `co_yield` | 产出值并挂起（生成器） |
| 事件循环 | 就绪队列 + 定时器堆，统一调度挂起协程 |
