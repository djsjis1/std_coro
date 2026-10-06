# coro — C++20 协程事件循环框架

> 📚 **文档中心**: [docs/README.md](docs/README.md) — 全部文档的地图
>
> - 📖 完整指南: [docs/coro-guide.md](docs/coro-guide.md)（教程 + API 参考合并版）
> - 🎓 使用教程（10 讲）: [docs/tutorial/README.md](docs/tutorial/README.md)
> - 📘 API 参考手册: [docs/api-reference.md](docs/api-reference.md)
> - 🔬 架构与源码剖析: [docs/architecture.md](docs/architecture.md)
> - 🧠 C++20 协程课程（14 讲）: [docs/cpp20-coroutines-course/README.md](docs/cpp20-coroutines-course/README.md)
> - 🌐 Web 框架: [docs/web-framework.md](docs/web-framework.md) ·
>   ⚡ 性能: [docs/performance.md](docs/performance.md) ·
>   🛠 FAQ: [docs/faq.md](docs/faq.md)

TLS 收口代码与生命周期修复的验证步骤见 [维护者验证计划](docs/tls-validation-plan.md)。
Linux 开启 `CORO_ENABLE_TLS=ON` 后可单独链接 `coro::tls` 使用 TLS API；
开启 `CORO_BUILD_EXAMPLES` 可构建 [本地 TLS 示例](examples/tls_demo.cpp)。
TLS 实现编译为独立静态库，OpenSSL 头文件不进入公共头；Windows TLS 构建尚未支持。
本轮改动未运行构建或测试，待维护者验证。

类似 Python **asyncio** 的 C++20 协程库，核心部分 header-only；平台 IO 使用系统库，
Linux 的 io_uring 为可选依赖。核心能力：

- **asyncio 完整对标**:`sleep`/`spawn`/`gather`/`wait_for`/`wait_tasks`/`TaskGroup`(结构化并发)/`ExceptionGroup`/`Future`/`Lock`/`Semaphore`/`Event`/`Condition`/`Queue`(含 join)/`to_thread`
- **并发工具集**:`channel<T>`(Go 风格有界通道) / `Context`(结构化取消传播) / `select`(多路等待) / `Timer`(可取消可重置定时器) / `rate_limiter`(令牌桶限流) / `task_registry`(服务组件任务登记表) / `shutdown`(优雅关停)
- **多核并行**:每线程独立事件循环(对标 asyncio 的 loop-per-thread),跨线程唤醒自动路由
- **取消语义**:`cancel()` 注入 `CancelledError`,RAII 清理、循环终止、取消保护全部对标 Python
- **网络**:TCP/UDP(IOCP / io_uring) / DNS / Unix Domain Socket / TLS(OpenSSL 3.5 LTS)
- **HTTP**:HTTP 协议解析 / HTTP 客户端(http+https) / Web 服务框架
- **IO 扩展**:异步文件 IO / 管道 / 信号 / 目录监视 / 子进程(与网络同一条完成路径)
- **性能基线**：当前 Windows/IOCP Release 压测中，10 万协程
  `sleep(1ms)` 约 141ms、400 万次 `yield` 约 807ms、Scheduler 分发 40 万任务约
  198ms；这些是单机参考值，不是性能保证，详见 [性能指南](docs/performance.md)。

## 5 秒快速开始

```cpp
#include <coro/coro.hpp>
#include <iostream>

using namespace std::chrono_literals;

coro::Task<int> compute() {
    std::cout << "开始计算..." << std::endl;
    co_await coro::sleep(1s);            // ← 挂起 1 秒，不阻塞线程
    std::cout << "计算完成!" << std::endl;
    co_return 42;
}

coro::Task<> main_task() {
    int result = co_await compute();     // 串行等待
    std::cout << "结果: " << result << std::endl;
}

int main() {
    coro::run(main_task());              // 启动事件循环
}
```

编译运行后输出（等待 1 秒后打印完成）：

```
开始计算...
计算完成!
结果: 42
```

## 安装为 CMake 包

核心库提供 `coro::coro` 的安装导出，应用侧可直接使用：

```powershell
cmake --preset core               # 纯协程核心 (测试/示例/Web 默认关闭); 或 cmake -S . -B build
cmake --build build/core --config Release
cmake --install build/core --config Release --prefix <install-prefix>
```

```cmake
find_package(coro CONFIG REQUIRED)
target_link_libraries(my_app PRIVATE coro::coro)
```

安装与平台验证边界见 [质量基线与发布说明](docs/quality-status.md)。

需要控制依赖时，按组件选择目标；`coro::coro` 保留为兼容聚合入口：

| 目标 | 用途与依赖 |
|---|---|
| `coro::core` | Task、调度、同步与定时器，标准线程设施 |
| `coro::io` | 原生网络与文件 I/O，依赖 core，不链接 OpenSSL |
| `coro::tls` | TLS 会话，依赖 io 与私有 OpenSSL 实现；只在 TLS 开启时提供 |
| `coro::http_protocol` | HTTP 解析和报文构建，只依赖 llhttp；开启 `CORO_ENABLE_HTTP` 或 Web 后提供 |
| `coro::tcp_udp` | TCP/UDP 服务封装，依赖 io |
| `coro::router` | 独立路由算法，无协程或网络依赖 |

例如 `find_package(coro CONFIG REQUIRED COMPONENTS io)` 后链接 `coro::io`。
只使用 TLS 的应用链接 `coro::tls`，不需要手动配置 OpenSSL 头路径。

只需要 HTTP 协议时可设置 `CORO_ENABLE_HTTP=ON`、`CORO_ENABLE_NATIVE_IO=OFF`，
安装后使用 `find_package(coro CONFIG REQUIRED COMPONENTS http_protocol)` 并链接
`coro::http_protocol`。公共头为 `<http_parse.h>`、`<http_protocol.h>`；该组件不依赖
协程调度器、socket、TLS 或 Web 服务。`thirdparty/http` 也保留独立构建与
`find_package(http CONFIG REQUIRED)` / `http::http` 用法。

## 头文件与模块边界

`coro.hpp` **只聚合核心层**（Task/EventLoop/gather/Future/sync/Queue/wait/TaskGroup/to_thread/Scheduler 等，零平台依赖），**不含** IO 层与并发扩展——按需单独 include：

| 需要什么 | include |
|---|---|
| TCP / UDP socket | `<coro/net.hpp>` |
| 异步文件 | `<coro/fs.hpp>` |
| 管道 / fd 轮询 | `<coro/pipe.hpp>` |
| Unix domain socket | `<coro/unix.hpp>` |
| DNS / 子进程 / 信号 / 目录监视 | `<coro/dns.hpp>` / `<coro/process.hpp>` / `<coro/signal.hpp>` / `<coro/fs_watch.hpp>` |
| 流式读写适配（read_line/write_all…） | `<coro/stream.hpp>` |
| channel / select / Timer / Context(取消) / rate_limiter / Pool | `<coro/channel.hpp>` / `<coro/select.hpp>` / `<coro/timer.hpp>` / `<coro/context.hpp>` / `<coro/rate_limit.hpp>` / `<coro/pool.hpp>` |
| 高级服务框架 | `<tcp_udp/tcp_server.hpp>`、`<tcp_udp/udp_server.hpp>` |
| TLS 会话 | `<coro/tls.hpp>`（链接 `coro::tls`） |

能力宏（由 CMake 目标定义注入，header-only 消费者无需关心）：

| 宏 | 含义 | 缺失时的行为 |
|---|---|---|
| `CORO_HAS_URING` | Linux 上启用 io_uring 事件源 | 事件源回退 condition_variable，IO awaiter 返回 `-ENOTSUP` |
| `CORO_HAS_UNIX` | 由 `CORO_HAS_URING` 派生 | unix.hpp 编译为空 |
| `CORO_HAS_CONCURRENCY_EXT` | 并发扩展层随构建参与编译 | 头文件仍可 include（构建级门控，非 API 门控） |

## Python asyncio → C++20 映射速查

| Python | coro (本库) |
|---|---|
| `await asyncio.sleep(1)` | `co_await coro::sleep(1s)` |
| `await asyncio.sleep(0)` | `co_await coro::yield()` |
| `await coro()` | `co_await task()` |
| `asyncio.create_task(coro())` | `coro::spawn(task())` |
| `await asyncio.gather(a,b,c)` | `co_await coro::gather(a(),b(),c())` |
| `asyncio.wait(tasks, return_when=...)` | `co_await coro::wait_tasks(tasks, coro::WaitMode::...)` |
| `asyncio.wait_for(coro, t)` | `co_await coro::wait_for(task, t)` |
| `asyncio.TaskGroup` (3.11+) | `coro::TaskGroup` + `co_await group.wait()` |
| `ExceptionGroup` (3.11+) | `coro::ExceptionGroup` |
| `asyncio.to_thread(fn)` | `co_await coro::to_thread(fn)` |
| `f = asyncio.Future()` | `coro::Promise<T> p; auto f = p.get_future();` |
| `f.set_result(x)` | `p.set_value(x);` |
| `asyncio.Lock / Semaphore / Event / Condition / Queue` | `coro::Lock / Semaphore / Event / Condition / Queue` |
| `asyncio.Channel` (third-party) | `coro::channel<T>` (有界通道) |
| `asyncio.with contextlib.cancel_scope()` | `coro::Context` / `CancellationSource` |
| `async with lock:` | `auto g = co_await lock.guard();` |
| `q.task_done() / q.join()` | `q.task_done() / co_await q.join()` |
| `asyncio.run(main())` | `coro::run(main_task())` |
| `task.cancel()` | `t.cancel()`（协程体内收到 `CancelledError`） |
| aiofiles `read()`/`write()` | `co_await coro::fs::read_all/write_all(...)`、`File::read_at/write_at` |
| `os.pipe()` | `coro::pipe::pair()` |
| `loop.add_signal_handler(SIGINT, cb)` | `coro::signal::handle(SIGINT, ...)`（RAII 注册对象） |
| `watchfiles.awatch(dir)` | `co_await coro::fs::watch(dir)` + `co_await w.next()` |
| `asyncio.create_subprocess_exec` | `co_await coro::process::spawn({...}, opts)` |
| 每线程一个 loop | `EventLoop::get().run()`（每线程独立 loop,多核并行） |

### 并发组合器选择

| 需求 | 首选 API | 关键语义 |
|---|---|---|
| 编译期固定、可混合返回类型的任务 | `gather(a(), b(), ...)` | 全部完成；返回按参数顺序组成的 tuple；首个异常在全部结束后抛出 |
| 运行期动态、同一返回类型的一组任务 | `wait_tasks(std::move(tasks), WaitMode::...)` | `FirstCompleted` / `FirstException` / `AllCompleted` 三种策略 |
| 失败时自动取消同组任务的结构化并发 | `TaskGroup` + `co_await group.wait()` | 失败聚合为 `ExceptionGroup`，其余任务被取消 |
| 多个 `channel` 收发分支竞速 | `select(...)` | 仅 channel 专用；落选分支不消费数据 |

`wait_any`、`gather_all`、`gather_void` 是兼容入口，已标记为 deprecated；分别迁移到
`wait_tasks(..., FirstCompleted)`、`wait_tasks(..., AllCompleted)`、`gather(...)`（忽略 tuple）。

---

## 功能详解

### 1. `sleep` — 协程睡眠（不阻塞线程）

**有！** `coro::sleep()` 和 Python 的 `asyncio.sleep()` 行为完全一致：

- 挂起**当前协程**指定时间
- **不阻塞**操作系统线程（事件循环可以去执行其他就绪协程）
- 精度取决于操作系统调度（通常 ~1-15ms）

```cpp
coro::Task<> demo_sleep() {
    std::cout << "A" << std::endl;
    co_await coro::sleep(500ms);        // 挂起 500 毫秒
    std::cout << "B" << std::endl;
    co_await coro::sleep(2s);           // 再挂起 2 秒
    std::cout << "C" << std::endl;
}
```

还有一个 `yield()` 原语，类似 `asyncio.sleep(0)`：

```cpp
co_await coro::yield();  // 主动让出 CPU，让其他协程执行
```

### 2. `spawn` — 后台启动协程（类似 `create_task`）

```cpp
coro::Task<> demo_spawn() {
    // 立即启动两个后台协程，不等待它们
    auto t1 = coro::spawn(work1());
    auto t2 = coro::spawn(work2());

    // 做其他事情...
    co_await coro::sleep(100ms);

    // 现在等待结果
    int r1 = co_await std::move(t1);
    int r2 = co_await std::move(t2);
}
```

### 3. `gather` — 并发等待多个任务

```cpp
coro::Task<> demo_gather() {
    // 三个 API 调用并发执行，总耗时 ≈ 最慢的那个
    auto [users, posts, photos] = co_await coro::gather(
        fetch("/api/users",  300ms),
        fetch("/api/posts",  200ms),
        fetch("/api/photos", 100ms)
    );
    // users, posts, photos 都是 std::string
    // 总耗时约 300ms，而非串行的 600ms
}
```

支持混合类型：

```cpp
auto [name, count] = co_await coro::gather(
    fetch_name(),     // → Task<string>
    fetch_count()     // → Task<int>
);
```

异常处理：任一任务失败 → 等待所有完成 → 重新抛出第一个异常。

### 4. `Future/Promise` — 桥接回调到协程

```cpp
// 场景：包装一个老式的回调 API
coro::Task<std::string> download_async(const std::string& url) {
    // 必须用 shared_ptr: 回调可能在协程帧销毁后才触发,
    // 捕获局部引用 (&promise) 会导致 use-after-free。
    auto promise = std::make_shared<coro::Promise<std::string>>();
    auto future = promise->get_future();

    // 回调按值捕获 shared_ptr, 保证 Promise 活到回调执行
    legacy_api.download(url, [promise](const std::string& data) {
        promise->set_value(data);
    });

    co_return co_await future;  // 挂起直到回调触发
}
```

如果值已经提前设置好，`await_ready()` 返回 `true`，不会挂起：

```cpp
promise->set_value("立即就绪!");
auto result = co_await future;  // 不会挂起，立即返回
```

### 5. `wait_for` / `wait_tasks` — 超时与动态任务集

```cpp
// 超时: 超时后任务被自动取消, 抛 TimeoutError
try {
    auto r = co_await coro::wait_for(slow_task(), 500ms);
} catch (const coro::TimeoutError&) { /* 超时 */ }

// N 路竞速 (对标 asyncio.wait 的 return_when):
auto first = co_await coro::wait_tasks(std::move(tasks),
                                       coro::WaitMode::FirstCompleted);
// WaitMode::FirstException — 任一失败立即抛
// WaitMode::AllCompleted   — 全部完成返回全部结果
```

### 6. `cancel()` — 取消(注入 CancelledError)

```cpp
auto t = coro::spawn(slow_task());
t.cancel();                      // 协程在下一个 await 点收到 CancelledError
try {
    co_await std::move(t);
} catch (const coro::CancelledError&) {
    // 被取消: 协程体内栈对象已全部析构 (清理逻辑已执行)
}
```

循环任务也能终止;协程体 `catch (CancelledError)` 可实现"取消保护"(对标 shield)。

### 7. `TaskGroup` — 结构化并发(对标 Python 3.11)

```cpp
coro::TaskGroup group;
group.spawn(fetch_a());          // 任意返回类型的子任务
group.spawn(fetch_b());
try {
    co_await group.wait();       // 等待全部完成
} catch (const coro::ExceptionGroup& eg) {
    // 任一失败 → 其余被自动取消; eg.exceptions() 逐个检查
}
```

### 8. 同步原语 — Lock / Semaphore / Event / Condition

```cpp
coro::Lock lock;                 // 互斥锁
auto g = co_await lock.guard();  // RAII: 离开作用域自动释放 (异常路径也安全)

coro::Semaphore sem(3);          // 信号量: 限制并发数

coro::Event ev;                  // 一次性事件通知
co_await ev.wait();              // 等待
ev.set();                        // 触发 (唤醒全部)

coro::Condition cond(&lock);     // 条件变量
{
    auto g = co_await lock.guard();
    while (!ready) co_await cond.wait();  // 释放锁挂起; 唤醒后重新持锁
}
```

### 9. `Queue` — 生产者-消费者(含收尾协议)

```cpp
coro::Queue<int> q(10);          // 有界队列 (默认无界)
co_await q.put(42);              // 满则挂起生产者
int v = co_await q.get();        // 空则挂起消费者
auto nv = q.get_nowait();        // 非阻塞取
bool ok = q.put_nowait(42);      // 非阻塞放

q.task_done();                   // 处理完一个任务
co_await q.join();               // 挂起直到所有任务处理完
```

### 10. `to_thread` — 阻塞代码丢线程池

```cpp
// 阻塞函数在工作线程执行, 事件循环线程不阻塞 (对标 asyncio.to_thread)
int v = co_await coro::to_thread([] { return blocking_read(); });
// 异常同样跨线程传播
```

### 11. TCP 网络(IOCP / io_uring)

```cpp
// 片段示例 (服务器 accept 循环); 完整可运行版见 examples/echo_server.cpp
#include <coro/net.hpp>

coro::net::TcpListener listener;
listener.bind_listen("127.0.0.1", 8888);
while (true) {
    auto conn = co_await listener.accept();
    char buf[1024];
    int n = co_await conn.read(buf, sizeof(buf));   // 异步读
    co_await conn.write(buf, n);                    // 异步写
}
```

### 11b. IO 扩展 — 文件 / 管道 / 信号 / 目录监视 / 子进程

```cpp
// 异步文件 (IOCP/io_uring 完成包驱动, 定位读写可多协程并发分块)
std::string s = co_await coro::fs::read_all("data.bin");
auto f = co_await coro::fs::open("data.bin", coro::fs::mode::read);
int n = co_await f.read_at(buf, sizeof(buf), /*offset=*/0);  // 0=EOF

// 管道 (满写/空读自动挂起 = 天然背压)
auto [rd, wr] = coro::pipe::pair();
co_await wr.write("x", 1);
int n2 = co_await rd.read(buf, sizeof(buf));  // 0 = 写端关闭

// 信号 (Ctrl+C 优雅关停)
coro::signal::handler h = coro::signal::handle(SIGINT, [] { return shutdown_task(); });
co_await coro::signal::wait(SIGTERM);         // 单次等待

// 目录监视
// Windows 和 Linux 都支持递归子目录
auto w = co_await coro::fs::watch("src", /*recursive=*/true);
coro::fs::watch_event ev = co_await w.next(); // created/removed/modified/renamed

// 子进程
auto [code, out] = co_await coro::process::run_capture({"cmd", "/c", "echo hi"});
```

统一错误约定: 失败返回 -1, `errno` = 标准 errno 值, 平台原生码在
`coro::io::last_error()`。详见 `docs/coro-guide.md` 第十节。

### 12. 多线程并行(每线程一个事件循环)

```cpp
void worker() {
    auto t = my_task();
    t.start();
    coro::EventLoop::get().run();   // 本线程的 loop
}
std::thread t1(worker), t2(worker); // N 线程 = N 核并行
// 同线程内仍是单线程语义; Promise 跨线程 set_value 自动路由到正确 loop
```

---

## 架构概述

```
┌──────────────────────────────────────────┐
│        EventLoop (每线程一个, 多核并行)    │
│  ┌──────────────┐  ┌──────────────────┐  │
│  │  就绪队列     │  │  定时器堆         │  │
│  │  (FIFO+去重) │  │  (min-heap+惰性清理)│  │
│  │  立即恢复     │  │  按 deadline 排序  │  │
│  └──────────────┘  └──────────────────┘  │
│         ↑                   ↑             │
│    schedule()        schedule_timer()     │
└──────────────────────────────────────────┘
         ↑                   ↑
    ┌────┴────┐        ┌────┴────┐
    │  yield  │        │  sleep  │
    └─────────┘        └─────────┘
```

**事件循环主循环** (`EventLoop::run()`)：

```
while (有就绪协程 或 定时器 或 活跃协程 或 挂起 I/O) {
    1. 惰性清理僵尸定时器 + 到期定时器 → 就绪队列
    2. 就绪队列为空 → EventSource 等待 (定时器/IOCP/跨线程唤醒)
    3. 批量取出就绪队列, 逐个 resume
}
```

**协程生命周期** (`Task<T>`)：

```
创建 Task → [惰性挂起] → start()/co_await → 执行协程体
    → co_await 其他对象 → 挂起 → 被恢复 → 继续执行
    → co_return → 存结果 → 调度 continuation → 销毁帧
```

## 项目结构

```
coro/
├── include/coro/          # 核心、I/O 与可选组件公共头
├── src/tls.cpp           # TLS 实现，隔离 OpenSSL 头文件
├── CMakeLists.txt         # 库目标 coro::coro + 安装导出
├── examples/              # 示例
├── tests/                 # 单元测试（googletest，当前 24 个测试源）+ 压测
├── Web/                   # HTTP 服务器框架 (见 docs/web-framework.md)
├── router/                # 独立泛型基数树路由 radix_router<T>
├── docs/                  # 文档中心 (docs/README.md 为地图)
│   ├── tutorial/          #   使用教程 (10 讲)
│   └── cpp20-coroutines-course/  #   C++20 协程语言课程 (14 讲)
├── thirdparty/            # googletest / llhttp 等仓库内源码集成 (库本身零依赖; 仅启用对应模块时构建)
└── main.cpp               # 协程练习场（改完直接编译运行）
```

## 作为依赖接入

推荐使用上面的 CMake 安装包方式；开发阶段也可以把本仓库根目录作为子目录引入：

```bash
# 复制仓库（或用 git submodule / FetchContent 获取）
cp -r path/to/coro your_project/thirdparty/coro
```

```cmake
# 你的 CMakeLists.txt
add_subdirectory(thirdparty/coro)
target_link_libraries(your_app PRIVATE coro::coro)
```

`coro::coro` 目标自动携带 include 路径、C++20 标准与平台链接库
（Windows: `ws2_32`；Linux: `pthread`，以及启用 io_uring 时的 `liburing`）。

## 构建

```bash
# 需要: CMake 3.20+, 支持 C++20 的编译器 (MSVC 2022 / GCC 11+ / Clang 14+)
cmake -B build
cmake --build build

# 运行示例
./build/Debug/example_basic.exe
./build/Debug/example_gather.exe
./build/Debug/example_future.exe
./build/Debug/example_file_io.exe    # 异步文件 IO
./build/Debug/example_dir_watch.exe  # 目录监视
./build/Debug/example_process.exe    # 子进程

# 单元测试
ctest --test-dir build -C Debug --output-on-failure

# 高并发压测 (10 万协程 / 400 万 yield / 100 万队列 / 多线程)
cmake --build build --config Release --target coro_stress
./build/Release/coro_stress.exe
```

## 限制 & 注意事项

| 项目 | 说明 |
|---|---|
| **线程模型** | 每线程一个事件循环 (对标 asyncio loop-per-thread): 同线程内单线程语义, 多线程各跑各的 loop 即多核并行; 协程不能跨线程迁移 |
| **跨线程 set_value** | `Promise::set_value()` 可从任意线程调用 (自动路由唤醒到等待者所在的 loop) |
| **Task 生命周期** | 析构未完成的 `Task` 会安全放弃/销毁该任务；若希望任务继续运行，必须保存 `spawn` 返回值，或明确调用 `detach()` |
| **取消语义** | `Task::cancel()` 对标 Python：协程在下一个 await 点收到 `CancelledError`（可 catch 做清理），循环任务也能终止。请在事件循环线程调用；挂起在 I/O 上的任务会先取消底层 I/O（CancelIoEx / ASYNC_CANCEL），由完成包唤醒 |
| **嵌套 run()** | 不支持（检测到嵌套调用会抛出 `std::runtime_error`，错误信息说明原因；异常后事件循环状态复位，可再次运行） |
| **协作式调度** | 无栈协作式: 协程体内不要写不含 co_await 的死循环, 否则阻塞整个事件循环 (与 Python asyncio 相同, Go 无此限制) |
| **lambda 协程** | 支持；捕获存放在闭包对象中，闭包必须活到协程结束，或改用无捕获 lambda + 参数 |

### lambda 协程：支持，但要管理闭包生命周期

lambda 当然可以作为协程。容易踩坑的是**捕获型协程 lambda 的临时闭包**：
捕获属于 lambda 闭包对象，不会自动复制进协程帧；协程挂起后若闭包已经析构，
再次访问捕获就是 use-after-free。这个规则与 Debug/Release、MSVC/GCC/Clang 无关。

```cpp
// ❌ 临时捕获闭包在本语句结束时析构，Task 之后恢复会访问悬空的闭包
for (int i = 0; i < 3; ++i)
    tasks.push_back([i]() -> coro::Task<int> {
        co_await coro::yield();
        co_return i * 10;
    }());

// ✅ 无捕获 lambda；i 是按值参数，复制进协程帧
auto worker = [](int i) -> coro::Task<int> {
    co_await coro::yield();
    co_return i * 10;
};
for (int i = 0; i < 3; ++i)
    tasks.push_back(worker(i));

// ✅ 捕获也可以，只要 worker 闭包确定活到 task 完成
int result = 0;
auto captured_worker = [&result]() -> coro::Task<> {
    co_await coro::yield();
    result = 42;
};
auto task = captured_worker();
co_await task; // worker 此时仍然存活

// ✅ 复杂/逃逸任务优先用命名函数，生命周期最直观
coro::Task<int> make_task(int i) { co_return i * 10; }
for (int i = 0; i < 3; ++i)
    tasks.push_back(make_task(i));
```

需要 fire-and-forget 时优先使用命名函数或无捕获 lambda，把所有权对象作为
按值参数传入协程帧；不要指望捕获 `shared_ptr` 能延长临时闭包自身的生命周期。

## 许可证

本项目基于 [MIT License](LICENSE) 开源。
