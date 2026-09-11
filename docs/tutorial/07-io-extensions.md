# 第 7 讲 — IO 扩展：文件、管道、信号、目录监视、子进程

> 本讲目标：掌握 TCP 之外的五个 IO 模块。它们与网络共用同一条
> 完成路径（IOCP / io_uring），因此**API 风格、错误约定、取消语义
> 完全一致**——学会了第 6 讲，这一讲几乎是零成本。

五个模块各自单独 include（与 net.hpp 一样不在 coro.hpp 里）：

```cpp
#include <coro/fs.hpp>       // 异步文件 IO
#include <coro/pipe.hpp>     // 异步管道 (+ Linux fd 轮询)
#include <coro/signal.hpp>   // 信号事件 (Ctrl+C 优雅停机)
#include <coro/fs_watch.hpp> // 目录监视
#include <coro/process.hpp>  // 子进程
```

统一错误约定（第 6 讲已讲，再强调一次）：返回 `int` 时
`>=0` 为字节数、`-1` 为失败（此时 `errno` = 标准 errno 值，
原生码在 `coro::io::last_error()`）；`read` 类返回 `0` = EOF/对端关闭。

---

## 7.1 异步文件 IO — coro::fs

### 便捷接口：整文件读写

```cpp
std::string s = co_await coro::fs::read_all("config.json");   // 失败 → 空串
bool ok = co_await coro::fs::write_all("out.txt", "hello");   // "w" 语义: 创建/截断
```

### 元信息（同步函数，微秒级）

```cpp
auto st = coro::fs::stat("config.json");
if (st.exists) std::cout << st.size << " 字节, 目录? " << st.is_dir << std::endl;
```

### 句柄式：定位读写（核心设计）

```cpp
coro::fs::File f = co_await coro::fs::open(
    "data.bin", coro::fs::mode::write | coro::fs::mode::create);
if (!f.valid()) { /* 失败: errno / io::last_error() */ }

int n = co_await f.write_at("XYZ", 3, /*offset=*/100);   // 写到字节 100~102
char buf[16];
int m = co_await f.read_at(buf, sizeof(buf), 100);       // 从字节 100 读回
co_await f.fsync();                                      // 刷盘
f.close();
```

**为什么是 `read_at`/`write_at`（显式 offset）而不是"当前游标"？**
因为定位读写**不共享文件游标**，同一个文件可以被多个协程并发
分块读写而互不干扰——这是异步文件 IO 最自然的形态：

```cpp
// 四个协程并发读一个大文件的四个分块, gather 汇合
auto st = coro::fs::stat("big.bin");
uint64_t quarter = st.size / 4;

auto read_slice = [&](uint64_t off, uint64_t len) -> coro::Task<std::string> {
    std::string out(len, '\0');
    int n = co_await co_await coro::fs::open("big.bin", coro::fs::mode::read)
              .then_read(out.data(), len, off);          // 伪码, 见下方真实写法
    co_return out;
};
```

真实写法（打开句柄 + 并发分块读）：

```cpp
coro::Task<std::string> read_slice(const coro::fs::File& f, uint64_t off, uint64_t len)
{
    std::string out(len, '\0');
    int n = co_await f.read_at(out.data(), len, off);    // 挂起, 定位读
    if (n < 0) throw std::runtime_error("读失败");
    out.resize(n);
    co_return out;
}

coro::Task<> main_task()
{
    auto f = co_await coro::fs::open("big.bin", coro::fs::mode::read);
    auto st = coro::fs::stat("big.bin");
    uint64_t q = st.size / 4;

    auto&& [a, b, c, d] = co_await coro::gather(          // 四路并发
        read_slice(f, 0, q), read_slice(f, q, q),
        read_slice(f, 2 * q, q), read_slice(f, 3 * q, st.size - 3 * q));
    // 拼接 a+b+c+d 即完整内容; 总耗时 ≈ 最慢一块
}
```

### 打开模式（对标 fopen）

| mode | 含义 |
|---|---|
| `read` | "r"：只读，须存在 |
| `write` | "w"：隐含 create + truncate |
| `create` | 配合 write；配 `exclusive` 则已存在时报错（原子创建锁） |
| `truncate` | 打开即清空 |
| `append` | "a"：`write_at` 的 offset 被忽略，OS 原子追加 |
| `exclusive` | 与 create 组合：`O_EXCL` / `CREATE_NEW` |

### 平台差异（要知道，但通常不用管）

- Windows：`open`/`stat` 是同步调用（元数据操作，OS 有缓存）；
  读写/刷盘全异步（IOCP）。
- Linux：open/read/write/fsync 全部走 io_uring 全异步。
- `fsync`：Windows 上经 `to_thread` 调 `FlushFileBuffers`（不阻塞循环）；
  Linux 用 `IORING_OP_FSYNC`。

---

## 7.2 异步管道 — coro::pipe

一条单向字节通道，**满写/空读自动挂起 = 天然背压**，
常用于"生产者喂字节、消费者排空"的解耦：

```cpp
auto [rd, wr] = coro::pipe::pair();        // {读端, 写端}, 默认 64KB 缓冲

// 写端在后台协程
auto producer = coro::spawn(write_side(wr));

// 读端在当前协程
char buf[4096];
while (true) {
    int n = co_await rd.read(buf, sizeof(buf));
    if (n == 0) break;                     // 写端已全部关闭 → EOF
    if (n < 0)  break;                     // 错误
    process(buf, n);
}
co_await std::move(producer);
```

```cpp
coro::Task<> write_side(coro::pipe::PipeEnd& wr)
{
    for (int i = 0; i < 100; ++i) {
        int w = co_await wr.write("chunk;", 6);
        if (w < 0) break;
    }
    wr.close();                            // 关写端 → 读者收到 0
}
```

- 背压实测：小缓冲管道里写大量数据，写满即挂起，
  读者排空后写者自动续写——**不需要任何手动流控**。
- 对端关闭的表现：读端 `read` 得 0；写端 `write` 得 -1（EPIPE 语义）。
- Windows 用命名管道对实现（匿名管道不支持 OVERLAPPED）；
  Linux 是 `pipe2(O_NONBLOCK)` + io_uring。API 两平台一致。

### Linux 专属：fd 就绪轮询

```cpp
uint32_t revents = co_await coro::io::poll(fd, POLLIN);   // 挂起到 fd 可读
```

（Windows 的 socket 是完成制，无"就绪轮询"概念，故此 API 仅 Linux 提供。）

---

## 7.3 信号 — coro::signal（优雅停机的正解）

支持 SIGINT（Ctrl+C）、SIGTERM、SIGBREAK（Ctrl+Break）、SIGHUP（关窗）：

```cpp
#include <coro/signal.hpp>

// 用法一: 单次等待 (多个协程可同时等同一信号)
int sig = co_await coro::signal::wait(SIGINT);
std::cout << "收到信号 " << sig << std::endl;

// 用法二: 持续处理 — 每次信号到达执行 factory() 返回的协程
// 返回 RAII 注册对象, 析构/stop() 时自动注销
coro::signal::handler h = coro::signal::handle(
    SIGINT, [] { return shutdown_task(); });
```

**服务器优雅停机的完整骨架**（第 9 讲会实战）：

```cpp
coro::Task<> shutdown_task(web::web_server& s)
{
    s.stop();                    // 关监听 socket → accept 挂起点被唤醒
}

coro::Task<> main_flow(web::web_server& s)
{
    auto srv = coro::spawn(s.serve());                       // accept 循环
    auto h1  = coro::signal::handle(SIGINT,  [&s] { return shutdown_task(s); });
    auto h2  = coro::signal::handle(SIGBREAK, [&s] { return shutdown_task(s); });
    co_await std::move(srv);                                 // serve 正常退出
    s.wait_all();                                            // 等所有连接协程收尾
}
```

平台说明：Windows 没有真信号——库把控制台事件（Ctrl+C /
Ctrl+Break / 关窗）与 CRT `raise()` 双路桥接成上面的信号语义；
Linux 用 signalfd + io_uring。注意 Linux 下信号在首次
`wait`/`handle` 时于当前线程阻塞，之后创建的线程继承该掩码，
多线程程序应先注册信号再开线程。

---

## 7.4 目录监视 — coro::fs::watch

对标 watchfiles / inotify 工具，监控目录下文件的变化：

```cpp
auto w = co_await coro::fs::watch("src", /*recursive=*/true);
if (!w.valid()) { /* 打开失败 */ }

while (true) {
    coro::fs::watch_event ev = co_await w.next();   // 挂起到下一个事件
    switch (ev.type) {
    case coro::fs::watch_event_type::created:  std::cout << "新建 " << ev.path; break;
    case coro::fs::watch_event_type::removed:  std::cout << "删除 " << ev.path; break;
    case coro::fs::watch_event_type::modified: std::cout << "修改 " << ev.path; break;
    case coro::fs::watch_event_type::renamed:
        std::cout << "改名 " << ev.old_path << " → " << ev.path; break;
    case coro::fs::watch_event_type::overflow:
        std::cout << "内核缓冲溢出, 可能丢事件, 建议全量扫描"; break;
    }
    std::cout << std::endl;
}
```

注意事项：

- `ev.path` 是相对监视目录的 UTF-8 路径（`/` 分隔）；
- 内核事件粒度粗：编辑器保存一次常产生多条
  created/modified——应用层做**去抖**（例如同路径事件收进 100ms 窗口）；
- Linux 平台 v1 的递归参数只作用于初始已存在的子目录枚举限制
  （详见[已知限制](../coro-guide.md#十五已知限制)），Windows 原生递归完整；
- 配合 `wait_for(w.next(), 100ms)` 可实现"轮询 + 事件"混合模式。

---

## 7.5 子进程 — coro::process

对标 `asyncio.subprocess`：

### 便捷版：跑完收 stdout

```cpp
auto [code, out] = co_await coro::process::run_capture(
    {"cmd", "/c", "echo hello"});        // Linux: {"bash", "-c", "echo hello"}
std::cout << "退出码 " << code << ", 输出: " << out;
```

### 完整版：交互式 stdio

```cpp
coro::process::options opt;
opt.capture_stdout = true;               // 要读子进程 stdout
opt.capture_stdin  = true;               // 要写子进程 stdin

coro::process::Process p = co_await coro::process::spawn(
    {"cmd", "/c", "findstr x"}, opt);
if (!p.valid()) { /* 启动失败: io::last_error() */ }

co_await p.stdin_pipe()->write("xbox\n", 5);
p.stdin_pipe()->close();                 // EOF → 子进程输入结束

char buf[256];
int n = co_await p.stdout_pipe()->read(buf, sizeof(buf));   // 读输出

int exit_code = co_await p.wait();       // 挂起等退出; 多协程可同时 wait
p.terminate();                           // 需要强杀时
```

### 生命周期约定（重要）

- `Process` **析构不杀进程也不等待**——想要哪种结局就显式调
  `wait()` 或 `terminate()`，防止误杀；
- 退出通知走跨线程 Promise（自动路由到等待者的 loop），
  所以 `wait()` 可以在任何线程的协程里等；
- 并发多进程就一个 `gather`：

  ```cpp
  auto [r1, r2] = co_await coro::gather(
      coro::process::run_capture({"cmd", "/c", "task1"}),
      coro::process::run_capture({"cmd", "/c", "task2"}));
  ```

---

## 7.6 综合示例：热重载式配置监视

把本讲三个模块串起来：目录监视 + 异步读文件 + 子进程：

```cpp
#include <coro/coro.hpp>
#include <coro/fs.hpp>
#include <coro/fs_watch.hpp>
#include <coro/process.hpp>
#include <iostream>

using namespace std::chrono_literals;

coro::Task<> main_task()
{
    co_await coro::fs::write_all("app.conf", "mode=debug");

    auto w = co_await coro::fs::watch(".", /*recursive=*/false);

    while (true) {
        coro::fs::watch_event ev = co_await w.next();
        if (ev.path != "app.conf") continue;             // 只关心配置文件

        // 去抖: 配置保存常触发多条事件, 等 50ms 后一次性读
        co_await coro::sleep(50ms);

        std::string conf = co_await coro::fs::read_all("app.conf");
        std::cout << "[配置变更] " << conf << std::endl;

        // 配置变了 → 重启一个子进程加载
        auto [code, out] = co_await coro::process::run_capture(
            {"cmd", "/c", "echo reload with " + conf});
        std::cout << "[重载结果] code=" << code << std::endl;
    }
}

int main() { coro::run(main_task()); }
```

---

## 7.7 练习

1. 用 `fs` 写一个协程版 `tail -f`：`watch` 监视日志文件 +
   循环 `read_at` 增量读取并打印（记住上次读到的 offset）。
2. 用管道实现"压缩流水线"：生产协程生成 1MB 数据写入管道，
   消费协程读出后 `to_thread` 做假压缩（sleep 模拟），观察背压
   ——把消费侧加慢，看生产侧是否自动变慢。
3. 给第 6 讲的 echo 服务器接上 SIGINT 优雅停机：
   收到信号后关闭监听、取消所有连接协程、打印"bye"。
4. （探索）`examples/dir_watch.cpp` 和 `examples/process_demo.cpp`
   是官方示例，跑一遍并改一个行为。

---

**下一讲**：[多核并行](08-multicore.md) —— 单线程吃满一个核之后，
怎么用剩下的核。
