# 第 6 讲 — TCP 网络编程

> 本讲目标：用 `coro::net` 写出**零配置**的异步 TCP 服务器与客户端。
> Windows 走 IOCP、Linux 走 io_uring，代码完全相同。
> 本讲是第 7 讲（IO 扩展）和第 9 讲（HTTP 服务）的地基。

网络层不在 `coro.hpp` 里，单独 include：

```cpp
#include <coro/net.hpp>
```

---

## 6.1 世界观：完成制异步 IO（Proactor）

coro 的网络是**完成制**（completion-based）：你发起 `read`，
协程挂起；数据真正到达并拷进你的缓冲区之后，协程才被唤醒，
`co_await` 返回实际字节数。与 epoll 式"就绪制"（ready → 自己调 read）
不同，你的代码里**没有** EAGAIN、没有非阻塞标志、没有重试循环：

```cpp
int n = co_await conn.read(buf, sizeof(buf));   // 挂起; 返回时 buf 里已经有数据
co_await conn.write(buf, n);                     // 挂起; 返回时数据已交给内核
```

这三个返回值约定是全库 IO 的统一语言，**背下来**：

| 返回值 | 含义 |
|---|---|
| `>= 0` | 实际传输的字节数 |
| `0`（仅 read 类） | 对端正常关闭 / EOF |
| `-1` | 出错：`errno` 已转成标准 errno；平台原生码在 `coro::io::last_error()` |

---

## 6.2 Echo 服务器

```cpp
// echo_server.cpp
#include <coro/coro.hpp>
#include <coro/net.hpp>
#include <coro/fs.hpp>          // 若需日志落盘等
#include <iostream>
#include <memory>

using namespace std::chrono_literals;

// 每个连接一个协程: 读到什么回什么, 直到对端关闭
coro::Task<> handle_connection(coro::net::TcpStream conn, int conn_id)
{
    std::cout << "[" << conn_id << "] 客户端接入" << std::endl;
    char buf[4096];
    try {
        while (true) {
            int n = co_await conn.read(buf, sizeof(buf));
            if (n == 0) break;                       // 对端关闭
            if (n < 0)  { std::cout << "读错误" << std::endl; break; }
            int w = co_await conn.write(buf, n);     // 原样回写
            if (w < 0) break;
        }
    } catch (const coro::CancelledError&) {
        // 服务器停机时连接协程被取消: 走到这里做清理
    }
    conn.close();                                    // 主动关也行; 析构会自动关
    std::cout << "[" << conn_id << "] 客户端离开" << std::endl;
}

coro::Task<> serve()
{
    coro::net::TcpListener listener;
    if (!listener.bind_listen("127.0.0.1", 8888)) {  // 同步: 一次性动作
        std::cout << "监听失败" << std::endl;
        co_return;
    }
    std::cout << "echo 服务运行在 127.0.0.1:8888" << std::endl;

    int next_id = 0;
    while (true) {
        auto conn = co_await listener.accept();      // 挂起等新连接
        if (!conn.valid()) break;                    // 监听 socket 已关闭/出错
        // fire-and-forget 的正确姿势: start + detach (见下方说明)
        auto t = handle_connection(std::move(conn), next_id++);
        t.start();
        t.detach();
    }
}

coro::Task<> main_task()
{
    // 第 7 讲会讲 signal; 这里先靠 Ctrl+C 强杀或超时退出
    co_await serve();
}

int main() { coro::run(main_task()); }
```

### 重点 ①：fire-and-forget 的正确姿势

accept 循环不能 `co_await` 每个连接（那样就串行了），但连接协程又
必须有人管理生命周期。标准姿势是 **`start()` + `detach()`**：
协程帧自持有运行到完成，`final_suspend` 时自动销毁帧，
本协程（accept 循环）退出作用域也不会误删一个"已入队未执行"的帧：

```cpp
auto t = handle_connection(std::move(conn), id);   // 命名协程函数 (参数进帧)
t.start();
t.detach();   // 之后 t 成为空壳, 协程自行运行到结束
```

注意：detach 后就**无法再 `cancel()`** 这个连接了。若需要
"停机时统一取消所有连接"（第 7/9 讲的服务管理器写法），
请自己持有 Task（如放进容器的成员、或 `shared_ptr<Task<>>`），
不要 detach。

### 重点 ②：为什么 bind_listen 是同步的？

`bind_listen` 只做 socket/bind/listen 三个一次性系统调用（微秒级），
失败返回 false。真正异步的是 `accept` / `read` / `write` / `connect`。

---

## 6.3 客户端

```cpp
coro::Task<> client()
{
    // connect 是静态 awaiter: 返回 TcpStream, 失败时 valid() == false
    auto conn = co_await coro::net::TcpStream::connect("127.0.0.1", 8888);
    if (!conn.valid()) {
        std::cout << "连接失败: " << strerror(errno) << std::endl;
        co_return;
    }

    const char* msg = "hello coro";
    int w = co_await conn.write(msg, 10);
    if (w < 0) { /* 写错误 */ co_return; }

    char buf[256];
    int n = co_await conn.read(buf, sizeof(buf));    // 读回显
    std::cout << "收到: " << std::string(buf, n > 0 ? n : 0) << std::endl;

    conn.close();                                    // 主动关闭; 不调也行(析构关)
}
```

把服务器和客户端放进一个程序自测（项目自带的
`examples/echo_server.cpp` 就是这么做的）：先 spawn 服务器，再
spawn 客户端，客户端验证回显。

---

## 6.4 并发连接模型

上面的 accept 循环就是全部秘密：**一个 accept 协程 +
N 个连接协程**，每个连接独立推进，互不阻塞：

```
accept 循环 ──accept──> 连接 1 协程 (read/write/...)
        │
        ├──accept──> 连接 2 协程 (read/write/...)
        └──accept──> 连接 3 协程 ...
```

单线程事件循环轻松支撑上万空闲连接（每个连接协程只占一小块
堆内存，挂起时零 CPU）。1 万活跃连接的瓶颈通常在内核，
不在协程框架（见[性能指南](../performance.md)）。

**拆包（消息边界）**：TCP 是字节流，`read` 返回多少字节由内核定。
协议帧化是应用层的活：要么定长、要么长度前缀、要么分隔符，
在协程里就是一个循环 + 缓冲区拼接，和同步 socket 编程相同。

---

## 6.5 错误处理速查

| 场景 | 表现 | 处理 |
|---|---|---|
| 对端正常关闭 | `read` 返回 0 | 关闭/清理连接 |
| 连接被重置 | `read` 返回 -1，errno = `ECONNRESET`（原生码 WSAECONNRESET 10054 在 `io::last_error()`） | 视为断开 |
| `connect` 失败 | `valid() == false` | 查 `errno` / `io::last_error()` |
| `write` 失败 | 返回 -1 | 视为断开 |
| 停机时关闭监听 | `accept` 以错误/取消完成 | 退出 accept 循环 |
| 协程被取消 | IO 点抛 `CancelledError` | catch 后清理；底层 IO 已被库自动取消 |

小函数取原生错误码：

```cpp
#include <coro/io.hpp>
int native = coro::io::last_error();   // thread_local, co_await 返回后立刻读
```

---

## 6.6 挂起中的 IO 与取消

连接协程挂在 `read` 上时被 `cancel()`：库会先调用平台取消
（Windows `CancelIoEx` / Linux `io_uring_prep_cancel`），
等系统以"已取消"完成包唤醒协程后，再在 await 点注入
`CancelledError`。**先取消底层、后唤醒协程**的顺序由库保证，
你不会遇到"协程没了但系统调用还在往已释放的缓冲区写"的竞态。

这正是优雅停机的机制基础：停机 = 取消所有连接协程 →
各自的 catch 里清理 → 服务器进程干干净净退出（第 9 讲实战）。

---

## 6.7 练习

1. 给 echo 服务器加连接计数：并发连接数、累计连接数，
   客户端断开时递减。
2. 实现长度前缀协议：4 字节小端长度 + 载荷。
   客户端发 3 条消息，服务器逐条解析回显。（提示：read 可能少读，
   需要循环凑满长度头。）
3. 给连接处理加超时：单个 read 超过 10 秒无数据就断开
   （对连接协程整体用 `wait_for`，或对单次 read 用嵌套任务超时）。
4. （进阶）把 echo 服务的每个连接改为"先 `to_thread` 做 CPU 解密，
   再回写"——体会 IO 协程与阻塞内核的混合。

---

**下一讲**：[IO 扩展](07-io-extensions.md) —— 同一套异步机制覆盖
文件、管道、信号、目录监视、子进程。
