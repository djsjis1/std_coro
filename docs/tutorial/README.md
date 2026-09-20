# coro 使用教程（10 讲）

> 面向 **C++ 使用者**（不是协程实现者）的渐进式教程：每一讲引入少量新概念，
> 配一个可以直接编译运行的完整程序。
>
> 想搞懂协程**语言机制本身**（帧、promise_type、awaiter），请配合
> [C++20 协程课程](../cpp20-coroutines-course/README.md)阅读；
> 本教程聚焦"**用 coro 这个库把异步程序写出来**"。

## 课程地图

| 讲 | 标题 | 核心问题 | 新 API |
|---|---|---|---|
| 1 | [第一个协程程序](01-hello-coroutine.md) | 怎么跑起来？`Task` 是什么？ | `Task<T>` `co_return` `co_await` `sleep` `run` |
| 2 | [并发编程](02-concurrency.md) | 怎么同时做三件事？ | `spawn` `gather` `gather_all` `gather_void` |
| 3 | [取消与超时](03-cancel-timeout.md) | 怎么优雅地停？ | `cancel` `wait_for` `wait_any` `wait_tasks` `TaskGroup` |
| 4 | [同步原语与队列](04-sync-queue.md) | 并发协程怎么配合？ | `Lock` `Semaphore` `Event` `Condition` `Queue` |
| 5 | [Future 与线程池](05-future-thread.md) | 阻塞代码和老回调怎么办？ | `Promise/Future` `to_thread` `call_soon/later` |
| 6 | [TCP 网络编程](06-networking.md) | 怎么写一个并发服务器？ | `TcpListener` `TcpStream` |
| 7 | [IO 扩展](07-io-extensions.md) | 文件/管道/信号/子进程？ | `fs` `pipe` `signal` `fs_watch` `process` |
| 8 | [多核并行](08-multicore.md) | 怎么吃满所有 CPU 核？ | `EventLoop::get().run()` `Scheduler` |
| 9 | [构建 HTTP 服务](09-web-server.md) | 怎么部署真实的网络服务？ | `web_server` `router` `http_request/response` |
| 10 | [综合实战](10-final-project.md) | 上述全部怎么拼成一个项目？ | —（项目实战） |

## 学习建议

- **每讲 20-40 分钟**。代码全部亲手敲一遍（或改 `main.cpp` 练习场），
  不要只看——协程的很多坑只有运行了才有体感。
- **第 1-5 讲是地基**，只用 `#include <coro/coro.hpp>`，零平台依赖，
  任何能编译 C++20 的环境都能跑。
- **第 6-7 讲涉及平台 IO**：Windows 开箱即用（IOCP）；
  Linux 需要先 `sudo apt install liburing-dev`。
- **贯穿全课程的约定**：逃逸当前语句的任务优先使用**命名函数**或
  “无捕获 lambda + 按值参数”。捕获型协程 lambda 也受支持，但闭包必须
  活到任务结束。详见[第 1 讲](01-hello-coroutine.md#命名函数-vs-lambda协程体)与
  [FAQ](../faq.md)。

## 预备知识

- C++ 基础：类、模板基本用法、`std::unique_ptr/shared_ptr`、lambda、`std::chrono`
- **不需要**：了解 C++20 协程的实现原理（第 1 讲会用 3 分钟讲清使用层面需要的部分）
- 会 Python asyncio 更好——教程会不断对照，但不是必须

## 环境要求

| 项目 | 要求 |
|---|---|
| 编译器 | MSVC 2022 / GCC 11+（<14 加 `-fcoroutines`）/ Clang 14+ |
| CMake | 3.20+ |
| C++ 标准 | C++20 |
| 平台 | Windows 全功能；Linux 网络与 IO 需 `liburing-dev`；其他平台核心功能可用 |
