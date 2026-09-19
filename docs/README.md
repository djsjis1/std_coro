# coro 文档中心

> C++20 协程事件循环框架 — 类似 Python asyncio 的使用体验。
> header-only、零第三方依赖、Windows IOCP / Linux io_uring 零配置。

这里是 coro 项目的完整文档地图。按你的目标选择入口：

| 你的目标 | 推荐路径 |
|---|---|
| 5 分钟跑起来 | [README 快速开始](../README.md) → [教程第 1 讲](tutorial/01-hello-coroutine.md) |
| 系统学会使用这个库 | [使用教程（10 讲）](tutorial/README.md) |
| 查某个 API 的精确语义 | [API 参考手册](api-reference.md) |
| 理解库内部是怎么实现的 | [架构与源码剖析](architecture.md) |
| 学 C++20 协程语言本身 | [C++20 协程课程（14 讲）](cpp20-coroutines-course/README.md) |
| 从协程原理到实战的完整路径 | 协程课程（原理）→ 使用教程（本库）→ 架构剖析（源码） |
| 部署 HTTP 服务 | [Web 框架指南](web-framework.md) |
| 性能调优 / 压测 | [性能指南](performance.md) |
| 排查问题 | [FAQ 与故障排查](faq.md) |
| 了解后续计划 / 参与开发 | [路线图](roadmap.md) |

---

## 文档总览

### 入门

- **[README](../README.md)** — 项目简介、5 秒快速开始、asyncio 映射速查表。
- **[coro-guide.md](coro-guide.md)** — 一篇合并的完整指南：协程速览 + 核心 API + IO 扩展 + 生命周期规则。
- **[使用教程（10 讲）](tutorial/README.md)** — 从零开始、每讲一个可运行程序的渐进式教学：
  1. [第一个协程程序](tutorial/01-hello-coroutine.md) — 环境搭建、Task、惰性启动、事件循环
  2. [并发编程](tutorial/02-concurrency.md) — spawn / gather / wait_tasks 三种并发形态
  3. [取消与超时](tutorial/03-cancel-timeout.md) — cancel / wait_for / TaskGroup 结构化并发
  4. [同步原语与队列](tutorial/04-sync-queue.md) — Lock / Semaphore / Event / Condition / Queue
  5. [Future 与线程池](tutorial/05-future-thread.md) — 桥接回调式 API、to_thread、跨线程通信
  6. [TCP 网络编程](tutorial/06-networking.md) — echo 服务器与客户端、并发连接、错误处理
  7. [IO 扩展](tutorial/07-io-extensions.md) — 文件 / 管道 / 信号 / 目录监视 / 子进程
  8. [多核并行](tutorial/08-multicore.md) — loop-per-thread 与 Scheduler 自动分发
  9. [构建 HTTP 服务](tutorial/09-web-server.md) — 用库自带 Web 框架 + 路由搭建服务
  10. [综合实战](tutorial/10-final-project.md) — 并发文件处理器：全功能串联的项目

### 参考

- **[API 参考手册](api-reference.md)** — 全部公开类型的签名、语义、错误约定、线程安全性，
  按头文件组织：task / sleep / gather / wait / task_group / sync / queue / future /
  thread / schedule / scheduler / net / fs / pipe / signal / fs_watch / process / io。
- **[架构与源码剖析](architecture.md)** — 事件循环主循环逐行讲解、Task 的 promise_type 设计、
  取消机制的完整链条、三大事件源（IOCP / io_uring / CV）、include 依赖图、
  八大横切设计模式。想给库贡献代码或学习协程框架设计必读。
- **[性能指南](performance.md)** — 实测性能数据、协程成本模型、压测方法、调优建议。
- **[Web 框架指南](web-framework.md)** — Web/ 目录的 HTTP 服务器：路由、动态参数、静态目录、
  优雅关停、压测；以及 router/radix_router.h 基数树路由的独立使用。
- **[路线图](roadmap.md)** — 后续优化与扩展的**教学级实施手册**：现状缺口、改库通用方法论
  （含 TcpStream::read 范本逐行讲解）、分阶段任务（逐步操作 + 可照抄代码骨架 + 验证命令）、
  横切门禁与风险回退。想给库贡献代码从这里认领任务。

### 教学

- **[C++20 协程课程（14 讲）](cpp20-coroutines-course/README.md)** — 讲语言本身的协程机制
  （帧、promise_type、awaiter、对称传输、生成器、取消、异常、性能、陷阱），
  不依赖本库，全部示例可用任意 C++20 编译器编译。
- **[架构与源码剖析](architecture.md)** — 也可作为"如何设计一个协程框架"的进阶教材。

### 帮助

- **[FAQ 与故障排查](faq.md)** — 按症状索引：编译错误、链接错误、运行时挂死、UB 崩溃、
  MSVC Debug 特有问题、跨线程问题。
- **[已知限制](#)** — 见 [coro-guide.md 第十五节](coro-guide.md#十五已知限制)。

---

## 一图看懂项目结构

```
coro/                     # 项目根
├── coro/                 # ★ 库本体（header-only，可整体复制到别的项目）
│   ├── CMakeLists.txt    #   库目标 coro::coro
│   └── include/coro/     #   24 个头文件（核心 14 + IO 6 + 平台事件源 2 + 总入口等）
├── examples/             # 8 个可运行示例
├── tests/                # googletest 单元测试（22 个文件）+ 高并发压测 stress.cpp
├── Web/                  # HTTP 服务器框架（llhttp 解析 + 路由 + 多线程 worker）
├── router/               # 独立的泛型基数树路由 radix_router<T>（header-only）
├── docs/                 # 本文档中心
│   ├── tutorial/         #   使用教程（10 讲）
│   └── cpp20-coroutines-course/  #   C++20 协程语言课程（14 讲）
├── main.cpp              # 协程练习场（改完直接编译运行）
└── CMakeLists.txt        # 开发仓库根 CMake
```

## 推荐学习路线

**如果你已经会 Python asyncio**：
README 的映射速查表 → 直接翻 [API 参考](api-reference.md) → 写代码，
遇到问题查 [FAQ](faq.md)。

**如果你是 C++ 协程新手**：
1. [教程 1-5 讲](tutorial/README.md)：只用核心库把并发、取消、同步、桥接学透；
2. [协程课程 1-8 讲](cpp20-coroutines-course/README.md)：搞懂 `Task` 底下发生了什么；
3. [教程 6-10 讲](tutorial/README.md)：网络与 IO 扩展实战；
4. [架构剖析](architecture.md)：读源码，理解事件循环与取消机制。

**如果你想给库贡献代码**：
[协程课程](cpp20-coroutines-course/README.md)（原理）→ [架构剖析](architecture.md)
（现状）→ [测试目录](../tests/)（行为基线）→ 提 PR。
