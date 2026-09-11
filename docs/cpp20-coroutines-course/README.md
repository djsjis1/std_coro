# C++20 协程系统学习课程

> 一套独立、自包含的 C++20 协程课程文档。
> 本课程**不依赖任何第三方协程库**，所有示例代码均为自包含的最小实现，
> 读者只需一个支持 C++20 的编译器即可从头开始理解协程的每一个机制。

## 本课程面向谁

- 已经掌握 C++ 基础语法（模板、移动语义、智能指针），想系统学习 C++20 协程的开发者
- 使用过 Python `async/await`、JavaScript `async/await` 或 Kotlin 协程，想理解 C++ 版本背后原理的开发者
- 准备基于协程实现自己的异步框架 / 事件循环 / 网络库的工程师

## 编译器要求

| 编译器 | 最低版本 | 说明 |
|---|---|---|
| MSVC | Visual Studio 2022 (19.31+) | 原生支持，无需额外参数 |
| GCC | 11（推荐 14+） | GCC < 14 需要 `-fcoroutines` |
| Clang | 14 | 需要 `-std=c++20` |
| Apple Clang | 15+ | 随 Xcode 15 发布 |

验证方式：编译运行 `02-first-coroutine.md` 中的第一个示例即可。

## 课程地图

课程分为四个阶段，共 14 讲。每一讲都是**独立文件**，可顺序学习，也可按需查阅。

### 第一阶段：建立心智模型（1-2 讲）

| 章节 | 文件 | 核心问题 |
|---|---|---|
| 01 协程基础概念 | [01-basics.md](01-basics.md) | 协程到底是什么？C++20 协程为什么是"无栈"的？ |
| 02 第一个协程 | [02-first-coroutine.md](02-first-coroutine.md) | 编译器把一个协程函数变成了什么？最小可运行示例 |

### 第二阶段：核心机制（3-6 讲）

| 章节 | 文件 | 核心问题 |
|---|---|---|
| 03 coroutine_handle 与协程帧 | [03-coroutine-handle.md](03-coroutine-handle.md) | 谁"持有"协程？协程的局部变量存在哪里？ |
| 04 promise_type 深入 | [04-promise-type.md](04-promise-type.md) | promise 是编译器与代码之间的"控制面板"，每个接口何时被调用？ |
| 05 awaiter 与 awaitable | [05-awaiter-awaitable.md](05-awaiter-awaitable.md) | `co_await` 一个对象时到底发生了什么？三种 `await_suspend` 返回值 |
| 06 挂起与恢复 | [06-suspend-resume.md](06-suspend-resume.md) | 对称传输 vs 不对称传输，调度器的最小实现 |

### 第三阶段：工程实践（7-10 讲）

| 章节 | 文件 | 核心问题 |
|---|---|---|
| 07 设计一个实用的 Task 类型 | [07-task-design.md](07-task-design.md) | 如何设计返回值、移动语义、RAII 生命周期管理 |
| 08 co_yield 与生成器 | [08-co-yield-generator.md](08-co-yield-generator.md) | 惰性序列、`co_yield` 的底层映射 |
| 09 取消与停止 | [09-cancellation.md](09-cancellation.md) | 如何让协程"随时可停"？CancelledError 模式与 `std::stop_token` |
| 10 错误处理与异常传播 | [10-error-handling.md](10-error-handling.md) | 异常如何穿过挂起点？`unhandled_exception` 的职责 |

### 第四阶段：高级主题（11-14 讲）

| 章节 | 文件 | 核心问题 |
|---|---|---|
| 11 单线程事件循环中的同步原语 | [11-sync-primitives.md](11-sync-primitives.md) | 没有线程的情况下，互斥锁、条件变量、队列怎么做？ |
| 12 异步 IO 基础 | [12-async-io.md](12-async-io.md) | 回调式 IO 如何变成 `co_await`？epoll/IOCP/io_uring 的关系 |
| 13 性能：协程帧与堆分配 | [13-performance.md](13-performance.md) | 协程有多大开销？堆分配消除（HALO）是什么？ |
| 14 常见陷阱与最佳实践 | [14-pitfalls.md](14-pitfalls.md) | 悬空引用、生命周期、调试技巧——前人踩过的坑 |

## 学习路径建议

**新手路径**（按顺序读完）：
`01 → 02 → 03 → 04 → 05 → 06 → 07 → 10 → 14`

完成这条路径后，你将能够：

- 读懂任何主流 C++ 协程库（cppcoro、folly、Boost.Cobalt 等）的核心代码
- 独立实现一个可用的 `Task<T>` 类型
- 解释 `co_await` 表达式的完整求值流程

**进阶路径**（想做异步框架/网络库）：
在基础路径之上继续 `08 → 09 → 11 → 12 → 13`。

**快速查阅**：每讲开头都有"核心要点"列表，可跳过讲解直接看结论。

## 本课程刻意不覆盖的内容

- 生成器之外的高级模式（如 `co_await` 运算符重载的库级滥用）
- 特定框架（如 OpenMP、CUDA 流）中的协程用法
- 编译器内部实现细节（LLVM coroutine lowering 等）

## 代码示例约定

所有示例遵循以下约定，保证开箱即用：

1. 每个 `.cpp` 示例都是**完整的单文件程序**，复制即可编译运行
2. 示例中的辅助类型（如 `lazy`、`generator`）随课程逐步构建，不依赖外部库
3. 编译命令以注释形式写在示例顶部，例如：

   ```cpp
   // 编译: g++ -std=c++20 example.cpp && ./a.out
   ```

### 各讲示例状态（均已在 MSVC 2022 Debug 实测通过）

| 章节 | 示例 | 说明 |
|---|---|---|
| 02 | `first.cpp` / `drive.cpp` | ✅ 完整程序 |
| 03 | `handle.cpp` | ✅ 完整程序 |
| 04 | `promise_trace.cpp` | ✅ 完整程序 |
| 05 | `sleep_awaiter.cpp` | ✅ 完整程序（`future_demo` 为片段） |
| 06 | `scheduler.cpp` | ✅ 完整程序 |
| 07 | `task_full.cpp` + `Task<void>` 特化 | ✅ 可拼装为完整程序 |
| 08 | `generator.cpp` | ✅ 完整程序 |
| 09 | `cancel_inject.cpp` | ✅ 完整程序 |
| 10 | `exception_flow.cpp` | ✅ 完整程序 |
| 11 | `AsyncLock` 等 + 第 6 讲调度器 | ✅ 组合后为完整程序 |
| 12 | — | 概念片段（平台 API 示例） |
| 13 | `alloc_count.cpp` | ✅ 完整程序 |
| 14 | — | 正反例片段 |

### 术语表

| 术语 | 一句话解释 |
|---|---|
| 协程函数 | 函数体含 `co_await`/`co_yield`/`co_return` 的函数 |
| 协程帧 | 堆上存状态的内存块（参数 + 跨挂起局部变量 + promise + 状态机状态） |
| `coroutine_handle` | 帧的遥控器：`resume()`/`done()`/`destroy()` |
| `promise_type` | 帧内控制台：编译器按固定时机调用其接口 |
| awaitable / awaiter | 可被 `co_await` 的对象 / 实现三件套的对象 |
| 对称传输 | `await_suspend` 返回目标句柄，挂起并直接切换执行 |
| 不对称传输 | 挂起后控制权回到调度器，由调度器决定下一个 |
| 惰性/急切启动 | `initial_suspend` 挂起（等 start）/ 不挂起（立即执行） |
| HALO | 堆分配消除：编译器把帧内联到调用者栈上 |
| 事件循环 | 就绪队列 + 定时器 + IO 等待的统一调度器 |

祝学习愉快！
