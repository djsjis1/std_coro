# FAQ 与故障排查

> 按"症状"索引。遇到问题先在这里找；没有的话按
> [架构剖析](architecture.md) 的心智模型排查。

## 目录

- [编译/链接问题](#编译链接问题)
- [程序行为不对](#程序行为不对)
- [挂死 / 卡住](#挂死--卡住)
- [崩溃 / UB](#崩溃--ub)
- [协程与编译器注意事项](#协程与编译器注意事项)
- [多线程相关](#多线程相关)
- [IO（网络/文件/子进程）相关](#io相关)
- [从 Python asyncio 迁来的心智差异](#从-python-asyncio-迁来的心智差异)

---

## 编译/链接问题

### error: `co_await` / `co_return` 需要 C++20

- CMake：`set(CMAKE_CXX_STANDARD 20)` + `set(CMAKE_CXX_STANDARD_REQUIRED ON)`；
  用 `coro::coro` 目标会自动携带。
- GCC 11~13 需要显式 `-fcoroutines`（GCC 14 起默认开启）；
  Clang 需要 14+。

### `sync.hpp` / `queue.hpp` 单独 include 报 Task 未定义

这两个头文件使用了 `Task<>` / `CancelledError` 但没有 include
`task.hpp`（历史遗留的隐式依赖）。解决：先 `#include <coro/task.hpp>`
或直接用 `#include <coro/coro.hpp>`。

### Windows 链接错误：找不到 `WSAStartup`/socket 符号

没有链接 `ws2_32`。用 `target_link_libraries(your_app PRIVATE coro::coro)`
让库目标自动携带；不要手写 include 路径绕过目标。

### Linux 网络编译失败：找不到 `liburing/io_uring.h`

```bash
sudo apt install liburing-dev    # Debian/Ubuntu
# 或 dnf install liburing-devel / pacman -S liburing
```

### MSVC 报 C1128：段超过对象文件限制

协程是模板重代码，单个翻译单元过大所致。拆分源文件，或
`/bigobj`。

---

## 程序行为不对

### 调用了协程函数但什么都没发生

`Task` 是**惰性**的：`my_task()` 只创建任务。要 `co_await my_task()`、
`t.start()` 或 `coro::spawn(my_task())` 才会执行。

### `coro::run` 立即返回，后台任务没跑完

`run` 的主协程完成后事件循环**会**等活跃协程跑完才退出——
但如果你的后台任务已经不被任何人引用（Task 被析构），帧已被销毁，
自然什么都不剩。检查：

1. `spawn` 的返回值有没有保存？
2. fire-and-forget 是不是用了正确姿势（`shared_ptr` 自持有，
   见 [第 6 讲](tutorial/06-networking.md#重点--fire-and-forget-的正确姿势)）？

### 结果顺序和完成顺序不一致

`gather` / `gather_all` 的结果**按参数顺序**排列，与完成顺序无关。
要"先完成先处理"，用 `wait_tasks(..., WaitMode::FirstCompleted)`
逐个收割，或自己实现完成回调。

### gather 里一个任务失败，其他任务没有立刻停

这是**设计行为**（对标 `asyncio.gather`）：等全部完成后重抛第一个
异常。要"一个失败立刻取消全部"，用 `TaskGroup`。

### `wait_any` / `wait_tasks` 落选者还在跑

竞速类 API **不取消**落选任务（对标 `asyncio.wait`）。落选者会自然
跑完，结果丢弃。需要"落选者立刻死"：自己 spawn 并在胜者出现后
`cancel()` 其余。

### 取消后协程还在跑了一会儿

取消在**下一个 await 点**生效。协程若正在执行不含 `co_await`
的纯计算段，要等它到达下一个挂起点。长计算应周期性
`co_await coro::yield()`（或拆分进 `to_thread`）。

### catch 住 CancelledError 之后任务"没死"

也是设计行为——**一次性注入 + 取消保护**：catch 后不再抛出，
任务可以继续运行甚至正常完成（对标 `asyncio.shield` 的效果）。
如果这不是你想要的，catch 里做完清理后重新 `throw;`。

---

## 挂死 / 卡住

### 程序整体卡死，一个协程都不动

三嫌疑：

1. **协程体里有不含 `co_await` 的死循环**（协作式调度无抢占）。
   挂上调试器看线程栈停在哪一行。
2. 在事件循环线程调用了**阻塞函数**
   （`std::this_thread::sleep_for`、同步 socket/文件/`std::mutex`
   锁等待）。包进 `coro::to_thread` 或换异步 API。
3. `std::mutex` 试图跨 `co_await` 持有 → 自我死锁或未定义行为。
   协程内一律用 `coro::Lock`。

### 事件循环不退出（run 不返回）

`run` 等待"就绪队列、定时器、挂起 IO、**活跃协程**"全部清空。
常见残留：

- spawn 的任务没被 await/cancel（孤儿任务把活跃计数撑着）；
- 忘记 `q.join()` 后取消常驻消费者；
- 挂在 `signal::wait` / `accept` 上（这是**故意的**——服务场景请用
  `EventLoop::stop()` 或取消主流程）。
- 定位工具：编译时定义 `CORO_TASK_REGISTRY`，运行中打印
  `coro::EventLoop::get().active_task_count()` 观察谁没结束。

### `run` 嵌套调用没有效果

不支持嵌套 `run()`（检测到直接返回）。一个线程同时只有一个
事件循环在跑。

### Condition/Event 等不到唤醒

- Condition 忘了 `notify`，或等待方没用 `while (!cond)` 循环
  （被虚假唤醒后条件其实不成立）；
- `set/notify` 发生在 `wait` 之前？Event 是粘性的（没问题），
  Condition 的 notify 是非粘性的（会丢）——先确认时序；
- 在**另一个线程**的原语上等待（原语不跨线程，见下节）。

---

## 崩溃 / UB

### spawn 返回值没保存，任务没有执行完

`coro::spawn(t());` 丢弃返回值后，临时 `Task` 会安全终止该任务；它不会
自动变成后台任务。保存返回值并稍后 `co_await`，或明确 `detach()`；定时
回调可用 `call_soon`/`call_later`（它们内部自持有）。

### "coroutine frame destroyed while suspended" 类崩溃

公共 `Task` 析构会撤销定时器/等待队列登记；挂起的底层 I/O 会先请求取消，
等完成包被消费后才释放帧。若仍出现此类问题，优先检查自定义 awaiter 是否在
帧销毁时撤销外部登记，以及协程参数/缓冲区指向的外部对象是否已经析构。
fire-and-forget 使用 `Task::detach()`。

### 同一个 Task 被 co_await 两次

Task 只能被等待一次。需要多个等待者：把结果放进 `Future`，
大家等 Future。

### TaskGroup wait 之后子任务结果不对

子任务的结果值不能从组里取（`spawn` 不返回句柄）。用引用参数/
共享状态带回；确认带回目标的生命周期覆盖整个 group 存活期。

### double-free / 帧被销毁两次

不要对同一个协程句柄 `destroy()` 两次——也不要既让 Task 析构
销毁帧，又手工 `handle.destroy()`。库的 RAII 已覆盖正常路径；
手工句柄操作仅限你完全理解 [架构剖析第 4 节](architecture.md#4-task-的-promise_type-设计) 时。

---

## 协程与编译器注意事项

### lambda 协程能不能捕获变量

能，但捕获仍属于 lambda 闭包对象，而不是协程帧。若从临时捕获闭包创建
任务，语句结束后闭包析构，任务恢复时访问捕获会 use-after-free：

```cpp
// ❌ 临时捕获闭包先于任务析构
for (int i = 0; i < 3; ++i)
    tasks.push_back([i]() -> coro::Task<int> {
        co_await coro::yield();
        co_return i * 10;
    }());

// ✅ 无捕获 lambda + 按值参数
auto worker = [](int i) -> coro::Task<int> {
    co_await coro::yield();
    co_return i * 10;
};
for (int i = 0; i < 3; ++i)
    tasks.push_back(worker(i));

// ✅ 命名协程函数
coro::Task<int> make(int i) { co_return i * 10; }
for (int i = 0; i < 3; ++i)
    tasks.push_back(make(i));
```

命名的捕获闭包只要确定活到任务结束也完全合法。需要 fire-and-forget 时，
用 `shared_ptr` **按值参数**传入协程帧，不要让临时 lambda 捕获自身。

### 只有 throw 的 Task 函数为什么同步抛异常

函数只有在函数体中出现 `co_await`、`co_yield` 或 `co_return` 时才是协程。
如果一个返回 `Task` 的函数只有 `throw`，它其实是普通函数，因此异常在调用时
同步抛出，不会进入 promise 的 `unhandled_exception`。加不可达的 `co_return`
只是为了让该函数被识别为协程；若函数其他路径已有协程关键字，就无需在每个
`throw` 后机械补写 `co_return`。

### Debug 崩溃但 Release 正常

先排查上面两条；再检查是否**跨线程创建/销毁了协程帧**
（Debug CRT 会在跨线程 free 时断言）——典型场景是把已创建的
Task 对象传给 `Scheduler`，应改为传**工厂**
（`sched.spawn_any([x]{ return task(x); })`）。

---

## 多线程相关

### 原语在别的线程上 set/notify 没反应

`Lock/Semaphore/Event/Condition/Queue` 是**事件循环内**原语，
不是线程原语——它们不属于任何线程，但要求操作它们（除 Future 的
set 外）都在各自 loop 线程上。跨线程一次性通知用
`Promise::set_value`（线程安全，自动路由唤醒）。

### 跨线程共享数据竞争

线程亲缘**不豁免跨线程共享**：不同 loop 线程碰同一个非原子变量
就是数据竞争。同一线程内协程间共享无需加锁；跨线程用
`std::atomic` / `std::mutex`，或干脆通过 Future/Promise 传值。

### socket 在 worker 线程上不可用（Windows）

socket 与创建线程的 IOCP 绑定。accept 出的连接要交给
Scheduler worker：`accept_noattach()` accept + worker 协程内
`reattach()`。Linux io_uring 无此约束。

### Scheduler 的任务都在一个 worker 上跑

`spawn_any` 按活跃协程数分发——如果你的任务都是"启动即完成"
的极短任务，主键全为 0，会退化到按累计分发数轮转。
确认你传的是**工厂**（`[x]{ return task(x); }`）而不是已创建的
Task（后者根本没有这个 API，别硬塞）。

---

## IO相关

### read 返回 -1，strerror 打印乱码

旧版库直接把 WSA 码塞进 errno 的历史问题已修复（现在自动转换成
标准 errno）。若你缓存了跨线程的 errno——errno 是 thread_local，
请在 `co_await` 返回后**立刻**读；精确平台码用
`coro::io::last_error()`（同样是 co_await 返回后立即读）。

### read 返回 0 是错误吗

不是。`0` = 对端正常关闭 / EOF，是正常的流终止信号；`-1` 才是错误。

### Windows 上文件 open/stat 是同步的

设计如此：元数据操作微秒级且被 OS 缓存，同步调用；
read/write/fsync 全异步。Linux 上 open 也走 io_uring 全异步。

### 目录监视事件太多 / 太碎

内核粒度粗：一次保存常触发多条 created/modified。
应用层去抖（同路径事件收进 100ms 窗口合并处理）。

### 子进程 stdin 写完子进程还在等

给子进程的输入没有 EOF。写完后调
`p.stdin_pipe()->close()`。

### Process 析构后子进程还活着

`Process` 析构**故意**不杀也不等（防误杀）。要收尾：
`co_await p.wait()`；要强杀：`p.terminate()`。

### Ctrl+C 直接杀掉了进程，优雅停机没触发

确认在发出信号前已创建 `signal::wait/handle`；Linux 的 `sigaction`
处理器在首个等待者注册时安装，更早的信号仍会走系统默认处理；
Windows 下确认用的是 `coro::signal::handle/wait`（库内桥接），
而不是混用 CRT `signal()`。

---

## 从 Python asyncio 迁来的心智差异

| asyncio 习惯 | coro 中的对应/差异 |
|---|---|
| `async def` | 返回 `coro::Task<T>` 的函数；lambda 也支持，但捕获闭包必须活到任务结束 |
| 协程对象即 async 函数调用 | 相同：`my_task()` 惰性创建 |
| `asyncio.create_task` | `coro::spawn`，但**必须保存返回值**（Python 有事件循环帮你持有，C++ 没有 GC） |
| `async with lock:` | `auto g = co_await lock.guard();`（RAII 作用域） |
| 永远不结束的守护任务 | 一样可以有，但退出时要负责 cancel + await（没有 loop.close 自动清扫） |
| `loop.call_soon` | `coro::call_soon`（生命周期全自动） |
| `asyncio.shield` | 没有专门 API：catch `CancelledError` 后继续即可 |
| GIL | 不存在——但要记住同 loop 单线程语义是**约定**不是运行时保证 |
| `await` 阻塞线程？ | `co_await` 永不阻塞线程；阻塞的是"协程"。别把阻塞函数直接放进协程体 |

还有一条总纲：**Python 的运行时替你兜底的事（GC 持有任务、
事件循环持有回调、异常默认打印），C++ 里要么是库的明确 API
（detach 兜底打印、call_soon 自持有），要么是你的责任（保存 spawn
返回值、管理对象生命周期）。**
