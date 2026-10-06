# coro API 参考手册

> 全部公开类型的签名、语义、错误约定与线程安全性。按头文件组织。
>
> - 教程式讲解见 [使用教程](tutorial/README.md)；内部实现见 [架构剖析](architecture.md)。
> - 除非特别标注"线程安全"，所有操作默认**须在事件循环线程**内调用。
> - 约定：`Task<T>` 均指 `coro::Task<T>`；时长用 `std::chrono` 类型。

## 目录

- [总入口 coro.hpp](#总入口-corohpp)
- [异常类型 exceptions.hpp / task.hpp](#异常类型)
- [Task task.hpp](#task-taskhpp)
- [时间 sleep.hpp](#时间-sleephpp)
- [定时器 timer.hpp](#定时器-timerhpp)
- [上下文与取消 context.hpp](#上下文与取消-contexthpp)
- [通道 channel.hpp](#通道-channelhpp)
- [多路等待 select.hpp](#多路等待-selecthpp)
- [限流 rate_limit.hpp](#限流-ratelimithpp)
- [字节流 stream.hpp](#字节流-streamhpp)
- [异步 DNS dns.hpp](#异步-dns-hpp)
- [Unix 域套接字 unix.hpp](#unix-域套接字-unixhpp)
- [资源池 pool.hpp](#资源池-poolhpp)
- [TLS tls.hpp](#tls-tlshpp)
- [HTTP 协议 http.hpp](#http-协议-httphpp)
- [HTTP 客户端 http_client.hpp](#http-客户端-http_clienthpp)
- [TCP/UDP 服务与关闭结果](#tcpudp-服务与关闭结果)
- [并发组合 gather.hpp / wait.hpp](#并发组合)
- [TaskGroup task_group.hpp](#taskgroup-task_grouphpp)
- [同步原语 sync.hpp](#同步原语-synchpp)
- [Queue queue.hpp](#queue-queuehpp)
- [Promise / Future future.hpp](#promise--future-futurehpp)
- [to_thread thread.hpp](#to_thread-threadhpp)
- [定时回调 schedule.hpp](#定时回调-schedulehpp)
- [Scheduler scheduler.hpp](#scheduler-schedulerhpp)
- [事件循环 event_loop.hpp](#事件循环-event_loophpp)
- [错误模型 io.hpp（IO 各模块共用）](#错误模型-iohpp)
- [TCP 网络 net.hpp](#tcp-网络-nethpp)
- [文件 fs.hpp](#文件-fshpp)
- [管道 pipe.hpp](#管道-pipehpp)
- [信号 signal.hpp](#信号-signalhpp)
- [目录监视 fs_watch.hpp](#目录监视-fs_watchhpp)
- [子进程 process.hpp](#子进程-processhpp)

---

## 总入口 coro.hpp

```cpp
namespace coro {
    template <typename T> T run(Task<T> task);   // Task<> 特化返回 void
}
```

| API | 语义 |
|---|---|
| `coro::run(task)` | 启动任务并驱动**当前线程**的事件循环直到无工作。返回主协程 `co_return` 值；主协程的异常在此重新抛出。嵌套调用直接返回。 |

`coro.hpp` 聚合**核心 + 高级并发**头文件（event_loop / task / exceptions /
sleep / gather / future / sync / queue / schedule / wait / task_group /
thread / scheduler）。**不包含** IO 模块——网络与扩展需单独 include：

```cpp
#include <coro/net.hpp>      // TCP
#include <coro/fs.hpp>       // 文件 + 目录监视 (fs_watch)
#include <coro/pipe.hpp>     // 管道 (+ Linux fd 轮询)
#include <coro/signal.hpp>   // 信号
#include <coro/process.hpp>  // 子进程
```

---

## 异常类型

```cpp
// task.hpp
struct coro::CancelledError : std::runtime_error;   // "coroutine cancelled"
struct coro::TimeoutError   : std::runtime_error;   // "operation timed out"

// exceptions.hpp
class coro::ExceptionGroup : std::runtime_error {
public:
    const std::vector<std::exception_ptr>& exceptions() const noexcept;
};
```

| 类型 | 何时抛出 |
|---|---|
| `CancelledError` | 任务被 `cancel()`，在下一个 await 点注入；调用方主动取消 `wait_for` 时原样传播；组/父级取消传播 |
| `TimeoutError` | `wait_for` 超时；`fs::watch` 的 `next()` 等配合 wait_for 使用时同理 |
| `ExceptionGroup` | `TaskGroup::wait()` 有子任务真实失败（单个也打包；组内取消产生的 CancelledError 不聚合） |

---

## Task task.hpp

```cpp
template <typename T> class coro::Task {
public:
    Task() = default;                       // 空任务
    Task(Task&&) noexcept;                  // 仅可移动 (拷贝已删除)
    ~Task();                                // 帧未完成则销毁帧

    // ---- 等待/启动 ----
    T await_resume();                       // co_await 时取结果 (异常重抛)
    void start();                           // 显式启动 (只允许一次)
    T take_result();                        // 非协程上下文取结果 (coro::run 用)
    bool is_ready() const noexcept;         // 结果是否已就绪
    bool is_started() const noexcept;

    // ---- 取消 ----
    void cancel();                          // 线程安全; 见下"取消语义"

    // ---- 高级 ----
    void detach() noexcept;                 // 放弃所有权: 协程自持有到完成
                                            // detach 后异常走 detached 兜底打印
    std::coroutine_handle<> handle() const; // 原始句柄 (知道自己在做什么再用)
};
template <> class coro::Task<void> { /* 同上, 无返回值 */ };

namespace coro {
    template <typename T> Task<T> spawn(Task<T> task);   // start + 返回
}
```

### 语义要点

- **惰性启动**：调用协程函数只创建任务不执行；启动入口三选一：
  `co_await task` / `task.start()` / `coro::spawn(task)`。
- **只能被 co_await 一次**；重复等待是 UB。
- **生命周期**：Task 是协程帧的 RAII 所有者。销毁未完成的 Task 会安全终止该任务；
  若要让 `spawn` 创建的任务继续运行，必须保存返回的 Task，或明确调用
  `start()` 后再 `detach()`。丢弃 `spawn` 返回值不会自动变成后台任务。
- **移动语义**：move 后原 Task 变空；promise 与外壳的双向回指在
  move 中正确更新。
- **detach 的异常**：不会静默丢失——全局
  `coro::detail::detached_exception_handler()`（默认 stderr 打印，
  `CancelledError` 静默）可整替。

### cancel() 取消语义（对标 Python task.cancel()）

- 可从**任意线程**调用；
- 尚未启动 → 任务直接以 `CancelledError` 为结果（函数体不执行）；
- 挂起中 → 在**当前 await 点**被唤醒并抛 `CancelledError`
  （含挂起在网络/文件 IO 上的：库先取消底层系统调用再唤醒）；
- 在就绪队列中 → 运行到第一个 await 点抛出；
- **一次性注入**：协程体 catch 住且不再抛，任务可继续正常运行完成
  （"取消保护"，对标 shield 的效果）；
- 等待被取消任务的协程同样收到 `CancelledError`。

---

## 时间 sleep.hpp

```cpp
namespace coro {
    // 挂起当前协程一段时长 (steady_clock, 不受系统时间影响)
    template <typename Rep, typename Period>
    /*awaiter*/ sleep(std::chrono::duration<Rep, Period> d);

    // 让出执行权: 排到就绪队列队尾 (等价 sleep(0))
    /*awaiter*/ yield();
}
```

- 精度取决于 OS 定时器（典型 ~1-15ms）。
- 取消安全：挂起中协程被取消/销毁时，定时器堆条目自动变
  "僵尸"并被惰性清理，绝不 resume 已销毁的帧。
- `co_await coro::yield()` 不经定时器堆，开销最小；当前压力测试场景的平均值约
  202ns/次（包含计数开销，具体值随机器变化）。

---

## 定时器 timer.hpp

> 属于并发扩展，需 `-DCORO_ENABLE_CONCURRENCY_EXT=ON`（默认 OFF）。
> 它**不进 `coro/coro.hpp` 聚合头**，请 `#include <coro/timer.hpp>` 显式引入。

一次性、可取消、可重置的定时器；**不会自己起后台任务**，周期行为由你循环 `reset`。

```cpp
#include <coro/timer.hpp>
using namespace std::chrono_literals;

coro::Timer idle{500ms};
if (co_await idle.wait()) {
    // 到期
} else {
    // 被 cancel() 唤醒
}
idle.reset(500ms);   // 上一轮已结束 -> 重新开局, 下一轮 wait() 会真的再等
idle.cancel();       // 唤醒等待者, wait() 返回 false
```

| 接口 | 语义 |
| --- | --- |
| `Timer{duration}` / `Timer{}` | 带期限构造，或先建后 `reset`（无限期：只由 `cancel` 唤醒） |
| `Task<bool> wait()` | 到期 `true`；被 `cancel()` 唤醒 `false`；等待任务自身被取消仍抛 `CancelledError` |
| `reset(duration)` | 有等待者时只把它的 deadline 往后搬；无等待者时重新开局 |
| `cancel()` | 非阻塞，可从任意线程调用（唤醒投递回等待者所属 loop） |
| `done()` / `has_waiter()` | 是否已进终态 / 是否正在被等待 |

要点：

- **只允许一个等待者**：第二个 `wait()` 当场抛 `StructuredConcurrencyError`，
  而不是"谁被唤醒看调度运气"。
- **析构等价于 `cancel()`**：不会留下再也醒不过来的等待者。
- 全部使用 `steady_clock`，不受系统时间调整影响。

## 上下文与取消 context.hpp

> 同样属于并发扩展（需开关），不进聚合头：`#include <coro/context.hpp>`。

只携带两件事：**取消关系**与 **deadline**（`steady_clock`）。不内置日志、服务定位
或任意键值容器；必须**显式传递**，没有 thread_local 隐式上下文。

```cpp
#include <coro/context.hpp>
using namespace std::chrono_literals;

coro::CancellationSource src;
auto parent = coro::Context::from(src.token()).with_deadline(5s);
auto child  = parent.make_child();          // 继承取消关系; deadline 取较早值

child.throw_if_cancelled();                 // 长任务里的同步检查点
co_await child.wait();                      // 取消 -> CancelledError; 超期 -> TimeoutError
src.cancel();                               // 非阻塞, 可从任意线程调用
```

| 接口 | 语义 |
| --- | --- |
| `Context::root()` / `Context::from(token[, deadline])` | 根上下文 / 从取消源的令牌派生 |
| `Context::with_timeout(d)` | 只带期限、不带取消源 |
| `make_child()` | 继承取消关系与父 deadline（不新建取消链） |
| `with_deadline(d)` | 派生额外带期限的视图，**不会放宽**已有期限 |
| `wait()` | 挂起直到取消或超时，分别抛 `CancelledError` / `TimeoutError` |
| `throw_if_cancelled()` | 同步检查点：已取消抛 `CancelledError`，已超期抛 `TimeoutError` |
| `cancelled()` / `deadline_passed()` / `deadline()` / `cancellation()` | 状态查询与取令牌 |
| `CancellationSource::cancel()` | 唤醒**全部**等待者，每个都回到它自己的 loop；重复调用无副作用 |
| `CancellationToken::cancelled()` / `wait_cancelled()` | 只观察取消（这里取消是正常结局，不抛异常） |

要点：

- 取消与超时同时发生时**取消优先**（`cancel` 已被 `claim_cancel` 串行化）；只有未被
  取消时才报告超时。
- 取消源比令牌/上下文先析构是安全的：取消状态由 `shared_ptr` 共享，不反指回源。
- 默认构造的 `CancellationToken` 视为**已取消**——它不可能再有人 cancel，定为
  "永不取消"会静默吞掉本该发生的取消。

### 取消屏蔽 `cancellation_shield`

```cpp
co_await ctx.wait_and_save();                 // 假设: 中间步骤
{
    auto guard = src.make_shield();           // 作用域内本源的 cancel() 被延后
    co_await commit_irreversible_step();      // 这一步不该被取消打断
}                                             // 退出时若期间有 cancel 请求, 立即补发
```

| 行为 | 语义 |
| --- | --- |
| 作用域内 `src.cancel()` | 只记账：`token.cancelled()` 仍为 `false`，等待者不被唤醒 |
| 最后一个作用域退出 | 补发取消：置终态并按各等待者的 loop 投递唤醒 |
| 嵌套 | 按计数，最外层退出才生效 |
| `Context::wait()` 的 deadline | **不受屏蔽影响**：只延后"取消"，超时照常报 `TimeoutError` |
| 不使用屏蔽时 | 与原先完全一致（取消立即置终态） |

注意它屏蔽的是**某个源**的取消，作用域由 RAII 对象决定；这与 `asyncio.shield(task)`
"保护被 await 的那个任务"角度不同，本库的上下文是显式传递的，所以按源屏蔽更自洽。
对象可移动、不可拷贝，也不提供赋值——避免"退出时机"被复制搞混。

## 通道 channel.hpp

> 并发扩展（需 `-DCORO_ENABLE_CONCURRENCY_EXT=ON`），不进聚合头：`#include <coro/channel.hpp>`。
> loop-local：只在创建它的 `EventLoop` 线程内使用，不跨线程投递。

```cpp
auto ch = coro::channel<int>::bounded(8);   // 或 rendezvous() / unbounded()
auto tx = ch.make_sender();                 // 端点句柄可拷贝, 分发给多个生产者
auto rx = ch.make_receiver();

co_await tx.send(42);                       // 满则挂起 (背压), 关闭则抛 ClosedChannelError
if (auto v = co_await rx.recv()) use(*v);   // 先排空存量, 之后返回 nullopt 表示 EOF
```

| 接口 | 语义 |
| --- | --- |
| `bounded(n)` / `rendezvous()` / `unbounded()` | 有界 / 容量 0（只直接交接） / 显式无界 |
| `make_sender()` / `make_receiver()` | 端点句柄，可拷贝分发给多个生产者/消费者 |
| `sender::send(T)` | 协程；满则挂起，通道关闭时抛 `ClosedChannelError` |
| `sender::try_send(T)` | 非阻塞：`true` 已受理，`false` 需要等待；未命中时值仍归调用方 |
| `receiver::recv()` | 协程；空则挂起，返回 `optional<T>`，`nullopt` 即 EOF |
| `receiver::try_recv(out)` | 非阻塞取一个值（含直接从挂起发送者手里取） |
| `close()` / `closed()` / `size()` | 关闭发送侧（存量仍可排空）/ 状态查询 |

关闭与所有权合同：

- **最后一个 `sender` 释放等价于关闭发送侧**；**最后一个 `receiver` 释放**会让挂起的
  发送者以 `ClosedChannelError` 退出，不会永久等待。
- 关闭后接收侧**先排空缓冲**再报 EOF；EOF 用 `nullopt` 表示，与"暂时没值"区分靠 `closed()`。
- **取消安全**：挂起的发送者被取消时，其值随等待节点一起消失（既不残留也不误交给后来者），
  占用的名额归还。这一点由析构兜底保证——任务被取消时 `CancelledError` 由框架的取消检查
  包装器抛出，**不会经过本 awaiter 的 `await_resume`**。
- 支持 move-only 元素：值只在"入队/成功交接"处移动。

与 `coro::Queue` 的分工（刻意不合并）：`Queue` 面向 `task_done/join` 的任务完成计数
模型，没有 rendezvous、没有端点句柄、也没有"关闭后排空再 EOF"；两者合同不同，合并会
让两边语义都变模糊。

## 多路等待 select.hpp

> 并发扩展（需 `-DCORO_ENABLE_CONCURRENCY_EXT=ON`），不进聚合头：`#include <coro/select.hpp>`。
> 分支只接受 `channel` 的 send/recv、`after()` 定时器与 `default_nowait()`。

```cpp
auto r = co_await coro::select(
    coro::recv_of(rx1),          // 分支 0: 值在 r.value (nullopt = 该通道已 EOF)
    coro::recv_of(rx2),          // 分支 1
    coro::send_of(tx, 42),       // 分支 2: 可与 recv 混用, 元素类型须一致
    coro::after(100ms),          // 分支 3: r.timed_out
    coro::default_nowait());     // 分支 4: r.defaulted, 绝不挂起
switch (r.index) { /* ... */ }
```

| 接口 | 语义 |
| --- | --- |
| `select(specs...)` | 协程；第一个参数必须是 `recv_of`/`send_of`（由它决定元素类型） |
| `recv_of(rx)` / `send_of(tx, v)` | 通道分支；所有分支元素类型必须一致（编译期检查） |
| `after(d)` / `default_nowait()` | 定时器 / 立即分支；`default` 存在时任何情况下都不挂起 |
| `select_result<T>` | `index`（赢家分支号）+ `value`（recv 载荷，nullopt 即该通道 EOF）+ `timed_out`/`defaulted` |

合同与实现要点：

- **只有一个分支能提交副作用**：内部按 登记 → 仲裁 → 提交 → 撤销 执行，等待节点挂在
  通道队列上时会先过"成交闸门"，输家不会被交付，撤销即干净退出——**不会发生"落选的
  recv 其实已经取走了消息"**（这正是它与 `wait_any` 的本质区别：wait_any 的落选任务会
  继续执行完）。
- 快速路径按**轮转起点**扫描，多个分支同时就绪时不固定偏向第一个。
- 关闭的通道分支表现为该分支的 `nullopt`（EOF），不会吞掉其他分支的机会。
- select 退出后通道上**不残留任何登记**：后到的值照常可被接收。
- 不支持嵌套 select 与任意 `Task` 参与竞速（有意划小的第一版范围）。

## 限流 rate_limit.hpp

> 并发扩展：测试与示例受 `-DCORO_ENABLE_CONCURRENCY_EXT=ON` 门控，头本身只依赖纯核心。
> `#include <coro/rate_limit.hpp>`。loop-local，与 `channel` 一致不跨线程投递。

```cpp
coro::rate_limiter rl(100, std::chrono::seconds(1));  // 容量 100, 每 1s 补 100
co_await rl.acquire();                                // 不足则挂到补充点, 不会失败
if (!rl.try_acquire()) 走降级分支;                      // 非阻塞
co_await rl.acquire_n(5);                              // 批量: 要么给 5 个, 要么一个不扣
auto ms = rl.retry_after<std::chrono::milliseconds>(1); // 给上层回 429 Retry-After
```

| 接口 | 语义 |
| --- | --- |
| `rate_limiter(capacity, period[, refill])` | 桶容量（允许的突发）/ 补充周期 / 每周期补充量（缺省 = capacity）；初始满桶 |
| `acquire()` / `acquire_n(n)` | 协程；不足时挂到下一个补充点；可被取消（抛 `CancelledError`） |
| `try_acquire(n = 1)` | 非阻塞原子扣减；失败立即返回 `false` 且**不部分扣减** |
| `available()` | 观测用；不要"读了再扣"（中间有竞态），要判定就用 `try_acquire` |
| `retry_after<Duration>(n)` | 距下次可满足 n 个的时长；已满足时为 0 |
| `set_rate(capacity, period[, refill])` | 运行时改速率；调小容量会截断存量，避免长期空转 |

三条设计取舍（同步写在实现注释里）：

- 令牌用**定点整数**（1 令牌 = 1024 份）而非 `double`：`double` 无法原子 CAS，而
  "结算 + 扣减"必须是单条原子操作，否则并发 `acquire` 会超发。
- 等待靠 `coro::sleep` 到下一个补充点，**不自建等待队列**：队列要处理取消摘链、帧销毁、
  惊群唤醒三件事，而 sleep 的 token 作废机制已解决这些。代价是醒来者靠 CAS 抢令牌，
  抢输的多睡一轮 —— 有界延迟、无死锁、无泄漏。
- **不保证 FIFO 公平性**：只保证不超发、不永久饿死。

与 `coro::Semaphore` 的分工：Semaphore 限"**同时**在途数量"（并发度），令牌桶限"**速率**"
并允许突发。要"最多 N 个并发"用 Semaphore，要"每秒 N 次"用 rate_limiter。

## 字节流 stream.hpp

> 只依赖协程核心；`#include <coro/stream.hpp>`。作用：把 `TcpStream`/`PipeEnd`/
> TLS 流上“一次 N 字节”的裸读裸写，包成协议层常用的“读一行 / 读满定长 / 全量写出”。

```cpp
coro::net::TcpStream sock = co_await coro::net::TcpStream::connect("127.0.0.1", 8080);
coro::stream_reader  reader(sock);        // 不接管所有权: sock 必须活得比 reader 久
coro::stream_writer  writer(sock);

while (auto line = co_await reader.read_line()) handle(*line);
co_await writer.write_all("PING\r\n");
```

| 接口 | 语义 |
| --- | --- |
| `read_line()` | 到下一个 LF（CR LF / 单 LF 均可，行尾不进结果）；`nullopt` = 已结束且无残留 |
| `read_until(delim)` | 自定义分隔符（分隔符自己吞掉）；尾部无分隔符的残段会作为最后一段交出 |
| `read_some(dst, n)` | 只要读到 ≥1 字节就返回（短读被吸收）；`nullopt` = EOF |
| `read_exactly(dst, n)` | 读满 n；凑不齐就结束 → 抛 `IncompleteStreamError`（带 `requested`/`received`） |
| `write_all(data, n)` | `true` = 全写完；`false` = 对端不可写（**已写出的部分不回退**，调用方需知道协议已不一致） |
| `write_line(s)` | `write_all(s)` + 一个 LF |
| `buffered()` | 已缓冲未消费字节数（跨次调用不丢数据） |

`max_capacity` 是硬上限：初始容量超过它时会缩小，值为 0 抛 `invalid_argument`。
`read_until()` 的分隔符不能为空；EOF 处单独的 CR 会保留为数据并消费。
字符串写入重载和分隔符会复制到任务里，允许传入临时字符串；指针重载仍要求调用者
保持缓冲有效到操作结束。同一 reader 的读取必须串行。

四种结局严格可区分（本层的核心合同，测试逐条锁定）：

| 情形 | 表现 |
| --- | --- |
| 对端正常结束 | `nullopt`（read_*）/ `false`（write_* 进不动了） |
| IO 错误 | 抛 `std::system_error`（`errno` 与 `io::last_error()` 已填好） |
| 被取消 | 抛 `CancelledError`（由 Task 的取消注入负责，本层不拦） |
| 要求读满却提前结束 | 抛 `IncompleteStreamError` |

另有“零进展写入”（`write` 返回 0）专门作为 `false` 返回而不是继续循环——它意味着对端
已不可写，继续转就是死循环。

多态方式选模板而非基类：`AsyncReadable`/`AsyncWritable` 只看得到两个方法，但每个方法都是
awaitable——用虚函数擦除就得让虚函数返回类型擦除的 awaiter，代价与复杂度都不划算；按
`Source` 模板化既零开销，也不会催生一个“什么都能包”的巨大基类。已有类型 `TcpStream`、
`PipeEnd` 直接满足这两个 concept，无需适配。

## 异步 DNS dns.hpp

> `#include <coro/dns.hpp>`。仅在具备原生网络的构建里有效（Windows 或 Linux+io_uring，
> 由头内 `CORO_HAS_DNS` 自行判定）；纯核心配置下它编译为空，不给消费者添依赖。

```cpp
auto eps = co_await coro::net::resolve("example.com", "80");   // 不占用事件循环线程
if (eps.empty()) { /* 查不到地址: 不算错误 */ }
auto sock = co_await coro::net::connect(eps[0]);               // 端点直接可连
```

| 接口 | 语义 |
| --- | --- |
| `resolve(host, service)` | 协程；返回 `vector<resolved_endpoint>`；失败抛 `DnsResolutionError`（带 `getaddrinfo` 返回码） |
| `connect(endpoint)` | 按解析结果建立 `TcpStream`（把端点适配回 `ip + port` 接口） |
| `resolved_endpoint` | `address`（点分 IPv4）、`port`（**主机字节序**）、`canonical_name`（可能为空） |

实现取舍与边界（都写进了头文件注释）：

- `getaddrinfo` 是同步阻塞调用（查 DNS、读 hosts、走 nsswitch），在事件循环线程上直接调
  会让整个 loop 停摆，一个慢 DNS 服务器就能拖死所有连接；因此交给**已有的** `to_thread`
  线程池，不新增依赖也不引入第二套调度器。
- 代价如实说明：这不是真异步——一次慢解析会占住一个线程池线程；无解析超时、无重试、
  无 SRV。需要这些时把 c-ares 作为**可选增强**源码化进 `thirdparty/` 并用开关门控
  （接口不变、只换实现）。
- **v1 只返回 IPv4**：`net.hpp` 目前只构造 `sockaddr_in`，返回 v6 地址等于交给调用方一个
  连不上的东西；等 `net.hpp` 支持 `sockaddr_in6` 再放开。
- 不做结果缓存与 happy-eyeballs 排序（属连接池 / HTTP Client 层策略），不做反向解析。
- 系统解析失败抛 `coro::net::DnsResolutionError`，保留旧 detail 名字的兼容别名；
  Windows 上解析会先初始化 Winsock，不要求调用者先创建 socket。
- 测试刻意用**非法服务名**触发失败而非"未知主机"：本机存在把任意域名（含 RFC 6761 保留的
  `.invalid`）解析到 198.18.0.8 的拦截器，用主机名断言会变成分环境偶发。

## Unix 域套接字 unix.hpp

> **仅 Linux + io_uring**。其他平台/配置下 `CORO_HAS_UNIX=0`，本头**编译为空**（不提供假实现），
> 因此非 Linux 消费者零成本：`#include <coro/unix.hpp>`。

```cpp
coro::net::UnixListener listener;
listener.bind("/tmp/app.sock");            // 或 listener.bind_abstract("myname") -> "@myname"
auto conn = co_await listener.accept();
co_await conn.write(msg, len);

auto peer = co_await coro::net::UnixStream::connect("/tmp/app.sock");
int n = co_await peer.read(buf, sizeof buf);   // 0 = 对端关闭 (EOF), -1 = 错误 (errno 已设)

coro::net::UnixStream a, b;                     // 无路径的互联端点对 (同机 IPC 最省事)
if (coro::net::unix_pair(a, b)) { ... }
```

| 接口 | 语义 |
| --- | --- |
| `bind(path[, backlog])` | 绑定文件系统路径；bind 前 `unlink` 同名陈旧 inode（否则 `EADDRINUSE`），析构只清理**自己**创建的路径 |
| `bind_abstract(name[, backlog])` | 抽象命名空间（`@` 前缀），不依赖文件系统权限 |
| `accept()` | 协程；返回 `UnixStream`，失败为无效对象并设 `errno` |
| `UnixStream::connect(path)` | 协程；路径或 `@抽象名` |
| `read` / `write` / `close` | 与 `TcpStream` 同一返回约定：`>=0` 字节数、`0` EOF、`-1` 错误 |
| `unix_pair(a, b)` | `socketpair` 封装，两端立即可用 |

三条边界（都写在头文件注释里）：

- **关闭必须先 `shutdown` 再 `close`**：挂起的 io_uring `accept`/`recv` 持有 fd 引用，只
  `close` 不会让它完成，事件循环会永久等待（`TcpListener`/`UdpSocket` 都为此踩过）。因此
  `UnixListener::close()` / `UnixStream::close()` 都走两步；对应回归用例是
  `UnixTest.CloseWakesPendingAccept`。
- **路径过长直接判 `ENAMETOOLONG`**，不做静默截断——截断会连到另一个路径，比报错难查得多。
- 抽象名的 `sun_path` 长度按名字实际长度计算（不能把尾部 NUL 算进去，否则内核侧是另一个名字）。
- 取消语义与 `net.hpp` 一致：awaiter 提供 `static cancel_op(void*)` 即由 Task 的
  await_transform 自动注册为取消钩子，走 `ASYNC_CANCEL` 让原操作以 `-ECANCELED` 完成，
  保证 CQE 先于协程帧销毁被消费（不需要、也不允许手动给 `uring_op` 赋钩子字段）。

## 资源池 pool.hpp

> 只依赖核心（`task`/`event_loop`/`exceptions`），任何配置都可用；loop-local（与
> `channel`、`rate_limiter` 一致，不跨线程投递）。`#include <coro/pool.hpp>`。

面向"建立成本高、可复用、会坏"的资源（TCP/Unix 连接、TLS 会话、后端客户端）。工厂本身就是
协程，所以建连可以是真异步的：

```cpp
coro::Pool<coro::net::TcpStream>::options o;
o.max_agents = 8;   // 同时存在的资源上限 (0 = 不限)
o.max_idle   = 4;   // 空闲保留数, 超出即丢弃

coro::Pool<coro::net::TcpStream> pool(
    []() -> coro::Task<coro::net::TcpStream> {
        co_return co_await coro::net::TcpStream::connect("127.0.0.1", 8080);
    }, o);

auto lease = co_await pool.acquire();   // 复用空闲 → 额度内新建 → 否则排队
if (!lease.valid()) throw std::runtime_error("pool closed");
use(*lease);                            // 或 lease->write(...)
// 离开作用域自动归还; 连接已坏时 lease.discard() 让池少一个成员
```

| 接口 | 语义 |
| --- | --- |
| `acquire()` | 协程；优先复用空闲，其次在 `max_agents` 额度内新建，额度满则排队。池已关闭时返回**无效 lease** |
| `PoolLease`（`*` / `->` / `valid`） | 移动专属；析构即归还（RAII）；对无效 lease 解引用抛 `StructuredConcurrencyError` |
| `discard()` | 资源不可信时**不归还**并让出名额——忘记调用会让坏连接被下一个借用者拿到，这是连接池最典型的故障模式 |
| `close()` / `closed()` | 拒绝新的 `acquire`、丢弃空闲资源、唤醒排队者 |
| `idle_count()` / `in_use_count()` / `waiting_count()` | 观测用 |

四条合同（逐条有测试）：

- **归还靠 RAII**：异常路径与提前 `return` 都不会漏还。
- **池可以先于借出的句柄析构**：状态在 `shared_ptr<pool_state>` 里，lease 与等待者都持有
  引用（与 `task_registry`/`TcpServer` 同一范式）。
- **取消安全**：`acquire` 挂起时被取消，等待节点由 awaiter **析构**摘除（写在
  `await_resume` 里会因框架的取消检查包装器绕过它而失效），名额随之释放。
- **不超发**：同时在手的资源数不超过 `max_agents`；建连前先占名额，失败则归还名额并
  **原样上抛**工厂异常（不伪装成空句柄）。

范围：不做空闲 TTL 淘汰与健康探测——那需要资源侧"还能用吗"的回调，由使用者拿到 lease
后自行校验并 `discard()` 更诚实；也不做每键多池（按 endpoint 分池由调用方组合）。

`max_idle=0` 表示不保留空闲资源。工厂挂起期间池被关闭，`acquire()` 返回无效 lease；
工厂失败、取消或 acquire 帧直接销毁都会归还预留名额。移动后的 Pool 视为关闭。

## TLS tls.hpp

> **需要 `CORO_ENABLE_TLS=ON` 且仓库内存在 `thirdparty/openssl` 源码**（OpenSSL 3.5.8 LTS）。
> 关闭时 `tls.hpp` 整体编译为空：消费者既不需要 OpenSSL 头文件，也不会被链接 `libssl`。
> 链接 `coro::tls`；实现体在 `src/tls.cpp`，OpenSSL 头文件只在实现中出现。

```cpp
coro::tls::TlsContext ctx(coro::tls::TlsContext::role::client);
if (!ctx.load_system_trust()) throw std::runtime_error("无法配置默认信任路径");

auto sock = co_await coro::net::TcpStream::connect(ip, 443);
coro::tls::TlsStream tls(std::move(sock), ctx, "example.com");
if (!co_await tls.handshake()) throw coro::tls::TlsError(tls.last_error(), 0);

coro::stream_writer w(tls);            // TlsStream 满足 AsyncWritable
co_await w.write_all("GET / HTTP/1.0\r\nHost: example.com\r\n\r\n");
coro::stream_reader r(tls);            // 也满足 AsyncReadable
auto line = co_await r.read_line();
const bool clean = co_await tls.shutdown();   // 双向 close_notify
```

| 接口 | 语义 |
| --- | --- |
| `TlsContext(role)` | 持有 `SSL_CTX`；默认最低 TLS 1.2；客户端强制 `SSL_VERIFY_PEER` |
| `load_system_trust()` / `load_verify_file(path)` | 加载验证锚，**失败返回 false**（不静默降级为不校验） |
| `use_certificate_file` / `use_private_key_file` | 服务端证书与私钥（私钥会做 `check_private_key` 配对校验） |
| `set_min_version` / `set_alpn` / `set_hostname_verification` | 版本下限、ALPN（wire format 由实现拼装）、主机名校验开关 |
| `TlsStream::handshake()` | 协程；返回 `bool`，失败原因见 `last_error()`；可被取消 |
| `read` / `write` | 协程成员，返回 `Task<int>`，约定与 `TcpStream` 一致（`0` = 干净 EOF，`-1` = 错误） |
| `shutdown()` | 完成双向 `close_notify` 才返回 true；开始关闭后，无论结果如何都不可复用 |
| `peer_verified` / `negotiated_version` / `negotiated_alpn` | 握手结果观测 |

Context 配置在 Stream 构造时快照；Context 随后移动或析构不影响该会话。
同一 Stream 的异步操作必须串行，重叠操作抛 `logic_error`；操作期间不得移动或析构 Stream。
`set_verify_depth()` 会立即更新 Context；`set_alpn()` 校验协议长度，服务端按配置顺序选择共同协议。
客户端自动发送 DNS 主机名的 SNI，默认校验证书身份；空主机名不会隐式跳过验证。
`load_system_trust()` 返回 true 仅表示 OpenSSL 默认路径配置成功，验证链是否可信仍由握手判定。

`last_error_code()` 保存会话错误，流适配器优先读取它，避免其他协程覆盖线程 errno。
`reusable()` 仅说明 TLS 状态仍可传输；上层还必须确认完整消费响应、连接健康。
主动关闭、对端 close_notify、协议错误和操作取消后都不能复用。
超时由调用方用 `wait_for` 控制，本层没有隐藏 deadline。

三条设计要点（都在头注释里）：

1. **memory BIO 隧道**：`SSL_set_bio` 挂两个内存 BIO，`WANT_READ` 时从 socket `co_await read`、
   `WANT_WRITE` 时 `co_await write` 排空，全程不阻塞事件循环线程 —— 因此握手/读写天然可取消。
   （直接 `SSL_connect` 或用阻塞 BIO 会把整个 loop 卡住。）
2. **`read`/`write` 是协程成员而非手写 awaiter**：这样自动继承框架的取消注入与异常传播，
   不必重造挂起/摘链/取消兜底 —— 那是本项目反复出过错的地方。
3. **关闭后的 TLS 连接不得回连接池**：调用 `shutdown()` 后，无论返回 true/false 都应 `discard()`；
   实测踩过的坑是 `SSL_get_shutdown()` 的 `SSL_RECEIVED_SHUTDOWN` 位**只在库处理该记录后**才更新，
   刚 `BIO_write` 进去就查标志必然为空，会把已成功的交换误判为失败。

前置工具：OpenSSL 的构建脚本需要 **Perl**（CI 与本人都按外部前置处理）；NASM 缺失时统一
`no-asm`，保证不同机器编出同一套 C 实现。Windows 的 nmake/JOM 编排尚未实现，配置期显式
`FATAL_ERROR` 而不是静默产出坏库。

## HTTP 协议 http.hpp

> 始终可用，零第三方协程依赖。`HttpHeaders` / `HttpResponse` / `HttpError` 定义在
> `<coro/http.hpp>`，同时被 HTTP 客户端与 Web 层使用。

```cpp
coro::HttpHeaders h = {{"Content-Type", "application/json"}};
coro::HttpResponse resp;
resp.status = 200;
resp.headers = h;
resp.body = R"({"ok":true})";
auto ct = resp.header("content-type");   // 大小写不敏感查找 → "application/json"
```

| 类型 | 语义 |
|---|---|
| `HttpHeaders` | `vector<pair<string,string>>`；保留顺序与重复字段 |
| `HttpResponse` | `status` / `version` / `headers` / `trailers` / `body` / `keep_alive` |
| `HttpResponse::header(name)` | 大小写不敏感首次匹配，返回 `string_view`，未找到返回空 |
| `HttpError` | `runtime_error` 子类；连接失败、协议错误、超时均抛此异常 |

## HTTP 客户端 http_client.hpp

> **需要 `CORO_ENABLE_HTTP_CLIENT=ON`**。链接 `coro::http_client`（依赖 `coro::io`；
> https  additionally 依赖 `coro::tls`，TLS 未启用时 `supports_tls()` 返回 false）。
> 实现体在 `src/http_client.cpp`，公共头不泄漏 OpenSSL 类型。

```cpp
coro::HttpClient::Options opts;
opts.request_timeout = std::chrono::milliseconds{5000};
opts.max_connections_per_origin = 16;
coro::HttpClient client(opts);

// 简单 GET
auto resp = co_await client.get("https://example.com/api");
if (resp.status == 200) { /* resp.body */ }

// 自定义请求
coro::HttpRequest req;
req.method = "POST";
req.url = "https://api.example.com/submit";
req.headers = {{"Content-Type", "application/json"}};
req.body = R"({"key":"value"})";
auto resp2 = co_await client.request(std::move(req));

client.close();   // 拒绝新请求，释放空闲连接
```

| 接口 | 语义 |
|---|---|
| `HttpClient()` / `HttpClient(Options)` | 构造客户端；Options 控制超时、连接池上限、请求体上限等 |
| `get(url) → Task<HttpResponse>` | 快捷 GET |
| `request(HttpRequest) → Task<HttpResponse>` | 通用请求；方法/URL/头部/正文/keep-alive 均可控 |
| `close()` | 拒绝新工作，唤醒连接池等待者，丢弃空闲连接；在途请求按各自 deadline 完成 |
| `closed()` | 是否已关闭 |
| `supports_tls()` | 静态方法；构建时是否启用了 TLS（https:// 能力） |

**连接池隔离**：按 scheme + host + port + TLS 配置隔离。不自动重试、不跟随重定向、
不做代理/解压缩/协议升级。`max_idle_per_origin=0` 禁用连接缓存（每次请求新建连接）。

**超时模型**：`request_timeout` 覆盖全链路（排队 + DNS + 连接 + 正文），0 表示不限时。
超时抛 `HttpError`，连接可能被 discard 而非回池。

**安全限制**：`max_body_bytes`（默认 8MB）、`max_header_bytes`（64KB）、
`max_header_count`（100）、`max_response_wire_bytes`（16MB）防止对端喂垃圾耗尽内存。

## TCP/UDP 服务与关闭结果

链接 `coro::tcp_udp`，包含 `<tcp_udp/tcp_server.hpp>` 或 `<tcp_udp/udp_server.hpp>`。
关闭结果是 `<coro/shutdown.hpp>` 中的 `coro::ShutdownReport`：

```cpp
struct ShutdownReport {
    bool drained;
    std::size_t unfinished;
};

auto result = co_await server.shutdown();       // 使用 Config 中的宽限期
auto result2 = co_await server.shutdown(1s);    // 覆盖排空时间
auto result3 = co_await server.shutdown(1s, 500ms);
```

未启动的服务关闭时直接返回 `{true, 0}`。`stop_request()` 使用已配置的宽限期；
`stop()` 跳过排空并请求取消。入口 I/O 的取消完成后 `shutdown()` 才返回，
`unfinished` 表示仍未结束的 handler；还有任务时 `start()` 返回 false，也不能销毁它们借用的资源。
`last_shutdown_report()` 保留最近一次关闭时的快照。

服务的配置、启动、停止在所属事件循环线程执行；跨线程使用 `loop.dispatch()`。
`set_handler()` 的处理器在 `start()` 时复制，之后修改只影响下一次启动。
Server 必须存活到 `shutdown()` 返回；UDP handler 若借用 `socket()`，还须等待全部
handler 完成。负的关闭宽限期与不在 1–65535 范围内的 UDP 接收缓冲大小会被拒绝。
`UdpServer::pending_bytes()` 在无限额模式下也统计真实的在途数据量。

## 并发组合 gather.hpp / wait.hpp

### 选择决策

| 场景 | 首选 API | 关键合同 |
|---|---|---|
| 编译期固定、可混合结果类型 | `gather` | 全部完成；结果按参数顺序返回 tuple |
| 运行期动态、同一结果类型 | `wait_tasks` | 通过 `WaitMode` 选择 FirstCompleted / FirstException / AllCompleted |
| 失败即取消其余子任务 | `TaskGroup` | 结构化收尾；失败抛 `ExceptionGroup` |
| 多个 channel 收发操作竞速 | `select` | channel 专用；落选分支不消费数据 |

`wait_any`、`gather_all`、`gather_void` 是保留的源兼容入口，均标记为 deprecated；
新代码应分别使用 `wait_tasks(..., FirstCompleted)`、`wait_tasks(..., AllCompleted)`、
`gather(...)`（忽略 tuple）。

### gather — 静态 N 路（编译期数量）

```cpp
namespace coro {
    template <typename... Ts>
    /*awaiter*/ gather(Task<Ts>... tasks);        // → std::tuple<Ts...>
}
```

- 所有任务在 gather 点**一起启动**；总耗时 ≈ 最慢者。
- 结果按参数顺序放入 tuple（结构化绑定友好）；`Task<void>` 可参与
  （对应位置仅等待，无值）。
- 异常：第一个异常被记录，**其余任务继续跑完**，全部结束后重抛
  第一个异常（对标 `asyncio.gather` 默认）。
- 不支持 `Task<void>` 放 tuple 取值场景？——void 可以放进参数列表，
  只是该位置无返回值；需要"取值"就用非 void 任务。

### 兼容组合器：gather_all / gather_void（deprecated）

```cpp
namespace coro {
    template <typename T>
    [[deprecated]] Task<std::vector<T>> gather_all(std::vector<Task<T>> tasks);

    template <typename... Ts>
    [[deprecated]] Task<void> gather_void(Task<Ts>... tasks);
}
```

- `gather_all` 迁移到
  `wait_tasks(std::move(tasks), WaitMode::AllCompleted)`；两者都是动态同构任务、
  全部完成后按原顺序返回结果，异常在全部结束后传播。
- `gather_void` 迁移到 `gather(task1, task2, ...)` 并忽略它的 tuple；`gather`
  已支持 `Task<void>`（对应位置不提供值）。

### wait_for — 超时

```cpp
namespace coro {
    template <typename T, typename Rep, typename Period>
    Task<T> wait_for(Task<T> task, std::chrono::duration<Rep, Period> timeout);
    Task<void> wait_for(Task<void> task, std::chrono::duration<Rep, Period> timeout);
}
```

- 超时 → 任务被自动 `cancel()`，等待者收到 `TimeoutError`；
  提前完成 → 定时器撤销，零残留。

### wait_any — 兼容两路竞速（deprecated）

```cpp
namespace coro {
    template <typename T>
    [[deprecated]] Task<T> wait_any(Task<T> a, Task<T> b);
}
```

迁移到 `wait_tasks(std::move(tasks), WaitMode::FirstCompleted)`。先完成者
（成败均可）胜出；落选者**继续后台运行**不被取消——需要失败自动取消时改用
`TaskGroup`。

### wait_tasks — N 路等待（对标 asyncio.wait）

```cpp
namespace coro {
    enum class WaitMode { FirstCompleted, FirstException, AllCompleted };

    template <typename T>
    Task<std::vector<T>> wait_tasks(std::vector<Task<T>> tasks,
                                    WaitMode mode = WaitMode::AllCompleted);
}
```

| 模式 | 返回 | 失败行为 |
|---|---|---|
| `FirstCompleted` | 只含第一个完成者 | 第一个完成者失败 → 抛其异常 |
| `FirstException` | 全部结果 | 任一失败**立即抛**，其余后台继续 |
| `AllCompleted` | 全部结果 | 全部完成后抛第一个异常 |

任何模式都**不取消**落选任务。

---

## TaskGroup task_group.hpp

```cpp
namespace coro {
    class TaskGroup {
    public:
        TaskGroup();
        ~TaskGroup();                                   // 兜底取消未完成子任务
        template <typename T> void spawn(Task<T> task); // 立即启动; 任意 T
        /*awaiter*/ wait();                             // → co_await 收尾
        size_t pending_count() const noexcept;
        bool done() const noexcept;
    };
}
```

- **失败自动取消**：任一子任务真实失败 → 自动取消其余 → 全部结束后
  在 `co_await group.wait()` 处抛 `ExceptionGroup`（单个也打包；
  组内取消引发的 `CancelledError` 不聚合）。
- **只能 wait 一次**；空组 wait 立即通过。
- 子任务的结果值不能从组取——用引用参数/共享状态带回
  （同线程协作语义下安全）。
- 析构兜底：忘了 wait 也会取消残留子任务，孤儿无法逃出作用域。

---

## 同步原语 sync.hpp

全部为**事件循环内**使用（无内部线程锁）；等待队列 FIFO；
被取消协程的等待记录自动摘除（僵尸清理）。

### Lock

```cpp
class coro::Lock {
public:
    Lock& acquire();                       // co_await lock.acquire()
    void release();                        // 未持锁时调用 = no-op
    /*awaiter*/ guard();                   // → RAII Guard (析构自动 release)
    bool is_locked() const noexcept;
};
```

FIFO 公平、支持递归获取（重复 acquire 需要对应次数 release）；释放时直接移交队首等待者。

### Semaphore

```cpp
class coro::Semaphore {
public:
    explicit Semaphore(int permits);
    Semaphore& acquire();                  // co_await sem.acquire()
    void release();
    /*awaiter*/ guard();                   // RAII
    int available() const noexcept;
};
```

### Event

```cpp
class coro::Event {
public:
    Event& wait();                         // co_await ev.wait()
    void set();                            // 粘性: 唤醒全部等待者, 之后 wait 直接过
    void clear();
    bool is_set() const noexcept;
};
```

### Condition

```cpp
class coro::Condition {
public:
    explicit Condition(Lock* lock);        // 显式关联锁
    Task<> wait();                         // 须持锁调用; 返回时重新持锁
    void notify(size_t n = 1);
    void notify_all();
    size_t waiter_count() const noexcept;
    Lock* lock() const noexcept;
};
```

标准用法：`while (!cond) co_await cv.wait();`（持锁 + while 防虚假唤醒）。
取消安全：被取消时先重新拿锁再传播异常，保证锁恰好释放一次。

---

## Queue queue.hpp

```cpp
template <typename T> class coro::Queue {
public:
    explicit Queue(size_t maxsize = 0);    // 0 = 无界

    /*awaiter*/ put(T item);               // 满则挂起
    /*awaiter*/ get();                     // 空则挂起, 返回 T
    std::optional<T> get_nowait();         // 空 → nullopt
    bool put_nowait(T item);               // 满 → false

    void task_done();                      // 记一个已取元素处理完
    /*awaiter*/ join();                    // unfinished 归零前挂起

    size_t size() const noexcept;
    bool empty() const noexcept;
    bool full() const noexcept;
    size_t unfinished_count() const noexcept;
};
```

对标 `asyncio.Queue` 含 `task_done`/`join` 收尾协议
（unfinished 归零时的重复 `task_done` 被静默忽略，Python 抛 ValueError）。

---

## Promise / Future future.hpp

```cpp
template <typename T> class coro::Promise {
public:
    Future<T> get_future();               // 可多次调用 → 多 Future 共享状态
    void set_value(T value);              // 重复 set → std::logic_error
    void set_exception(std::exception_ptr);
    bool is_done() const noexcept;
};
template <> class coro::Promise<void> { void set_value(); /* ... */ };

template <typename T> class coro::Future {
public:
    bool valid() const noexcept;          // 空 Future false
    bool await_ready() const noexcept;    // 已 set → 不挂起
    T await_resume();                     // 异常重抛
};
```

- `set_value` / `set_exception` **线程安全**（任意线程可调），
  唤醒自动路由到各等待者所在的 loop；
- **多等待者**：全部被唤醒；
- 用途：桥接回调 API、跨线程一次性交付。

---

## to_thread thread.hpp

```cpp
namespace coro {
    template <typename F>
    Task<std::invoke_result_t<F&>> to_thread(F func);
}
```

- `func` 在进程级线程池（默认 `hardware_concurrency` 线程）执行；
- 支持仅可移动的闭包，例如捕获 `unique_ptr`；函数按左值调用；
- 返回值/异常跨线程传回 `co_await` 处；
- 纪律：`func` 内**不得**触碰事件循环与协程对象；
  与协程世界通信请用 `Promise::set_value`。

---

## 定时回调 schedule.hpp

```cpp
namespace coro {
    template <typename F> void call_soon(F&& func);
    template <typename Rep, typename Period, typename F>
    void call_later(std::chrono::duration<Rep, Period> delay, F&& func);
    template <typename F>
    void call_at(std::chrono::steady_clock::time_point deadline, F&& func);
}
```

- `func`：普通可调用物，**或**返回 `Task<>` 的协程工厂
  （到点启动并等待完成）；
- 在当前事件循环线程执行；生命周期全自动（内部协程自持有），
  无需保存任何返回值。

---

## Scheduler scheduler.hpp

```cpp
namespace coro {
    class Scheduler {
    public:
        explicit Scheduler(size_t workers = std::thread::hardware_concurrency()); // 0 → 1
        ~Scheduler();                                        // stop + join 全部 worker

        template <typename F> void spawn_any(F factory);     // 工厂: 返回 Task<T> 的可调用物
        void wait_all();                                     // 阻塞直到全部完成
        size_t worker_count() const noexcept;
        EventLoop* loop_at(size_t i) const;                  // 高级: 拿 worker 的 loop
    };
}
```

- N 个常驻 worker 线程，每线程一个事件循环；
- `spawn_any` 按"活跃协程最少（主）/累计分发数（辅）"选 worker，
  把**工厂函数** dispatch 到该线程——协程帧在 worker 线程创建/销毁
  （**必须传工厂**，不能传已创建的 Task）；
- 协程亲和：任务绑定 worker 不迁移；worker 内单线程语义；
- 负载均衡单次扫描 O(workers)；`wait_all` 自适应退避轮询
  （50µs 起翻倍至 1ms 封顶）。

---

## 事件循环 event_loop.hpp

```cpp
namespace coro {
    class EventLoop {
    public:
        static EventLoop& get();                    // 当前线程的 loop (惰性创建)
        void run();                                 // 驱动到无工作 (禁止嵌套)
        void run_until_stopped();                   // 常驻模式 (Scheduler 用)
        void stop();                                // 线程安全
        void wake();                                // 线程安全: 唤醒循环

        void schedule(std::coroutine_handle<> h);   // 线程安全; 就绪队列 (幂等去重)
        void dispatch(std::function<void()> fn);    // 线程安全; 投递普通函数到本 loop

        size_t active_task_count() const;           // 调试/监控
        bool has_active_coroutines() const;
        static std::coroutine_handle<> current_task();  // 协程内非空
    };
}
```

- **每线程一个实例**（thread_local 惰性单例）；
- `run()` 退出条件：就绪队列、定时器堆、挂起 IO、活跃协程全部为空；
- 构造时按平台自动选择事件源：Windows → IOCP，Linux → io_uring，
  其他 → condition_variable 回退（`set_event_source` 可替换）。

调试辅助（编译时定义 `CORO_TASK_REGISTRY` 启用任务注册表）：

```cpp
size_t n = coro::EventLoop::get().active_task_count();
auto cur = coro::EventLoop::current_task();   // std::coroutine_handle<>, 协程内非空
```

---

## 错误模型 io.hpp

IO 五件套（net/fs/pipe/process/fs_watch/signal）统一的错误约定：

```cpp
namespace coro::io {
    int last_error() noexcept;      // thread_local; 最近一次 IO 的平台原生错误码
    void clear_error() noexcept;
}
```

| 返回值形态 | 含义 |
|---|---|
| `int >= 0`（read/write 类） | 实际字节数 |
| `int == 0`（read 类） | 对端关闭 / EOF（不是错误） |
| `int == -1` | 失败：`errno` 已**转换为标准 errno**（`strerror` 可直接用），平台原生码在 `io::last_error()` |
| 抛 `CancelledError` | 挂起中的 I/O 被取消（任务 `cancel()` 或 `wait_for` 超时联动）——**绝不**以返回 0 表示，否则与 EOF 无法区分 |
| 抛 `TimeoutError` | 带 deadline 的操作（如 `Context::wait()`）到点而未完成 |
| `bool == false` | 失败（open 类） |
| `valid() == false` | 失败（对象类：TcpStream/File/Process/Watcher） |

errno 转换示例：Windows `ERROR_OPERATION_ABORTED`（取消）→ `EINTR`；
`ERROR_BROKEN_PIPE` → EOF（0）；`WSAECONNRESET` → `ECONNRESET`。
需要精确平台码（如 WSA 10054）时用 `io::last_error()`，
**co_await 返回后立即读**（thread_local，同线程后续 IO 会覆盖）。

---

## TCP 网络 net.hpp

```cpp
namespace coro::net {
    class TcpStream {                       // 可移动; 析构自动关闭
    public:
        static /*awaiter*/ connect(const char* ip, unsigned short port);
        /*awaiter*/ read(char* buf, size_t len);       // → int (错误模型见上)
        /*awaiter*/ write(const char* buf, size_t len);// → int
        void close();                        // 挂起操作以取消完成收尾
        bool valid() const;
        void reattach();                     // [WIN32] 关联到当前线程 loop 的 IOCP
    };

    class TcpListener {
    public:
        bool bind_listen(const char* ip, unsigned short port);   // 同步一次性
        /*awaiter*/ accept();                // → TcpStream (失败 valid()==false)
        /*awaiter*/ accept_noattach();       // 同上, 但不关联 IOCP (跨线程场景)
        void close();
    };
}
```

- `connect` 失败：socket 已在内部关闭，`valid() == false`；
- `read` 返回 0 = 对端正常关闭；-1 = 错误（伴 `io::last_error()`）；挂起中被取消则抛
  `CancelledError`——三种情况互相可区分（`NetTest.ReadAfterLocalCloseIsErrorNotEof` 与
  `NetTest.CancelledReadIsNotReportedAsEof` 锁住该合同）；
- Windows 上 socket 构造即关联创建线程的 IOCP；要把连接交给
  其他线程（Scheduler worker）处理：`accept_noattach()` + worker 内
  `reattach()`（Linux io_uring 无此约束，reattach 为空操作）；
- 挂起的 read/write/connect/accept 均可被 `Task::cancel()` 取消；
- **协议无消息边界**（TCP 字节流），拆包是应用层责任；
- 挂起期间 awaiter（协程帧内）与流对象必须存活。

---

## 文件 fs.hpp

```cpp
namespace coro::fs {
    enum class mode : unsigned {
        read = 1, write = 2, create = 4, truncate = 8, append = 16, exclusive = 32
    };
    constexpr mode operator|(mode, mode);
    constexpr bool  operator&(mode, mode);

    struct stat_info { uint64_t size; int64_t mtime_sec; bool is_dir; bool exists; };

    class File {                            // 可移动; 析构关闭
    public:
        bool valid() const;
        /*awaiter*/ read_at(char* buf, size_t len, uint64_t offset);   // → int; 0=EOF
        /*awaiter*/ write_at(const char* buf, size_t len, uint64_t offset);
        Task<bool> fsync();
        void close();
        HANDLE native() const;              // [WIN32] / int native() [Linux]
    };

    Task<File> open(std::string_view path, mode m);   // 失败 valid()==false
    stat_info  stat(std::string_view path);           // 同步; 不存在 → exists=false
    Task<std::string> read_all(std::string_view path);// 失败 → 空串(且 last_error()!=0)
    Task<bool> write_all(std::string_view path, std::string_view data,
                         mode m = mode::write);
}
```

- **定位读写**：`read_at`/`write_at` 显式 offset、不共享游标 →
  同一文件可被多协程并发分块读写；
- `append` 模式：`write_at` 的 offset 被忽略（OS 原子追加）；
- `exclusive | create`：原子创建（已存在则失败）；
- `stat` 恒同步（元数据操作，微秒级）；Windows 的 `open` 同步、
  Linux 全异步（io_uring OPENAT）；
- 挂起的读写（Linux 含 open）可被 `Task::cancel()` 取消；
- `read_all` 空文件与失败都返回空串：区分靠
  `coro::io::last_error() == 0`。

> 目录监视（`fs::watch` / `DirectoryWatcher`）与文件 IO 同属 `coro::fs`
> 命名空间，但实现独立在 `fs_watch.hpp`，见
> [目录监视](#目录监视-fs_watchhpp) 一节。

---

## 管道 pipe.hpp

```cpp
namespace coro::pipe {
    class PipeEnd {                         // 单向管道的一端; 可移动
    public:
        bool valid() const;
        /*awaiter*/ read(char* buf, size_t len);        // → int; 0=写端关闭
        /*awaiter*/ write(const char* buf, size_t len); // → int; 对端关闭 → -1/EPIPE
        void close();                       // 读端关 → 对端写报错; 写端关 → 对端读 0
        HANDLE native() const;              // [WIN32] / int native() [Linux]
    };

    std::pair<PipeEnd, PipeEnd> pair(size_t buffer = 64 * 1024);  // {读端, 写端}
}

namespace coro::io {                        // [仅 Linux]
    /*awaiter*/ poll(int fd, short events); // → uint32_t revents
}
```

- 满写/空读自动挂起 = 天然背压；`buffer` 参数 Windows 生效
  （命名管道缓冲），Linux 内核自管（忽略）；
- Windows 实现为命名管道对（匿名管道不支持 OVERLAPPED）；
  Linux 为 `pipe2(O_NONBLOCK)` + io_uring；
- `io::poll` 单次触发即返回（POLLIN/POLLOUT/POLLERR/POLLHUP），
  Windows 无对应 API。

---

## 信号 signal.hpp

```cpp
namespace coro::signal {
    bool allow(int sig);                   // 注册自定义信号 (进程级, 幂等);
                                           // 不可注册编号 → false
    bool disallow(int sig);                // 注销并恢复 SIG_DFL; 有等待者 → false
    void notify(int sig);                  // 库内投递, 效果等同信号到达 (跨平台)

    /*awaiter*/ wait(int sig);              // → int 信号编号; 未注册 → std::invalid_argument
                                            // 多协程可同时等同一信号

    class handler {                         // RAII 注册对象; 可移动不可拷贝
    public:
        void stop();                        // 幂等注销
        ~handler();                         // 自动注销
    };

    handler handle(int sig, std::function<Task<>()> factory);
                                            // 每次信号到达执行 factory(); 串行不重入
}
```

- 默认白名单（无需 allow）：Linux `{SIGINT, SIGTERM, SIGHUP}`，
  Windows `{SIGINT, SIGTERM, SIGBREAK, SIGHUP}`（自定义 SIGHUP=3 映射
  "关窗"事件）；
- 自定义信号先 `allow(sig)` 再 `wait`/`handle`：
  - Linux 接受 1..31（SIGKILL/SIGSTOP 除外）与 `SIGRTMIN..SIGRTMAX`，
    注册后 `raise()`/`kill()` 均可到达；32/33 是 glibc 线程库保留号被拒绝；
  - Windows 接受任意 1..63 编号；CRT 不认识的编号（如 42）只能经
    `notify()` 投递（`raise()` 不可达）；
- `disallow` 存在等待者时拒绝；注销后 Linux 恢复 `SIG_DFL`；
- Windows 无真信号：控制台事件（Ctrl+C / Ctrl+Break / 关窗）与 CRT
  `raise()` 双路桥接；SIGHUP 无法经 `raise()` 触发；
- Linux 用 `sigaction + eventfd + reader 线程`；信号处理器只做
  async-signal-safe 的 `write`，再由 reader 把通知路由到各等待者的 loop；
- `handle` 的 factory 在**注册时的 loop** 上执行，串行不重入
  （上一个协程完成前新信号排队）。

---

## 目录监视 fs_watch.hpp

```cpp
namespace coro::fs {
    enum class watch_event_type { created, removed, modified, renamed, overflow };

    struct watch_event {
        watch_event_type type;
        std::string path;                   // 相对监视目录, UTF-8, '/' 分隔
        std::string old_path;               // 仅 renamed
        bool is_dir;                        // Linux 提供; Windows 恒 false
    };

    class DirectoryWatcher {
    public:
        bool valid() const;
        Task<watch_event> next();           // 挂起到下一个事件 (内部有缓冲队列)
        void close();                       // 有挂起 next() 时不可调用
    };

    Task<DirectoryWatcher> watch(std::string_view path, bool recursive = true);
}
```

- 事件粒度由内核决定：编辑器一次保存常产生多条事件，
  应用层需去抖（如同路径 100ms 窗口）；
- `overflow` = 内核缓冲溢出，可能丢事件（建议全量扫描兜底）；
- Windows：`ReadDirectoryChangesW` 原生递归；
  Linux：inotify 通过 wd 到相对路径的映射实现递归，并动态跟踪新目录。

---

## 子进程 process.hpp

```cpp
namespace coro::process {
    struct options {
        bool capture_stdin = false;
        bool capture_stdout = false;
        bool capture_stderr = false;
    };

    class Process {                         // 可移动; 析构不杀不等待
    public:
        bool valid() const;
        int  pid() const;
        pipe::PipeEnd* stdin_pipe();        // 未 capture → nullptr; 用完应 close
        pipe::PipeEnd* stdout_pipe();
        pipe::PipeEnd* stderr_pipe();
        Task<int> wait();                   // 退出码; 多协程可同时 wait
        void terminate();                   // SIGTERM / TerminateProcess(1)
        void kill();                        // [Linux] SIGKILL; [WIN32] 同 terminate
    };

    Task<Process> spawn(std::vector<std::string> args, options opt = {});
                                            // args[0] = 程序 (PATH 可解析)
    Task<std::pair<int, std::string>> run_capture(std::vector<std::string> args);
                                            // 跑到退出; {退出码, stdout}; stderr 直通
}
```

- **析构语义**：Process 析构不杀进程也不等待——结局必须显式
  `wait()`（自然收尾）或 `terminate()`（强杀）；
- 公开入口是自由函数 `spawn()`；`Process::create()` 是它背后的
  静态实现工厂（要访问私有字段填充状态），正常代码不要直接调；
- 退出通知经跨线程 Promise 路由（等待者在哪个 loop 都能等）；
- 子进程 stdio 是异步管道（`pipe::PipeEnd`），写入 EOF 需主动
  `stdin_pipe()->close()`；
- 被信号杀死的退出码约定为 `128 + 信号号`（Linux）；
- Windows：stdio 用命名管道、句柄继承启动；Linux：fork + execvp +
  专用收割线程 waitpid（不碰全局信号掩码）。

---

## 头文件 include 速查

```
coro.hpp ── 核心 + 并发 (task/sleep/gather/wait/task_group/sync/queue/
            future/thread/schedule/scheduler/event_loop/exceptions)
net.hpp      ── 单独 include
fs.hpp       ── 单独 include (fs_watch.hpp 亦在 fs 命名空间, 单独 include)
pipe.hpp     ── 单独 include
signal.hpp   ── 单独 include
process.hpp  ── 单独 include (内部已带 pipe/io)
io.hpp       ── 错误模型; 已被上述 IO 头间接包含, 通常无需直接 include
```

注意：`sync.hpp` / `queue.hpp` 单独 include 时隐式依赖 `task.hpp`
（经由 `coro.hpp` 使用则无感知）。
