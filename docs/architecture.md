# coro 架构与源码剖析

> 面向两类读者：想**给库贡献代码 / 改造库**的人，和想通过一个真实
> 项目学习"**如何设计 C++20 协程框架**"的人。
> 使用层面的问题请看 [API 参考](api-reference.md) 与 [教程](tutorial/README.md)；
> 语言机制请看 [C++20 协程课程](cpp20-coroutines-course/README.md)。
>
> 库本体：`coro/include/coro/` 下 24 个头文件，共约 8900 行，header-only。
> 行文自底向上：事件源 → 事件循环 → Task → 取消 → 并发组合 → IO 层 →
> 多线程模型 → 横切设计模式。

## 目录

1. [全景图](#1-全景图)
2. [事件源抽象 EventSource 与三大实现](#2-事件源抽象-eventsource-与三大实现)
3. [事件循环 EventLoop](#3-事件循环-eventloop)
4. [Task 的 promise_type 设计](#4-task-的-promise_type-设计)
5. [取消机制：CancelledError 注入的完整链条](#5-取消机制)
6. [并发组合器：monitor 协程模式](#6-并发组合器monitor-协程模式)
7. [同步原语与 Future 的实现要点](#7-同步原语与-future-的实现要点)
8. [IO 层：Proactor 统一完成路径](#8-io-层proactor-统一完成路径)
9. [多线程模型：线程亲缘与跨线程路由](#9-多线程模型线程亲缘与跨线程路由)
10. [include 依赖图](#10-include-依赖图)
11. [八大横切设计模式](#11-八大横切设计模式)
12. [代码规模与阅读顺序](#12-代码规模与阅读顺序)

---

## 1. 全景图

coro 是一个 **Proactor 模型**的单线程协作式调度框架（每线程一个实例），
对标 Python asyncio。所有组件最终都落到两个原语上：

```
                      ┌────────────────────────────────────────────┐
                      │        EventLoop (每线程一个)               │
                      │   ready_queue_ (FIFO, 有锁, 幂等去重)        │
                      │   timer_heap_ (min-heap, 无锁, 仅loop线程)   │
                      │   fn_queue_   (dispatch 的普通函数)          │
                      │   active_coroutines_ (atomic 退出条件)       │
                      └───────────────┬────────────────────────────┘
                                      │ 委托睡眠/唤醒
                              EventSource (抽象)
                              ▲        ▲         ▲
                 IocpEventSource  UringEventSource  CVEventSource
                    [Windows]         [Linux]        [其他平台]
                 IOCP 完成包        io_uring CQE     condvar 模拟

  所有 awaiter 的挂起 = EventLoop::schedule(h) 或 schedule_timer(h, deadline)
  所有 IO 的完成      = EventSource::on_complete(h) → schedule(h)
```

一次 `co_await coro::sleep(1s)` 的完整旅程：

```
用户协程 H: co_await sleep(1s)
  → sleep_awaiter::await_suspend(H)
      → EventLoop::get().schedule_timer(H, now+1s, token)   [进定时器堆]
  → 控制权回到 run() 主循环
  → (无其他工作) 入睡协议 → wait_for(到堆顶 deadline)
  → 1s 后醒来: process_timers() 把 H 弹入就绪队列
  → 批量 resume: H 从挂起点继续执行
```

---

## 2. 事件源抽象 EventSource 与三大实现

`event_source.hpp`（120 行）定义了"事件循环如何睡眠与被唤醒"的抽象，
模仿 Python 的 selectors 模块：

```cpp
class EventSource {
public:
    virtual int  wait_for(std::chrono::milliseconds timeout) = 0;
    virtual void wake() = 0;                 // 线程安全, 任意线程
    virtual bool has_pending() const;        // 有挂起 I/O 吗 (决定 loop 能否退出)
protected:
    void on_complete(std::coroutine_handle<> h);   // 完成回调 → schedule
};
```

`wait_for` 返回值约定：`0` = 超时（loop 去处理定时器），`>0` =
有就绪事件（I/O 完成或被唤醒）。**调用方把 wake 当"睡醒了重新检查
所有状态"**，而不是精确事件——这个宽松契约让三个实现都极其简单。

完成回调通过 `set_completion_handler(ctx, fn)` 注入（loop 安装事件源时
传入 `[](ctx, h){ static_cast<EventLoop*>(ctx)->schedule(h); }`）。
历史上这里是全局函数指针，在多线程同时构造 loop 时存在数据竞争，
改为事件源成员后每 loop 自持。

### IocpEventSource（Windows，155 行）

- 构造：`CreateIoCompletionPort(INVALID_HANDLE_VALUE, ...)` 创建完成端口；
  socket/文件/管道/目录句柄经 `associate(SOCKET/HANDLE)` 挂进来；
- 每个异步操作有一个 `detail::iocp_op`：

  ```cpp
  struct iocp_op {
      OVERLAPPED ov;                        // 必须是第一个成员:
                                            // IOCP 用 &ov 反查整个结构
      std::coroutine_handle<> continuation;
      int error;                            // WSA/Win32 原生码
      DWORD transferred;
  };
  ```

- `wait_for`：阻塞 `GetQueuedCompletionStatus(timeout)`；拿到一个完成包后
  用 **0 超时循环再排空至多 64 个**（单次 loop 迭代收割一批，摊薄开销）。
  刻意不用 `GetQueuedCompletionStatusEx`：后者对失败完成只给 NTSTATUS
  需要手工转换，而 GQCS 的 FALSE + GetLastError 直接就是正确的 WSA 码；
- `wake`：`PostQueuedCompletionStatus(WAKE_KEY)` 投递假完成键；
- 三种 I/O 返回形态统一处理：`WSA_IO_PENDING`（真异步挂起）、
  同步成功（rc==0，**IOCP 仍会投递完成包**）、立即失败（无完成包，
  awaiter 自己 `schedule(h)` 恢复）。

### UringEventSource（Linux，150 行）

- 构造：`io_uring_queue_init(256, &ring_, 0)`；
- 操作状态 `uring_op { continuation, result, error }`，用 SQE 的
  `user_data` 携带 `uring_op*`（对应 IOCP 的 OVERLAPPED 反查）；
- 无需预关联 fd——每个操作直接提交 SQE（`io_uring_prep_recv/send/
  connect/accept/open/read/write/fsync/poll/cancel`）；
- 同步/异步完成**一定有 CQE**（没有 IOCP"同步成功不投递"的歧义）；
- `wake`：提交一个 `data=nullptr` 的 NOP SQE 作唤醒标记。

### CVEventSource（纯标准库回退，其他平台）

mutex + condition_variable + bool 模拟 self-pipe；`has_pending` 恒 false
（无真异步 IO，loop 不会因挂起 I/O 而驻留）。三大平台的
`std::condition_variable` 分别落到 `SleepConditionVariableCS` / futex /
`pthread_cond`，因此即便是回退实现也够用。

---

## 3. 事件循环 EventLoop

`event_loop.hpp`（618 行）是全库的心脏。核心数据结构：

| 成员 | 保护 | 用途 |
|---|---|---|
| `ready_queue_` | `queue_mutex_` | 就绪协程 FIFO（`HandleQueue`：vector + 头索引，见下） |
| `scheduled_set_` | 同上 | `unordered_set<const void*>`：schedule **幂等去重**——同一句柄不会重复入队（防 double-resume UB） |
| `timer_heap_` | 无锁（仅 loop 线程） | min-heap 定时器；每条带 `shared_ptr<atomic<bool>> token` 作"已消费/僵尸"标志 |
| `fn_queue_` | 同 ready 锁 | `dispatch()` 投递的普通函数（跨线程） |
| `awake_` | atomic seq_cst | 入睡协议标志（见下） |
| `active_coroutines_` | atomic | 退出条件之一：协程可能挂在跨线程事件上，计数>0 时 loop 必须无限等待 |
| `loop_thread_id_` | — | 区分同线程调度（不 wake）与跨线程调度 |
| `t_current_task` / `t_current_loop` | thread_local | `current_task()` 与 per-thread loop 的实现基础 |

### 3.1 run() 主循环逐步讲解

```cpp
void run_impl(bool stay)  // stay=true 即 run_until_stopped (Scheduler worker)
{
    if (running_) return;                    // 禁止嵌套
    running_ = true; awake_ = true;
    bind(this); loop_thread_id_ = ThisThread;

    while (running_) {
        if (!stay && !has_work()) break;     // has_work = 队列||定时器||挂起IO||活跃协程

        // 第 0 步: 执行 dispatch 的普通函数
        //   先复位 has_pending_fn_ 再锁内排空 → 复位后新到的 dispatch
        //   会重新置位, 不会漏 (反序会覆盖并发置位)
        ...

        // 第 1 步: 定时器
        now = process_timers();
        //   惰性清理: 堆顶 token 已置位(僵尸) → pop
        //   到期条目跳过 handle.done(), 批量经 scheduled_set_ 去重后入就绪队列
        //   (10 万定时器同到期只加一次锁)

        // 第 2 步: 入睡协议 (防丢唤醒)
        awake_.store(false, seq_cst);
        锁内复查 ready_queue_ && fn_queue_;
        ├─ 非空      → awake_=true; 跳过等待      // 复查覆盖"声明后到达"的入队
        ├─ 有定时器   → wait_for(距堆顶, 向上取整) // 取整防 <1ms 截断成忙转
        ├─ stay||挂起IO||活跃协程>0
        │            → wait_for(INFINITE)          // I/O完成/wake/stop 打断
        └─ 全空      → awake_=true; break          // 正常退出

        // 第 3 步: 批量恢复
        // batch 是成员 (HandleQueue): swap 后容量在 batch_/ready_queue_ 间
        // 往复保留, 稳态运行零堆分配 (MSVC deque 对 8 字节句柄每块只装
        // 2 个, vector 摊销后每几百次 push 才扩容一次)
        锁内 batch.swap(ready_queue_);            // 一次锁取出整批
        // 刻意"不"在这一刻从 scheduled_set_ 移除!
        // 防止 batch 内前一个协程 cancel 后一个协程时重复入队
        for h in batch:
            锁内 scheduled_set_.erase(h);          // resume 前逐个移除
            if (h && !h.done()) {
                t_current_task = h;
                h.resume();                        // resume 期间新 schedule 的
            }                                      // 留在新队列 → 下轮处理
            t_current_task = nullptr;
    }
    running_ = false; awake_ = true;
}
```

**入睡协议的正确性**是理解跨线程唤醒的钥匙：生产者
（`schedule`/`dispatch`）用 `awake_.exchange(true)` 抢占——
只有抢到 `false`（循环"刚"入睡）才投递唤醒包。
与循环端的"置 false → 锁内复查 → 睡"配对，既不丢唤醒
（复查覆盖窗口），也不滥发（循环醒着时零系统调用）。

### 3.2 定时器与僵尸条目

每条 `TimerEntry { deadline, handle, token }`。token 在两处置位：
`sleep_awaiter::await_resume`（正常到期，标记已消费）和
`sleep_awaiter` 析构（协程帧提前销毁，如 wait_for 提前返回、父协程
被取消）。`process_timers` 弹堆时跳过已置位条目——**僵尸条目惰性清理**，
绝不 resume 已销毁的帧，也无需支持"从堆中删除任意元素"。

---

## 4. Task 的 promise_type 设计

`task.hpp`（1067 行，最大文件）。`Task<T>` 同时是**返回类型 + 句柄
外壳 + awaitable** 三合一。

### 4.1 关键选型

| 选型 | 决定 | 理由 |
|---|---|---|
| `initial_suspend = suspend_always` | **惰性启动** | 创建与启动分离：`bind_loop` 可以在启动前调用；gather/TaskGroup 统一收集后再点火；调用协程函数零副作用 |
| `final_suspend = 自定义 final_awaiter`，`await_suspend` 返回 **false** | 收尾后帧**立即销毁** | 收尾五步（见下）执行完后结果已搬进 Task 外壳，promise 不再被需要——规避了"final_suspend 挂起导致帧滞留、需显式 destroy"这一 C++ 协程最经典的资源陷阱 |
| `await_transform` 存在 | **每个 co_await 被包装**成 `cancel_check_awaiter` | 取消注入的实现基座（第 5 节）；副作用：协程体内不能绕过包装直接 co_await 原始 awaiter |
| 结果双存储 | promise 的 `variant<monostate, T>` 暂存 → final_suspend 时 move 到 `Task::optional<T> result_` | `return_value` 可能先于异常发生；帧销毁后 Task 仍可 `await_resume`/`take_result` |

### 4.2 promise 与外壳的双向链

```
Task<T> (用户持有) ──handle_──> 协程帧 ──promise──> promise_type
      ▲                                                  │
      └────────────── task_ 回指 (move 时更新) ───────────┘

共享不变量:  Task::handle_ 非空 ⟺ 协程帧存活
  由 release_handle() 在帧销毁前置空 handle_ 维持
  —— 这是全库消灭"悬空句柄 UB"的基石
```

`final_awaiter::await_suspend` 收尾五步：

1. `promise.store_result()` — 结果/异常移交 Task 外壳
   （优先级：`cancelled_` 强制取消异常 > variant 结果 > `exception_`）；
2. 有 continuation → 调度回 `continuation_loop_`（**等待者的 loop**，
   await_suspend 时捕获；空则 `EventLoop::get()` 兜底）；
3. `promise.release_handle()` — 清 `Task::handle_`，防 double-free；
4. `on_coroutine_finished(h)` — 活跃计数 -1（记到 `target_loop_`）；
5. 返回 false → 编译器销毁协程帧。

### 4.3 生命周期安全三件套

1. **帧存活不变量**（`handle_` 非空 ⟺ 帧存活）：所有销毁路径
   （正常完成 `release_handle()` / cancel 未启动帧 / `detach()`）
   都在帧销毁**前**清空 Task 侧句柄——Task 永不持有悬空句柄，
   析构时 `handle_` 非空即可安全 destroy，不需要额外的堆上标志
   （早期版本用 `shared_ptr<bool> frame_alive_`，每协程多一次
   堆分配；逐路径审计确认不变量已由 `handle_` 承载后移除）；
2. **`release_handle()`**：收尾时清 Task 侧句柄；
3. **析构/移动赋值补记账**：销毁一个"已启动未完成"的帧时补调
   `on_coroutine_finished`——否则活跃计数泄漏会让事件循环
   **永不退出**（它以为还有协程在飞）。

### 4.4 detach 与 fire-and-forget

`detach()` 置 `task_=nullptr, handle_=nullptr, ready_=true`：协程
自持有到完成。detach 任务的异常不会被静默吞掉——进入全局
`detail::detached_exception_handler()`（默认 stderr 打印，
`CancelledError` 静默，对标 Python 的
"Task exception was never retrieved"），该回调可整体替换。

---

## 5. 取消机制

`CancelledError` 注入的完整链条（对照源码读：

```
task.cancel()
 │
 ├ p.cancelled_.store(true, release)          // atomic: 跨线程可调
 │
 ├ [尚未启动]      store_result 强制写取消异常 → destroy 帧 → 完成
 │
 ├ [挂起在 I/O]    cancel_hook_(cancel_hook_self_)
 │                 = awaiter 的静态 cancel_op:
 │                   CancelIoEx(handle, &op.ov)      // Windows
 │                   io_uring_prep_cancel(sqe)       // Linux
 │                 // 不直接 schedule! 必须等"已取消"完成包先被消费,
 │                 // OVERLAPPED/SQE 生命周期才安全; 完成包唤醒协程后
 │                 // 走下方注入点
 │
 ├ [挂起在等待]    target_loop_->schedule(handle_)   // 强制唤醒
 │                 (sleep/锁/队列/任务等待都走这条)
 │
 └ [就绪队列中]    什么都不做 (幂等去重已保证不重复入队)

注入点 (每次恢复都经过 cancel_check_awaiter):
  await_ready():   cancelled_ 已置 → true (不挂起, 直奔 resume 抛)
                   // 防"已取消的任务再次挂起而悬死"
  await_resume():  cancelled_ 为真 → 清除标志 (一次性注入!) →
                   throw CancelledError{}  // 不调 inner.await_resume,
                                           // 底层结果直接丢弃 (Python 语义)
                   // 清除标志的意义: 协程体 catch 住取消 (取消保护) 后
                   // 任务仍可正常完成

异常展开:
  CancelledError 沿协程体传播 → 栈对象析构 (RAII 清理) →
  未捕获则存为任务异常 → 等待者 rethrow / ExceptionGroup 过滤 /
  detached handler 静默
```

配套的**收尾清理**（防取消引发悬空）：

- `sleep_awaiter::~sleep_awaiter` → 置 token → 定时器堆僵尸惰性清理；
- `cancel_check_awaiter` 析构 → 若帧在挂起中被销毁（取消展开路径，
  await_resume 未执行）→ 调 inner 的 `on_waiter_destroyed(my_handle)`，
  把僵尸句柄从 Future.waiters / Lock / Semaphore / Event / Condition /
  Queue 等待队列 / signal 等待列表 / Task::continuation_ 中摘除。

这套设计保证了三条用户可感知的性质：

1. 取消总是发生在 await 点（不会撕裂任意两条指令之间）；
2. 挂在**任何**库原语上的协程都能被立刻唤醒取消（含底层 IO 系统调用）；
3. catch 住 `CancelledError` 即实现取消保护，任务可继续完成。

---

## 6. 并发组合器：monitor 协程模式

gather / gather_all / gather_void / wait_any / wait_tasks / TaskGroup
全部基于同一个模式：

```
调用者协程 C:  co_await gather(t1, t2, t3)

await_suspend:
  创建 shared_ptr<shared_state> { tuple<Ts...> results,
                                  atomic<size_t> remaining,
                                  coroutine_handle<> continuation,
                                  exception_ptr first_exception }
  为每个 Ti 生成一个 monitor_i 协程 (Task<void>, 命名函数):
      monitor_i: result_i = co_await ti;         // 异常则记 first_exception
                 if (--remaining == 0) schedule(continuation)
  全部 start() + detach() —— 真正并发启动, monitor 帧自持有运行到完成

完成: 最后一个 monitor 把 C 调度回来, await_resume 组装 tuple / 重抛异常
```

为什么需要 monitor？因为一个 Task 只能被 co_await 一次、
且要统一"谁启动、谁计数、谁唤醒"。monitor 一对一包装后，
每个子任务的生命周期互不干扰，共享状态只被原子计数器协调。

monitor 本身是 fire-and-forget：`start()` 后立即 `detach()`，
协程帧自持有到完成——不需要堆上的 `shared_ptr<Task>` 保活
（共享状态 `shared_ptr<shared_state>` 仍必需：FirstCompleted 等
提前返回模式下，caller 先离开而剩余 monitor 继续跑，状态生命周期
由它自动延长）。TaskGroup 例外：组需要持有 monitor 句柄做组取消，
仍用 `shared_ptr<Task<void>>` 管理。

TaskGroup 的组取消在此基础上多两步：

1. 任一 monitor 记录到真实异常 → 置 `cancel_requested` →
   遍历 monitors 逐个 `m->cancel()`，**跳过自己**
   （取消自己会在帧销毁后被 resume → UB）；
2. 收到组取消的 monitor 把取消转发给原任务并等它结束；
   任务在取消生效前已自行失败的异常仍会聚合（对标 Python）；
   纯粹由组取消引发的 `CancelledError` 不进 ExceptionGroup。

---

## 7. 同步原语与 Future 的实现要点

### sync.hpp（461 行）与 queue.hpp（257 行）

- 全部**无内部锁**：等待队列是 `deque<coroutine_handle<>>`，
  仅 loop 线程操作（与事件循环同一线程，天然串行）；
- `Lock::release` 的**锁移交**语义：有等待者时 `locked_` 保持 true
  直接 schedule 队首——省去"解锁→立即被抢→原等待者饿"的窗口；
- `Condition::wait` 是一个小协程：排队 → 释放锁挂起 → 被唤醒 →
  重新拿锁返回。取消时**先重新拿回锁再传播异常**（MSVC 不允许
  catch 块内 co_await，所以 catch 只记标志、块外重拿锁再抛）——
  保证守卫析构时锁恰好被释放一次；
- 五个原语都实现 `on_waiter_destroyed`，配合 cancel_check_awaiter
  的析构钩子做僵尸等待者摘除（见第 5 节）。

### future.hpp（402 行）

```
Promise<T> ──get_future──> SharedState(shared_ptr) <──共享── Future<T> ×N
                                │ mutex
                                ├ value / exception / ready
                                └ vector<Waiter{handle, loop}>
```

- `set_value`：锁内写状态 → swap 出全部 waiter → 锁外逐个
  `waiter.loop->schedule(waiter.handle)` —— **逐等待者 loop 路由**：
  每个等待者挂起时记录自己所在的 loop，跨线程 set 时各自被送回
  自己的"家"，协程帧绝不跨线程迁移；
- `await_suspend` 锁内**二次检查 ready**：封掉 "刚检查完未就绪、
  正要入队时对方 set 完并唤醒" 的丢唤醒窗口；
- `to_thread`、process 的退出通知、signal（Windows 路径）都构建在
  这套跨线程 Promise 之上。

---

## 8. IO 层：Proactor 统一完成路径

net / fs / pipe / fs_watch / signal(Linux) 共用同一条路径
（io.hpp 158 行 + 各模块头文件）：

```
awaiter (协程帧内) 持有 detail::iocp_op / uring_op
  发起: iocp: associate(handle) + op_start() + WSA/Win32 异步调用
        uring: uring_submit(u, sqe, op)   [io.hpp 提供]
  完成: wait_for 排空完成包/CQE
        → op.error / op.transferred (或 op.result)
        → on_complete(op.continuation) → EventLoop::schedule
  恢复: await_resume 把结果翻译成统一错误模型
        errno = 平台无关值; io::last_error() = 原生码 (thread_local)
```

### 统一错误模型（io.hpp）

`io::set_error(native)` 一次写两处：`errno`（经
`wsa_to_errno` 转换，strerror 可用）与 thread_local
`io::last_error()`（原生码）。历史教训写在注释里：旧版直接
`errno = WSA 码`，两套编码互不兼容，strerror 乱码、
`errno == EWOULDBLOCK` 判断失效。特殊翻译：
`ERROR_OPERATION_ABORTED`（取消）→ `EINTR`（MSVC errno 无 ECANCELED）；
`ERROR_HANDLE_EOF` / `ERROR_BROKEN_PIPE` → 0（EOF 语义）。

### 平台难点备忘（net.hpp 977 行 / fs.hpp 752 行）

- Windows `ConnectEx` 要求 socket 先 bind 本地地址（否则 WSAEINVAL）；
  `get_connect_ex()` 进程级 WSAIoctl 缓存；
- `AcceptEx` 需要本地/远端地址缓冲（`2*(sizeof(SOCKADDR_STORAGE)+16)`，
  awaiter 在协程帧内保活）；AcceptEx/ConnectEx **同步成功不投递
  完成包**，awaiter 手动 schedule——与 WSARecv 等的三形态差异；
- 文件定位读写：Windows 用 `OVERLAPPED.Offset/OffsetHigh` 拆 64 位
  offset；io_uring 的 READ/WRITE 原生带 offset——这就是 `read_at/
  write_at` 无游标设计的实现基础；
- 管道：Windows 匿名管道不支持 OVERLAPPED，用命名管道对
  （`\\.\pipe\coro_pipe_<pid>_<n>` 进程内计数器命名）；
  Linux `pipe2(O_NONBLOCK)` + 读写 offset -1；
- 目录监视：Windows `ReadDirectoryChangesW`（一次完成包含
  `FILE_NOTIFY_INFORMATION` 记录链，全部解析进 pending 队列；
  RENAMED_OLD/NEW_NAME 配对；`ERROR_NOTIFY_ENUM_DIR` → overflow）；
  Linux inotify fd 经 io_uring 持续读，`IN_MOVED_FROM/TO` 用 cookie
  配对，`IN_Q_OVERFLOW` → overflow；
- 信号：Windows 进程级单例 manager（`SetConsoleCtrlHandler` +
  CRT `std::signal` 双路桥接，MSVC `raise()` 后复位 SIG_DFL 所以
  处理器内每次重装）；Linux 每 loop 一个 manager（`pthread_sigmask`
  阻塞 + signalfd + 常驻读者协程，无等待者时读者被 cancel 不阻 loop
  退出）。

### 取消挂钩（cancel_op）

所有 IO awaiter 提供 `static cancel_op(void*)`：
`CancelIoEx`（产生 ERROR_OPERATION_ABORTED 完成包，**保证 OVERLAPPED
先于协程帧销毁被消费**）/ `io_uring_prep_cancel`（原操作收
-ECANCELED CQE）。task.hpp 的 `detail::has_cancel_op<T>` 检测该钩子，
cancel_check_awaiter 在挂起时注册到 `promise->cancel_hook_`。

---

## 9. 多线程模型：线程亲缘与跨线程路由

模型选择：**每线程一个 loop，任务不迁移**（对标 asyncio
loop-per-thread），而非 Go 式 work-stealing。收益：

- 同线程内单线程语义 → 库的全部原语零内部锁；
- 协程帧归属线程固定 → 无帧跨线程分配/释放问题
  （MSVC Debug CRT 会在跨线程 free 帧时堆断言）；
- 行为可预测、实现简单。

需要跨线程的两类事件都有精确路由：

| 事件 | 路由机制 |
|---|---|
| `Promise::set_value`（任意线程） | 每个等待者记录自己的 loop，唤醒逐个送回"家" |
| 任务完成唤醒 continuation | promise 记录 `continuation_loop_`（等待者的 loop） |
| `Task::cancel()`（任意线程） | 唤醒路由到协程自己的 `target_loop_` |
| `Scheduler::spawn_any` | dispatch **工厂**到 worker 线程，帧在 worker 上创建 |

`Scheduler`（scheduler.hpp 193 行）= N × `EventLoop::run_until_stopped()`
+ 负载选择 + dispatch。选 worker：主键 `active_task_count()`，
次键累计分发数（活跃数恒 0 的短任务场景防聚集，退化为 round-robin）。
`wait_all` 用自适应退避轮询（50µs 起每轮翻倍至 1ms 封顶）而非常规的
完成通知——每协程完成都 notify 会给热路径加系统调用，快任务场景
轮询反而快一个数量级。

与手动 loop-per-thread 完全兼容、可混合；Windows 上 socket 与 IOCP
（即创建线程）绑定，跨线程要用 `accept_noattach()` + worker 内
`reattach()`（io_uring 无此概念，fd 即用）。

---

## 10. include 依赖图

```
第 0 层 (无内部依赖):   exceptions.hpp, event_source.hpp
第 1 层:   iocp_event_source [WIN32] / uring_event_source [Linux] ─┐
           event_loop.hpp ◄────────────────────────────────────────┘
第 2 层:   task.hpp, sleep.hpp, future.hpp, io.hpp
第 3 层:   sync.hpp, gather.hpp, schedule.hpp, wait.hpp, task_group.hpp,
           thread.hpp, scheduler.hpp, net.hpp
第 4 层:   queue.hpp, fs.hpp, pipe.hpp, signal.hpp, fs_watch.hpp
第 5 层:   process.hpp
第 6 层:   coro.hpp (聚合核心 + 并发; 不含 IO 模块)
```

要点：**coro.hpp 是"核心 + 高级并发"**；IO 六件套（net/fs/pipe/
process/fs_watch/signal）按需单独 include。`io.hpp` 虽定位为 IO 底座，
include 的是 event_loop.hpp（取 `EventLoop::get().iocp()/uring()`），
Linux 部分直接引用 `net::UringEventSource`（经 event_loop.hpp 条件
包含获得）。

---

## 11. 八大横切设计模式

读源码时反复出现的套路，可作为代码评审/贡献的检查单：

1. **Proactor 统一完成路径**：所有 IO 共用 `iocp_op`/`uring_op` +
   `op_start`/`has_pending` 挂起计数 + `on_complete → schedule`；
   awaiter 三分支处理（PENDING / 同步成功仍投递 / 立即失败手动恢复）。
2. **双通道错误模型**：`errno`（跨平台）+ `io::last_error()`（原生码），
   `io::set_error` 一次写两处。
3. **取消安全三板斧**：
   `cancel_op` 静态钩子（先取消底层 IO，等完成包唤醒）；
   `on_waiter_destroyed`（帧销毁时摘等待队列僵尸句柄）；
   定时器 token（僵尸条目惰性清理）。
4. **monitor 协程模式**：每子任务一个 Task\<void\> 包装 +
   `shared_ptr` 状态 + atomic 计数器，最后一个完成者调度调用方。
5. **命名协程 + `start()+detach()` 自持有**：所有 fire-and-forget
   （schedule/wait/signal reader/handle loop）用命名协程函数
   （参数进协程帧，生命周期由帧保证）+ detach——协程帧自持有运行
   到完成，无需堆上 Task 对象，也规避了 MSVC Debug lambda 帧捕获问题。
6. **逐等待者 loop 路由**：Future / signal(Win) / process 退出通知，
   每个等待者记录挂起时所在 loop，唤醒路由回"家"。
7. **入睡协议**：`awake_` seq_cst exchange，跨线程唤醒只投递给
   "刚入睡"的循环——不丢唤醒也不滥发。
8. **每线程一个 EventLoop**：thread_local 惰性单例 + 线程亲缘，
   Scheduler 在其上做"帧在正确线程创建"的分发。

---

## 12. 代码规模与阅读顺序

| 文件 | 行数 | 主题 |
|---|---|---|
| task.hpp | 1046 | Task / promise / cancel |
| net.hpp | 977 | TCP (IOCP/io_uring) |
| wait.hpp | 463 | wait_for / wait_any / wait_tasks / gather_all |
| event_loop.hpp | 618 | 循环核心 |
| fs.hpp | 752 | 文件 IO |
| fs_watch.hpp | 535 | 目录监视 |
| pipe.hpp | 531 | 管道 |
| signal.hpp | 525 | 信号 |
| process.hpp | 518 | 子进程 |
| sync.hpp | 461 | 四原语 |
| future.hpp | 402 | Promise/Future |
| gather.hpp | 263 | 静态 gather |
| queue.hpp | 257 | 队列 |
| task_group.hpp | 230 | 结构化并发 |
| scheduler.hpp | 193 | 多核分发 |
| thread.hpp | 172 | to_thread |
| io.hpp | 158 | 错误模型 |
| iocp_event_source.hpp | 155 | Windows 事件源 |
| uring_event_source.hpp | 150 | Linux 事件源 |
| sleep.hpp | 132 | 时间原语 |
| schedule.hpp | 120 | call_* |
| event_source.hpp | 120 | 事件源抽象 |
| exceptions.hpp | 48 | ExceptionGroup |
| coro.hpp | 59 | 聚合入口 |

**推荐阅读顺序**（每个文件都能在前一个的知识上展开）：

```
sleep.hpp        → 最简单的 awaiter, 看懂挂起/恢复/定时器
event_loop.hpp   → 主循环 + 入睡协议 (本文档第 3 节对照读)
task.hpp         → promise_type 五件套 + 取消包装器 (第 4、5 节)
sync.hpp/queue.hpp → 无锁单线程等待队列怎么写
future.hpp       → 跨线程路由 + lost-wakeup 二次检查
gather.hpp/wait.hpp/task_group.hpp → monitor 模式三连
io.hpp → net.hpp → fs.hpp → pipe.hpp → signal.hpp → fs_watch.hpp
                 → Proactor 路径的六个变奏
process.hpp      → 组合技 (pipe + future + 平台进程 API)
scheduler.hpp/thread.hpp → 多核层
```

配套的测试（`tests/test_*.cpp`，22 个文件）是行为的最佳注解——
每个头文件都有同名测试，改代码前先跑
`ctest --test-dir build -C Debug --output-on-failure`。

---

## 13. 刻意的非优化与已知约束

记录「看起来能做、实际不做」的决策及原因——防止未来重复踩坑或误优化。

### 13.1 对称转移未启用（MSVC C4737）

`co_await task` 的 `await_suspend` 返回子协程句柄可实现对称转移
（直跳执行, 绕开就绪队列, 预计再省 ~30% 串行调度开销）。实测被
MSVC 拒绝：**`cancel_check_awaiter` 的析构函数承担取消清理职责**
（协程帧销毁时摘除等待队列中的僵尸句柄）, 而 MSVC 要求对称转移
路径上的 awaiter 临时对象可尾调用（C4737: "无法执行所需尾调用"）。
启用前提是重新设计等待者清理协议（如把清理挪进 promise 的
final_suspend 路径）, 收益/风险比暂不划算。代码内留有注释锚点
（`task.hpp` 搜「对称转移」）。

### 13.2 每 sleep 一次 shared_ptr 分配（保留）

定时器 token 用 `shared_ptr<atomic<bool>>`：堆条目可能比 awaiter
（协程帧内）活得久, 帧销毁后 `process_timers` 仍要读标志, 共享所有权
是唯一安全方案。池化需要「awaiter 销毁 ∩ 条目出堆」双条件归还,
等价于重新发明引用计数。

### 13.3 resume 前逐个 erase 去重集（不能批量）

见 3.2 节 batch 消费循环的注释——批量提前擦除会在「batch 内前面的
协程 cancel 后面的协程」场景引入 double-resume UB。

### 13.4 wait_all 保留（自适应）轮询

事件驱动关停需要每次协程完成时 notify（Windows 上是系统调用）,
给热路径加常数开销; 轮询只发生在关停/测试边界。已从固定 1ms 改为
50µs 起步指数退避, 上限 1ms。

### 13.5 Linux 路径未实测

io_uring 分支（含 fs/pipe/signal/fs_watch/process 的 Linux 段）按
net.hpp 模式对称编写, 无 Linux 构建环境验证。头文件注释均标注。
上线 Linux 前的先决条件: CI 加 ubuntu runner 编译 + 跑测试。
