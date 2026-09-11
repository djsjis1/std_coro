# 第 12 讲：异步 IO 基础

## 核心要点

- 异步 IO 的本质：**发起操作后不阻塞线程，完成时收到通知**。
- 把 IO 接入协程的标准三步：发起操作 → 挂起（把句柄注册给完成通知机制）→ 完成回调里 `resume`。
- `await_suspend` 是 IO 与协程的**唯一接缝**——句柄交给谁，谁就在完成时唤醒你。
- 三套主流完成通知机制：**回调**（最通用）、**多路复用 epoll/kqueue/IOCP**（事件循环）、**io_uring**（Linux 最新高性能方案）。
- 协程无法凭空变出异步 IO：它只是把"回调式 IO"改写成"同步形状"。底层 IO 机制决定了框架的性能上限。

---

## 12.1 阻塞 IO 的问题

```cpp
int n = ::read(fd, buf, size);     // 阻塞: 数据到达前, 线程什么都干不了
```

在事件循环线程里调用阻塞 IO = 灾难：整条线程冻结，所有协程停摆（同第 11 讲 `std::mutex` 问题）。

异步 IO 的形态：

```
发起:  "我要从 fd 读 1024 字节" → 立即返回 (可能部分完成)
等待:  线程去干别的
完成:  "数据到了!" → 通知等待者
```

## 12.2 回调式 IO → 协程的三步转换

以"注册回调"风格的假想 API 为例：

```cpp
// 回调式 API (库提供的)
void async_read(int fd, char* buf, size_t n,
                std::function<void(int result)> on_complete);
```

包装成协程可等待的 awaiter：

```cpp
struct AsyncRead {
    int fd;
    char* buf;
    size_t n;

    bool await_ready() const { return false; }              // 总是异步

    void await_suspend(std::coroutine_handle<> h) {
        // 关键: 把"被挂起协程的句柄"交给回调
        async_read(fd, buf, n, [h](int result) {
            completed_result = result;      // (跨线程传递需同步, 见 12.5)
            h.resume();                     // 完成 → 唤醒协程
        });
    }

    int await_resume() { return completed_result; }   // 协程恢复后取结果
};
```

三步归纳：

1. **发起**：`await_suspend` 里启动异步操作。
2. **挂起**：句柄 `h` 被存进回调/完成队列。
3. **恢复**：完成通知到达时 `h.resume()`。

之后协程代码就能写：

```cpp
Task<> echo(int fd) {
    char buf[1024];
    int n = co_await AsyncRead{fd, buf, sizeof(buf)};   // 像同步一样读
    co_await AsyncWrite{fd, buf, (size_t)n};
}
```

## 12.3 多路复用：epoll / kqueue / IOCP

### 事件循环的标准骨架

```
while (running) {
    1. 处理到期的定时器
    2. 等待 IO 就绪: epoll_wait / kevent / GetQueuedCompletionStatus
       (有超时上限 = 最近定时器的剩余时间)
    3. 就绪的 IO → 唤醒对应协程
    4. 跑就绪队列里的协程 (它们会发起新的 IO, 回到步骤 2)
}
```

### 三大平台的对应关系

| 平台 | 机制 | 模型 |
|---|---|---|
| Linux | `epoll` | 就绪通知（reactor）：fd 可读/可写了才去操作 |
| macOS/BSD | `kqueue` | 同 epoll |
| Windows | IOCP（完成端口） | 完成通知（proactor）：发起操作，完成时直接拿到数据 |
| Linux（新） | `io_uring` | 完成通知 + 批量化提交，性能最强 |

### epoll 风格（reactor）

```cpp
// 注册: 关心 fd 的可读事件
epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev);

// 等待: 单线程同时等几千个 fd
int n = epoll_wait(epfd, events, MAX_EVENTS, timeout_ms);

// 就绪 → 唤醒"正在等这个 fd 可读"的协程
for (int i = 0; i < n; ++i)
    wake_waiter(events[i].data.ptr);      // resume 协程句柄
```

关键映射：**fd 可读事件 → 某个挂起的协程**。事件循环维护 "fd → 等待者句柄" 的登记表，await_suspend 里登记，事件到达时唤醒。

### IOCP 风格（proactor）

```cpp
// 发起异步读: 提交一个 OVERLAPPED 结构 (内含协程句柄)
WSARecv(sock, &buf, 1, &received, &flags, &ov, nullptr);

// 等待: 任何操作完成都从这里取出
GetQueuedCompletionStatus(iocp, &bytes, &key, &ov, timeout);

// ov 里反查出等待的协程 → resume
```

区别：reactor 通知你"**可以去读了**"（还要发起读）；proactor 通知你"**已经读完了**"（数据直接给你）。

## 12.4 io_uring：新一代方案

io_uring 的核心是**提交队列（SQ）/ 完成队列（CQ）环形缓冲**：

```
用户态:    提交 SQE (我想读 fd, buf, n)      io_uring_submit()
内核:      执行 (异步)
用户态:    轮询 CQE → 得到结果 → resume 协程   io_uring_wait_cqe()
```

优势：系统调用次数极少（批量提交/批量收割）、零拷贝路径、可注册文件/缓冲。劣势：仅 Linux 5.1+，API 底层（一般用 liburing 封装）。

与协程的接缝同样简单：`await_suspend` 里提交 SQE（user_data 存协程句柄），CQE 到达时取回句柄并 `resume`。

## 12.5 跨线程完成通知

回调可能在**工作线程**触发（如线程池 IO），而协程属于事件循环线程。唤醒需要跨越线程：

```cpp
// 工作线程里的回调:
void on_complete(std::coroutine_handle<> h) {
    // ❌ 直接 h.resume() —— 协程会在工作线程执行, 破坏单线程模型
    // ✅ 正确: 把句柄塞回事件循环的就绪队列 (内部加锁) 并唤醒事件循环
    event_loop.schedule_from_other_thread(h);
}
```

要点：`schedule_from_other_thread` 内部 = 加锁入队 + 唤醒事件循环的等待（如 `eventfd` 写一字节、IOCP 投递唤醒包、`condition_variable`）。这与第 11 讲"单线程原语不加锁"并不矛盾：**跨线程边界才需要锁**。

## 12.6 取消挂起中的 IO

第 9 讲的取消语义在 IO 场景有特殊要求：协程挂起在 IO 上时被 cancel，必须**先取消底层 IO**（否则要么泄漏要么等 IO 完成才能响应取消）：

```cpp
void cancel() {
    cancelled_ = true;
    if (cancel_hook_) cancel_hook_();   // 挂起在 IO 上时注册的钩子
    else schedule(handle_);             // 挂起在时间/同步上: 直接唤醒
}

// await_suspend 里注册钩子 (以 IOCP 为例):
void await_suspend(std::coroutine_handle<> h) {
    promise.cancel_hook_ = [this] { CancelIoEx(sock, &op.ov); };  // 取消底层读
    /* 发起 IO... */
}
```

取消后：底层 IO 以"被取消"完成 → 完成包照常唤醒协程 → `await_resume` 检查 cancelled_ → 抛出 `CancelledError`。**顺序不能反**：完成包必须先被消费，协程帧销毁才安全（因为 OVERLAPPED 存在帧里）。

## 12.7 小结

1. 异步 IO 接入协程 = 发起 → 挂起（句柄登记）→ 完成时 resume。
2. reactor（epoll/kqueue）vs proactor（IOCP/io_uring）是两种完成通知哲学。
3. 跨线程完成通知必须"回投"事件循环，不能直接 resume。
4. 取消挂起中的 IO 要联动取消底层操作，并保证完成包先于帧销毁被消费。
5. 协程库的 IO 性能上限由底层机制决定——协程只是让它"写得像同步"。

## 思考题

1. reactor 与 proactor 的核心区别是什么？为什么 Windows 生态普遍用 proactor？
2. 为什么跨线程完成回调里不能直接 `h.resume()`？画出错误的时间线。
3. 一个事件循环同时支持 epoll 和定时器，`epoll_wait` 的超时参数应该怎么算？（提示：最近定时器与当前时间的差）
