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
| `CancelledError` | 任务被 `cancel()`，在下一个 await 点注入；超时（wait_for 内部先 cancel 后转 TimeoutError，等待者看到的是 TimeoutError）；组/父级取消传播 |
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
- **生命周期**：Task 对象必须存活到协程完成；`spawn` 返回值必须保存
  （丢弃 = 立即销毁协程帧）。
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
- `co_await coro::yield()` 不经定时器堆，开销最小（实测 ~160ns）。

---

## 并发组合 gather.hpp / wait.hpp

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

### gather_all / gather_void — 动态 N 路 / void 路

```cpp
namespace coro {
    template <typename T>
    Task<std::vector<T>> gather_all(std::vector<Task<T>> tasks);   // 接管所有权

    template <typename... Ts>
    Task<void> gather_void(Task<Ts>... tasks);    // 混合 void 任务, 编译期数量
}
```

- 语义同 gather：并发启动；结果按原顺序；异常 = 等全部完成后
  重抛第一个。
- 动态数量的 void 任务用
  `wait_tasks(std::move(v), WaitMode::AllCompleted)`。

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

### wait_any — 两路竞速

```cpp
namespace coro {
    template <typename T>
    Task<T> wait_any(Task<T> a, Task<T> b);       // FIRST_COMPLETED
}
```

先完成者（成败均可）胜出；落选者**继续后台运行**不被取消。

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

FIFO 公平、非递归（重复 acquire 死锁）；释放时直接移交队首等待者。

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
    Task<std::invoke_result_t<F>> to_thread(F func);
}
```

- `func` 在进程级线程池（默认 `hardware_concurrency` 线程）执行；
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
- `read` 返回 0 = 对端正常关闭；-1 = 错误；
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
    /*awaiter*/ wait(int sig);              // → int 信号编号; 不支持 → std::invalid_argument
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

- 支持集合：`SIGINT` / `SIGTERM` / `SIGBREAK` / `SIGHUP`（Windows 自定义
  SIGHUP=3 映射"关窗"事件）；其余信号抛 `std::invalid_argument`；
- Windows 无真信号：控制台事件（Ctrl+C / Ctrl+Break / 关窗）与 CRT
  `raise()` 双路桥接；SIGHUP 无法经 `raise()` 触发；
- Linux 用 signalfd + io_uring；**信号在首次 wait/handle 时于当前线程
  阻塞**（pthread_sigmask），之后创建的线程继承掩码——多线程程序
  应先注册信号再开线程；
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
  Linux：inotify（v1 递归能力有限，见
  [已知限制](coro-guide.md#十五已知限制)）。

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
