# 第 9 讲：取消与停止

## 核心要点

- 协程**不会自己消失**：取消必须有"注入点"——要么在 await 点抛异常，要么协程主动检查停止标志。
- 两种主流模式：
  - **CancelledError 异常模式**（Python asyncio 风格）：取消请求在下一个 await 点变成异常抛进协程体，栈上对象析构即清理。
  - **`std::stop_token` 模式**（C++20 标准）：协程显式携带令牌，主动轮询 `stop_requested()`，不抛异常。
- **绝不能**用"销毁未完成协程的帧"来取消：帧内对象的析构函数不会执行（资源泄漏、锁不释放）。
- 挂起中的协程被取消后，必须能被**唤醒**（否则取消永远不生效）——这是取消设计与调度器的交互点。

---

## 9.1 为什么取消很难

协程是"会停在半路"的函数。取消 = 让它**提前安全地结束**，难点有三：

1. **挂起中的协程听不到你的请求**：它在等 sleep/IO/锁。必须有人 resume 它，它才有机会"处理取消"。
2. **清理逻辑必须执行**：协程可能持有资源（socket、锁、事务），粗暴销毁帧 = 析构函数全部跳过。
3. **等待者要得到通知**：等待被取消协程的那一方，必须收到明确的"已取消"信号，而不是永远挂起。

## 9.2 反模式：直接 destroy

```cpp
auto t = spawn(long_task());
t.destroy_frame();      // ❌ 教学警示: 假设存在这样的 API
```

这样做的问题：协程帧里 `std::string`、`std::fstream`、RAII 锁守卫的**析构函数全部不会执行**。文件句柄泄漏、互斥锁永远锁死、数据库连接不归还。正确取消必须让协程**跑完"清理路径"**。

## 9.3 模式一：CancelledError 异常注入（asyncio 风格）

核心思想：**取消 = 在下一个 await 点抛异常**。栈展开过程自动执行所有清理——这正是异常的本职工作。

### 机制总览

```
cancel() 被调用
  → 置 cancelled_ 标志
  → 强制唤醒挂起中的协程 (schedule 它)
协程恢复执行
  → 在挂起点 (await_resume) 检查 cancelled_
  → 抛出 CancelledError
  → 协程体栈展开: 所有局部对象析构 = 清理逻辑
  → unhandled_exception 捕获
  → 任务以"已取消"状态完成
等待者收到 CancelledError
```

### 关键实现：用 `await_transform` 包装每个 await 点

第 4 讲学过：协程体内每次 `co_await` 都经过 `await_transform`。把"取消检查"注入到这里，就能让**任何** await 点感知取消：

```cpp
// 编译: g++ -std=c++20 cancel_inject.cpp && ./a.out
#include <coroutine>
#include <exception>
#include <iostream>
#include <queue>

struct CancelledError : std::runtime_error {
    CancelledError() : std::runtime_error("cancelled") {}
};

// ── 简化调度器 (第 6 讲) ──
struct Scheduler {
    std::queue<std::coroutine_handle<>> ready;
    void schedule(std::coroutine_handle<> h) { ready.push(h); }
    void run() {
        while (!ready.empty()) {
            auto h = ready.front(); ready.pop();
            if (!h.done()) h.resume();
        }
    }
};
Scheduler sched;

template <typename T = void> class Task;

// ── 取消检查包装器: 包住真正的 awaiter ──
template <typename Promise, typename Awaiter>
struct cancel_check {
    Awaiter inner;
    Promise* promise;
    const bool* flag;
    std::coroutine_handle<> me;

    bool await_ready() {
        if (*flag) return true;              // 已取消: 别挂起, 直接进 await_resume 抛
        return inner.await_ready();
    }
    void await_suspend(std::coroutine_handle<Promise> h) {
        me = h;
        promise->suspended = true;
        inner.await_suspend(h);
    }
    auto await_resume() {
        promise->suspended = false;
        if (*flag) throw CancelledError{};   // ← 注入点: 在挂起点抛出
        return inner.await_resume();
    }
    ~cancel_check() {
        promise->suspended = false;
    }
};

template <typename T>
class Task {
public:
    struct promise_type {
        Task get_return_object() {
            return Task{std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        std::suspend_always initial_suspend() { return {}; }

        // 自定义 final_awaiter: 帧保留, 收尾时搬运结果并唤醒等待者
        struct final_awaiter {
            bool await_ready() noexcept { return false; }
            void await_suspend(std::coroutine_handle<promise_type> h) noexcept {
                h.promise().store_result();          // 搬运 + 唤醒 waiter
            }
            void await_resume() noexcept {}
        };
        final_awaiter final_suspend() noexcept { return {}; }

        void unhandled_exception() { exception_ = std::current_exception(); }
        template <typename U> void return_value(U&& v) { result_ = std::forward<U>(v); }

        template <typename A>
        auto await_transform(A&& a) {
            return cancel_check<promise_type, A>{std::forward<A>(a), this, &cancelled_};
        }

        void store_result() {
            if (cancelled_) task_->exception_ = std::make_exception_ptr(CancelledError{});
            else if (exception_) task_->exception_ = std::move(exception_);
            else task_->result_ = std::move(result_);
            task_->ready_ = true;
            if (task_->waiter_) sched.schedule(task_->waiter_);
        }

        T result_{};
        std::exception_ptr exception_;
        std::coroutine_handle<> waiter_;
        Task* task_ = nullptr;
        bool cancelled_ = false;
        bool suspended = false;
    };

    using H = std::coroutine_handle<promise_type>;
    H h_ = nullptr;
    std::optional<T> result_;
    std::exception_ptr exception_;
    std::coroutine_handle<> waiter_ = nullptr;
    bool ready_ = false;
    bool started_ = false;

    explicit Task(H h) : h_(h) { if (h_) h_.promise().task_ = this; }
    ~Task() { if (h_) h_.destroy(); }   // final_suspend 挂起 → 帧由外壳销毁
    Task(Task&& o) noexcept : h_(std::exchange(o.h_, nullptr)),
        result_(std::move(o.result_)), exception_(std::move(o.exception_)),
        waiter_(o.waiter_), ready_(o.ready_), started_(o.started_) {
        if (h_) h_.promise().task_ = this;
    }
    Task(const Task&) = delete;

    bool await_ready() const { return ready_; }
    void await_suspend(std::coroutine_handle<> w) {
        h_.promise().waiter_ = w;
        if (!started_) { started_ = true; sched.schedule(h_); }
    }
    T await_resume() {
        if (exception_) std::rethrow_exception(exception_);
        return std::move(*result_);
    }

    void start() { if (!started_ && h_) { started_ = true; sched.schedule(h_); } }

    void cancel() {
        if (h_ && !ready_) {
            auto& p = h_.promise();
            p.cancelled_ = true;
            if (!started_) {                 // 还没启动: 直接完成
                p.store_result();
                return;
            }
            if (p.suspended) sched.schedule(h_);   // 挂起中: 强制唤醒 (关键!)
            // 若已在就绪队列: 不必重复调度, 它跑到第一个 await 点自然抛
        }
    }
};
// (Task<void> 特化略, 模式相同)

// ── 演示 ──
Task<int> slow_work() {
    struct Guard { ~Guard() { std::cout << "  [清理] Guard 析构\n"; } } g;
    std::cout << "  [协程] 开始工作\n";
    co_await std::suspend_always{};          // 第一段工作
    co_await std::suspend_always{};          // 第二段工作
    std::cout << "  [协程] 不该到达这里\n";
    co_return 42;
}

// 等待者视角: co_await 被取消的任务 → 收到 CancelledError
Task<int> expect_cancelled(Task<int> t) {
    try {
        (void)co_await std::move(t);
        std::cout << "[main] FAIL: 未收到取消\n";
    } catch (const CancelledError&) {
        std::cout << "[main] 捕获 CancelledError (等待者视角)\n";
    }
    co_return 0;
}

int main() {
    Task<int> t = slow_work();       // 惰性
    t.start();
    sched.run();                     // 协程打印"开始", 挂在第一个 suspend_always

    t.cancel();                      // 置标志 + 强制唤醒 (suspended == true)
    sched.run();                     // 恢复 → await_resume 抛 CancelledError
                                     // → Guard 析构 → 任务以异常完成

    Task<int> w = expect_cancelled(std::move(t));   // 等待已取消的任务
    w.start();
    sched.run();                     // await_ready == true → 直接重抛 → catch
    return 0;
}

// 输出:
//   [协程] 开始工作
//   [清理] Guard 析构
//   [main] 捕获 CancelledError (等待者视角)
```

> 说明：这个示例是完整可编译的。核心要观察两个点：`cancel_check::await_resume` 里的
> `throw CancelledError{}`（注入点），以及 `cancel()` 里的**强制唤醒**（suspended 时才
> schedule）。这两点构成异常注入式取消的完整闭环。

### 这个模式的优点

- 清理逻辑**免费**：栈展开即 RAII，用户零成本获得 `finally` 语义
- 等待者明确收到 `CancelledError`，不会挂死
- 无限循环任务也能终止（每个 await 点都是检查点）

### 注意边界

- **纯 CPU 死循环**（不含任何 `co_await`）无法取消——与 Python 相同
- 挂起在"无人唤醒的等待队列"（如永远不释放的锁）上时，强制唤醒后协程才能响应取消
- 挂起在底层 IO 上时，取消还要联动取消 IO 操作（第 12 讲）

## 9.4 模式二：`std::stop_token`（协作式轮询）

C++20 标准库自带 `std::stop_source` / `std::stop_token`（原为 `<thread>` 设施）：

```cpp
// 编译: g++ -std=c++20 stop_token.cpp && ./a.out
#include <coroutine>
#include <iostream>
#include <stop_token>

// 协程显式携带令牌
Task<> worker(std::stop_token st) {
    while (!st.stop_requested()) {          // 主动检查
        co_await do_chunk();
    }
    std::cout << "worker 收到停止请求, 优雅退出\n";
    co_return;
}

// 调用侧
std::stop_source src;
auto t = spawn(worker(src.get_token()));
// ...
src.request_stop();      // 请求停止: 不抛异常, 由 worker 自己决定何时退出
```

与异常模式的对比：

| | CancelledError 异常注入 | stop_token 轮询 |
|---|---|---|
| 取消如何传达 | await 点抛异常 | 协程主动查令牌 |
| 清理逻辑 | 栈展开(RAII) | 协程自己写 |
| 取消时机 | 下一个 await 点, 立即 | 下一次检查, 可能延迟 |
| 打断纯计算 | 否 | 可以(每轮循环查一次) |
| 侵入性 | 无(库自动注入) | 有(签名带 stop_token) |
| 异常语义 | 等待者收到 CancelledError | 协程正常返回 |

生产级设计可以**混合**：框架层用异常注入保证"可取消"，需要精确控制的协程再显式携带 stop_token 做优雅停机。

## 9.5 取消的传播：父取消子

结构化并发（第 14 讲）中的核心议题：

```
main_task 被取消
  └─ 它 spawn 的 10 个子任务怎么办?
     Python asyncio.TaskGroup: 父取消 → 全部子任务收到取消
     简单方案: 取消只影响被取消的协程, 子任务由用户手动管理
```

设计要点：取消**只对目标协程生效**是最小实现；"传播取消"需要父子关系登记（这是 TaskGroup 类设施的主要工作）。

## 9.6 小结

1. 取消必须走"注入点"：await 点异常 或 显式轮询；**不能**直接 destroy 帧。
2. 异常注入 = 栈展开清理 + 等待者通知 + 循环可终止。
3. `await_transform` 是实现注入的标准工具。
4. 挂起中的协程必须被强制唤醒，取消才生效。
5. `stop_token` 适合"协作式优雅停机"，异常模式适合"强制取消"。

## 思考题

1. 为什么 `cancel()` 里对"挂起中"的协程要 schedule 它？如果只置标志不唤醒，会发生什么？
2. 协程体 `catch (const CancelledError&)` 之后不重新抛出，会怎样？（提示：取消被"吞掉"，任务正常完成——Python 里这是合法的"取消保护"）
3. 用 `stop_token` 改写 9.3 的 `slow_work`，让它优雅停止。两种方案的用户体验差异是什么？
