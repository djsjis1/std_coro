# 第 7 讲：设计一个实用的 Task 类型

## 核心要点

- 一个"实用"的 `Task<T>` 至少需要：**结果存储**、**异常传播**、**可等待**（Task 等待 Task）、**移动语义**、**RAII 生命周期**。
- 结果的搬运路线：`co_return` → `promise.return_value()`（暂存）→ `final_suspend`（移交外壳）→ `await_resume()`（交给等待者）。
- 同一时间**只能有一个等待者**等待同一个 Task（`continuation` 单槽）；这是无栈协程库的通用简化，多等待者要另做（如 Future，见第 10 讲）。
- `Task<void>` 需要用特化处理（`return_void`、无结果存储）。
- **惰性启动 + 可等待**是异步框架里最舒服的组合：`co_await` 一个 Task 时自动启动它。

---

## 7.1 需求清单

从用户视角出发，一个框架级 `Task<T>` 要满足：

```cpp
Task<int> compute();                 // 1. 协程函数返回 Task<T>
int r = co_await compute();          // 2. co_await 拿到结果
auto t = spawn(compute());           // 3. 后台启动
int r2 = co_await std::move(t);      // 4. 稍后再等待
Task<> background();                 // 5. void 版本
try { co_await compute(); }          // 6. 异常传播给等待者
catch (const std::exception&) {}
```

对应到实现上的要求：

| 用户需求 | 实现机制 |
|---|---|
| 1、5 | 主模板 + `Task<void>` 特化 |
| 2、3、4 | `await_ready/await_suspend/await_resume` + 惰性启动（`started_` 标志） |
| 6 | promise 存 `exception_ptr`，`await_resume` 重新抛出 |

## 7.2 结果搬运的三棒接力

关键设计：`co_return 42` 的 42 需要穿越三个对象，最终到达等待者手里。

```
co_return 42;
    │ 第 1 棒: promise.return_value(42)
    ▼
promise.result_  (variant/optional 暂存)   ← promise 住在帧里, 帧销毁前必须搬走
    │ 第 2 棒: final_suspend 里 store_result()
    ▼
Task.result_    (optional<T>, 外壳对象)     ← 外壳比帧活得久
    │ 第 3 棒: await_resume() return std::move(*result_)
    ▼
等待者拿到了 42
```

为什么不能省掉第 2 棒？因为 `final_suspend` 之后（尤其返回 `suspend_never` 时）**帧被销毁**，promise 随之消失。结果必须在此之前搬进"外壳"。

## 7.3 完整实现（教学级）

```cpp
// 编译: g++ -std=c++20 task_full.cpp && ./a.out
#include <cassert>
#include <coroutine>
#include <exception>
#include <iostream>
#include <optional>
#include <variant>

// ── 取消/超时异常类型 (第 9、10 讲展开) ──
struct CancelledError : std::runtime_error {
    CancelledError() : std::runtime_error("cancelled") {}
};

template <typename T = void>
class Task;

// ============ Task<T> 主模板 ============
template <typename T>
class Task {
public:
    struct promise_type {
        Task get_return_object() {
            return Task{std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        std::suspend_always initial_suspend() { return {}; }   // 惰性启动

        // 自定义 final_awaiter: 挂起帧, 在收尾时完成第 2 棒搬运并唤醒等待者
        struct final_awaiter {
            bool await_ready() noexcept { return false; }
            void await_suspend(std::coroutine_handle<promise_type> h) noexcept {
                h.promise().store_result();                 // 第 2 棒: 结果搬进外壳
                if (h.promise().continuation_)
                    h.promise().continuation_.resume();     // 唤醒等待者
                // 教学版直接 resume; 生产版应交给调度器 (第 6 讲) 或对称传输
            }
            void await_resume() noexcept {}
        };
        final_awaiter final_suspend() noexcept { return {}; }

        void unhandled_exception() { exception_ = std::current_exception(); }

        template <typename U>
        void return_value(U&& v) { result_.template emplace<1>(std::forward<U>(v)); }

        // 第 2 棒: 帧销毁前把结果/异常搬进外壳
        void store_result() {
            if (result_.index() == 1)
                task_->result_ = std::move(std::get<1>(result_));
            if (exception_)
                task_->exception_ = std::move(exception_);
            task_->ready_ = true;
        }

        std::variant<std::monostate, T> result_;
        std::exception_ptr exception_;
        std::coroutine_handle<> continuation_;   // 等待者 (单槽)
        Task* task_ = nullptr;                   // 回指外壳
    };

    using H = std::coroutine_handle<promise_type>;

    Task() = default;
    explicit Task(H h) : h_(h) {
        if (h_) h_.promise().task_ = this;       // 建立回指
    }
    ~Task() {
        // final_suspend 挂起 → 帧保留 → 无论完成与否都由外壳销毁
        if (h_) h_.destroy();
    }
    Task(Task&& o) noexcept
        : h_(std::exchange(o.h_, nullptr)), result_(std::move(o.result_)),
          exception_(std::move(o.exception_)), ready_(o.ready_) {
        if (h_) h_.promise().task_ = this;       // 移动后更新回指!
    }
    Task(const Task&) = delete;
    Task& operator=(const Task&) = delete;
    Task& operator=(Task&&) = delete;            // 教学版简化 (生产版应支持)

    // ── Awaitable: 让 Task 可以被 co_await ──
    bool await_ready() const { return ready_; }

    void await_suspend(std::coroutine_handle<> waiter) {
        assert(h_);
        h_.promise().continuation_ = waiter;     // 谁在等我
        if (!started_) {                         // 惰性启动: 第一次 await 才开跑
            started_ = true;
            h_.resume();                         // 教学版直接同步启动 (见注)
        }
    }

    T await_resume() {
        if (exception_) std::rethrow_exception(exception_);
        assert(result_.has_value());
        return std::move(*result_);
    }

    // ── 公开 API ──
    void start() {
        if (!started_ && h_) { started_ = true; h_.resume(); }
    }
    bool done() const { return ready_ || !h_; }
    bool started() const { return started_; }

    /// 非协程上下文取结果 (main 等): 有异常则重新抛出
    T take_result() {
        if (exception_) std::rethrow_exception(exception_);
        assert(result_.has_value());
        return std::move(*result_);
    }

private:
    H h_ = nullptr;
    std::optional<T> result_;
    std::exception_ptr exception_;
    bool ready_ = false;
    bool started_ = false;
};
```

> **注**：教学版在 `await_suspend` 里直接 `h_.resume()` 同步启动子协程。生产版应把句柄交给调度器（第 6 讲），让调度器统一安排执行，避免调用栈增长。

## 7.4 `Task<void>` 特化

`void` 不能存进 `std::optional`，需要特化：

```cpp
template <>
class Task<void> {
public:
    struct promise_type {
        Task get_return_object() {
            return Task{std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        std::suspend_always initial_suspend() { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }
        void unhandled_exception() { exception_ = std::current_exception(); }
        void return_void() {}                    // 注意: return_void 而非 return_value

        void store_result() {
            if (exception_) task_->exception_ = std::move(exception_);
            task_->ready_ = true;
        }

        std::exception_ptr exception_;
        std::coroutine_handle<> continuation_;
        Task* task_ = nullptr;
    };

    using H = std::coroutine_handle<promise_type>;
    // 构造/析构/移动同主模板 (略)
    bool await_ready() const { return ready_; }
    void await_suspend(std::coroutine_handle<> waiter) {
        h_.promise().continuation_ = waiter;
        if (!started_) { started_ = true; h_.resume(); }
    }
    void await_resume() {                        // 返回 void
        if (exception_) std::rethrow_exception(exception_);
    }
    // ...
private:
    H h_ = nullptr;
    std::exception_ptr exception_;
    bool ready_ = false;
    bool started_ = false;
};
```

使用：

```cpp
Task<int> compute() {
    co_return 42;
}
Task<> say() {
    std::cout << "hello\n";
    co_return;
}

Task<int> main_coro() {
    int r = co_await compute();      // 42
    co_await say();                  // hello
    co_return r;
}

int main() {
    // 教学版: 惰性启动后需手动驱动 (完整事件循环见第 6 讲)
    auto t = main_coro();
    t.start();
    while (!t.done()) { /* 等待 (此例同步启动, 主循环立即退出) */ }
    std::cout << "result: " << t.take_result() << "\n";
}
```

> 教学版的 `await_suspend`/`final_awaiter` 里都是**同步 resume**（直接调用），所以整个
> 任务链在 `start()` 里一次跑完，`done()` 立即为 true——不需要真实的事件循环。
> 生产版把这些 resume 换成 `scheduler.schedule(...)` 即可（第 6 讲）。

## 7.5 为什么"单等待者"

细心的读者会问：`continuation_` 只有一个槽，两个协程同时 `co_await` 同一个 Task 怎么办？

- 后设置的 `continuation_` 会**覆盖**前者 → 前一个协程永远不被唤醒（挂死）。
- 这是教学版 Task 的固有简化。生产级方案：
  - **Task**：保持单等待者（文档写明"同一时刻只能被 co_await 一次"）。大多数用法天然满足。
  - **Future/Promise**（第 10 讲）：需要多等待者时用 Future，等待者存 `vector`，完成时全部唤醒。

## 7.6 生命周期全景图

结合第 3、4 讲，画一张 Task 的一生：

```
创建:  Task<int> t = compute();     帧已分配, promise 已构造, initial_suspend 挂起
        │
启动:  co_await t (或 t.start())    started_=true, 协程体开始执行
        │
运行:  ... 可能 co_await 别的 → 挂起 → 被恢复 → 继续 ...
        │
完成:  co_return 42 → return_value → final_suspend(不挂起)
        → store_result: 结果搬进外壳 → continuation 被唤醒 → 帧自动销毁
        │
取用:  等待者 await_resume() 拿到 42
        │
释放:  t 析构: ready_==true, 不再 destroy (帧早已销毁)
```

## 7.7 小结

1. 结果三棒接力：promise 暂存 → final_suspend 移交外壳 → await_resume 交给等待者。
2. 移动构造必须更新 `promise.task_` 回指指针。
3. Task 单等待者（`continuation_` 单槽）是合理的工程简化。
4. `Task<void>` 需要特化。
5. 惰性启动 + 第一次 await 时启动，是框架里最省心的组合。

## 思考题

1. 为什么移动构造后必须更新 `task_` 回指？不更新会发生什么？（提示：final_suspend 把结果写进谁？）
2. 教学版在 `await_suspend` 里同步 `resume()` 子协程有什么隐患？A await B、B await C、C await A 会出现什么？（第 14 讲预告）
3. 若要支持"同一 Task 被多次等待"，你会怎么改？列出设计要点（不用写完整代码）。
