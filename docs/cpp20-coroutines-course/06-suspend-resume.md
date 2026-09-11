# 第 6 讲：挂起与恢复——对称传输与调度模型

## 核心要点

- 协程的控制权转移有两种模型：**不对称传输**（挂起 → 回到调用者/调度器 → 再被恢复）与**对称传输**（挂起 → 直接进入另一个协程）。
- `await_suspend` 返回 `coroutine_handle` 就是对称传输：**不经过任何调度器**，把控制权直接交给目标协程。
- 对称传输避免了"恢复往返"的栈深度积累，是高性能协程库的关键技术。
- 一个最小**调度器**只需要：一个就绪队列 + `schedule(handle)` + 一个循环。
- 单线程事件循环 = 调度器 + 定时器 + IO 等待的融合（第 11、12 讲的基石）。

---

## 6.1 挂起/恢复的本质

"协程挂起"精确地说发生了什么？

```
协程 A 执行中
  → 遇到 co_await X
  → X.await_suspend(A_handle) 执行完毕
  → A 的控制流"冻结": 局部变量在帧里, 状态机记录着恢复点
  → 函数返回 (回到调用者 / 调度器 / 另一个协程)
```

"恢复"则是：

```
某处调用 A_handle.resume()
  → 状态机跳转到恢复点
  → X.await_resume() 执行
  → A 继续向下执行
```

注意：`resume()` 是**同步函数**——它一直执行到协程再次挂起或结束才返回。所以单线程下，"并发"的本质是：每个协程**每次只跑一小段**，主动让出。

## 6.2 不对称传输（asymmetric transfer）

模型：**控制权总是先回到"上一级"（调用者或调度器），再由它决定接下来跑谁。**

```
调度器               协程 A               协程 B
   │ schedule(A)        │                    │
   ├───────────────────>│                    │
   │                    │ co_await X → 挂起   │
   │<───────────────────┘                    │
   │ schedule(B)                             │
   ├────────────────────────────────────────>│
   │                    │                    │ co_await Y → 挂起
   │<────────────────────────────────────────┘
   │ ... A 的 X 就绪 → schedule(A) ...       │
   ├───────────────────>│                    │
```

特点：调度器是**唯一的控制权枢纽**。所有协程都通过它中转。这是最简单、最易推理的模型——第 3 讲的手动驱动、大多数教学实现都是这个模型。

## 6.3 对称传输（symmetric transfer）

模型：**协程 A 挂起时，直接把控制权交给协程 B**，不回调度器。

```cpp
struct transfer_to {
    std::coroutine_handle<> target;
    bool await_ready() const noexcept { return false; }
    std::coroutine_handle<> await_suspend(std::coroutine_handle<>) const noexcept {
        return target;               // ← 关键: 返回目标句柄
    }
    void await_resume() const noexcept {}
};
```

`co_await transfer_to{b};` 的效果：A 挂起，**B 立刻开始执行**。控制流是 `A → B → C → ...` 一条链，直到某个协程用不对称方式挂起（返回调度器）。

图示：

```
调度器 ──schedule──> A ──对称──> B ──对称──> C ──不对称挂起──> 回到调度器
                    (A 直接叫 B, 不经调度器)
```

### 为什么需要对称传输？

**栈深度问题**。想象 A 完成后立即要恢复 B（"A 干完了，接着跑等它的 B"）。不对称模型下：

```
调度器 resume(B) → B 的 await_resume → B 继续 → ... → B 完成 → 恢复 C
```

如果 B 完成后又立即恢复 C、C 完成后又恢复 D……每一次"完成 → 恢复等待者"都要先回到调度器再 resume 下一个。**在对称模型下**，`final_suspend` 返回等待者句柄，完成即切换：

```cpp
// 经典对称传输 final_awaiter (概念版)
struct final_awaiter {
    bool await_ready() const noexcept { return false; }
    std::coroutine_handle<> await_suspend(std::coroutine_handle<> h) const noexcept {
        auto continuation = h.promise().continuation;   // 谁在等我
        if (continuation) return continuation;          // 直接转给它!
        return std::noop_coroutine();                   // 没人等: 回到空协程
    }
    void await_resume() const noexcept {}
};
```

这样"任务链"（task → 等待它的 task → 再等待它的 task）的完成传播是 **O(1) 深度** 的直接跳转，而不是 O(n) 的恢复往返。

### `std::noop_coroutine()`

标准库提供的"空协程"：`resume()` 什么都不做，永不结束（`done()` 恒为 false）。对称传输链的**终点**必须回到某个"不对称点"（调度器或 noop），否则控制权无处可去。`noop_coroutine` 就是最方便的终点。

## 6.4 最小调度器（30 行）

把第 3 讲的手动驱动升级为通用调度器：

```cpp
// 编译: g++ -std=c++20 scheduler.cpp && ./a.out
#include <coroutine>
#include <iostream>
#include <queue>

// ── 全局调度器: 一个就绪队列 ──
struct Scheduler {
    std::queue<std::coroutine_handle<>> ready;

    void schedule(std::coroutine_handle<> h) { ready.push(h); }

    void run() {
        while (!ready.empty()) {
            auto h = ready.front();
            ready.pop();
            if (!h.done()) h.resume();     // 跑一小段, 直到它再次挂起
        }
    }
};
Scheduler g_sched;

// ── 返回类型: 惰性启动 + final_suspend 挂起 (帧由外壳析构销毁) ──
struct Task {
    struct promise_type {
        Task get_return_object() {
            return Task{std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        std::suspend_always initial_suspend() { return {}; }    // 惰性
        std::suspend_always final_suspend() noexcept { return {}; } // 保留帧
        void return_void() {}
        void unhandled_exception() { std::terminate(); }
    };
    using H = std::coroutine_handle<promise_type>;
    H h_;
    explicit Task(H h) : h_(h) {}
    Task(Task&& o) noexcept : h_(o.h_) { o.h_ = nullptr; }
    ~Task() { if (h_) h_.destroy(); }    // final_suspend 挂起 → 这里负责销毁
    void start() { if (h_ && !h_.done()) h_.resume(); }
};

// ── 让出: 把自己放回队尾 (不对称传输) ──
struct yield_now {
    bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<> h) const noexcept {
        g_sched.schedule(h);               // 回调度器, 排到队尾
    }
    void await_resume() const noexcept {}
};

Task task(const char* name, int rounds) {
    for (int i = 0; i < rounds; ++i) {
        std::cout << name;
        co_await yield_now{};
    }
}

int main() {
    Task a = task("A", 3);
    Task b = task("B", 3);
    a.start();                  // 惰性协程: start() 后进入协程体
    b.start();                  // start 里的 resume 会跑第一个 yield → 回队尾
    g_sched.run();              // 交替恢复: A B A B A B
    std::cout << "\n";
}
// 输出: A B A B A B   —— 两个协程交替让出, 单线程实现"并发"
```

> 注意：`final_suspend` 返回 `suspend_always` 是刻意的——帧保留到 Task 析构才销毁。
> 若改成 `suspend_never`，编译器会在协程结束时**自动销毁帧**，随后 Task 析构里的
> `h_.destroy()` 就是双重销毁（UB）。记住：**谁销毁帧，由 final_suspend 决定，只能有一个。**

这个模型就是**单线程事件循环**的雏形：第 11、12 讲把"就绪队列"扩展为"定时器堆 + IO 等待 + 跨线程唤醒"，就是完整的生产级调度器。

## 6.5 两种模型的取舍

| | 不对称传输 | 对称传输 |
|---|---|---|
| 控制流 | 挂起 → 回调度器 → 被恢复 | 挂起 → 直接进下一个协程 |
| 实现复杂度 | 低，易推理 | 较高（要处理链条终点） |
| 性能 | 完成传播 O(n) 往返 | 完成传播 O(1) 跳转 |
| 调试友好度 | 高（每步都经过调度器） | 低（控制流跳跃） |
| 典型场景 | 教学、事件循环主体 | 高性能库的 task 链（folly、cppcoro） |

生产级库通常是**混合**：正常调度用不对称模型（经过事件循环），而"任务完成 → 恢复等待者"这一热点路径用对称传输。

## 6.6 小结

1. 不对称传输 = 经过调度器中转；对称传输 = `await_suspend` 返回目标句柄直接切换。
2. 对称传输的意义：避免完成传播的栈深度/往返开销。
3. `std::noop_coroutine()` 是传输链的安全终点。
4. 最小调度器 = 就绪队列 + `schedule` + 循环 `resume`；事件循环是其超集。

## 思考题

1. 6.4 中把 `yield_now::await_suspend` 改成返回 `h`（对称传输给自己）会怎样？（无限循环——永远不回到调度器）
2. 为什么对称传输链不能无限长？什么机制保证它最终回到调度器或 noop 协程？
3. 在"任务完成 → 恢复等待者"场景，用对称传输替代调度器中转，为什么能提升性能？画图说明。
