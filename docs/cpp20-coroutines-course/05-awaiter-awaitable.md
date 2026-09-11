# 第 5 讲：awaiter 与 awaitable

## 核心要点

- **awaitable**：能被 `co_await` 的东西（实现三件套，或提供 `operator co_await`）。
- **awaiter**：三件套的载体——`await_ready` / `await_suspend` / `await_resume`。
- `await_suspend` 的返回值有三种形态，对应三种截然不同的控制流：`void`（挂起）、`bool`（false 立即继续）、`coroutine_handle`（对称传输）。
- `await_suspend(h)` 收到的 `h` 是**被挂起的协程自己**——拿到它才能"到点了叫醒它"。
- awaiter 临时对象住在**协程帧**里，它的析构时机需要仔细对待（第 14 讲）。

---

## 5.1 术语澄清：awaitable vs awaiter

这两个词经常混用，先精确区分：

```cpp
struct awaitable {              // "可以被 co_await"
    awaiter operator co_await() const;   // 方法一: 转换出一个 awaiter
};

struct awaiter {                // "真正执行挂起协议"
    bool await_ready() const;
    void await_suspend(std::coroutine_handle<>) const;
    void await_resume() const;
};
```

- **awaitable** 是外层概念：`co_await` 的操作数。
- **awaiter** 是执行协议的对象：三件套的提供者。
- 一个类型可以同时是两者（大多数库的类型都直接实现三件套，跳过 `operator co_await`）。

`co_await expr` 的完整求值顺序：

```
1. 求值 expr
2. promise.await_transform(expr)      ← 若 promise 定义了 (第 4 讲)
3. expr.operator co_await()           ← 若类型定义了
4. 得到 awaiter 对象 a (存入协程帧)
5. a.await_ready() ?
     ├─ true  → 跳至 8
     └─ false → 6
6. a.await_suspend(handle_of_this_coroutine)
     ├─ 返回 void              → 7
     ├─ 返回 bool              → true 则 7, false 则直接 8
     └─ 返回 coroutine_handle  → 挂起, 并立即 resume 那个句柄 (对称传输)
7. 协程挂起, 控制权交还外部; 未来某刻被 resume
8. a.await_resume() 的返回值 = 整个 co_await 表达式的值
```

## 5.2 三件套逐个拆解

### `await_ready()`：快速路径

```cpp
bool await_ready() const {
    return future_.is_ready();   // 已经就绪 → true
}
```

- 返回 `true`：**不挂起**，直接 `await_resume()`。这是"快速路径"——比如 Future 已提前完成。
- 返回 `false`：进入挂起流程。
- 性能敏感场景里，`await_ready` 是省掉一次挂起/恢复往返的关键。

### `await_suspend(handle)`：挂起后的事

```cpp
void await_suspend(std::coroutine_handle<> h) {
    timer_.start(h);   // 把"被挂起协程的句柄"交给定时器/IO/事件源
}
```

- 参数 `h` 是**当前协程**的句柄（类型擦除版）。
- 此函数在**协程已处于"半挂起"状态**时执行——典型动作：把 `h` 注册到某个等待队列/定时器/操作系统 IO 上。
- 它返回时，控制权就交出去了。

### `await_resume()`：恢复后的收尾

```cpp
T await_resume() {
    return future_.take_result();   // 恢复时拿结果; 可抛异常
}
```

- 返回值就是 `co_await` 表达式的值。
- 这里抛出的异常会**直接进入协程体**（就像在 `co_await` 那一行抛出一样）——这是把异常"注入"协程的标准位置。

## 5.3 三种 `await_suspend` 返回值

这是本讲的核心，用三个场景讲透。

### 形态一：`void` — 无条件挂起

```cpp
void await_suspend(std::coroutine_handle<> h) {
    waiters_.push_back(h);        // 挂起, 等人唤醒
}
```

最常见形态。协程挂起后，未来某个时刻由外部调用 `h.resume()` 恢复。

### 形态二：`bool` — 有条件挂起

```cpp
bool await_suspend(std::coroutine_handle<> h) {
    if (try_lock()) {
        return false;             // 竞态中拿到了资源: 别挂起, 继续执行
    }
    waiters_.push_back(h);
    return true;                  // 挂起
}
```

`await_suspend` 执行期间状态可能发生变化（尤其多线程环境），返回 `false` 可以"反悔"，让协程不挂起直接继续。**经典用途**：修复 lost-wakeup 竞态——在 `await_ready()` 返回 false 之后、挂起之前，事件恰好完成了。

### 形态三：`coroutine_handle` — 对称传输

```cpp
std::coroutine_handle<> await_suspend(std::coroutine_handle<> h) {
    return next_;                  // 挂起我, 直接恢复 next_ (不经过任何调度器)
}
```

返回一个句柄意味着：**当前协程挂起，控制权不还给调用者，而是直接转移到另一个协程**。这就是"对称传输"（symmetric transfer），第 6 讲详述。

## 5.4 实战一：手写一个"可等待的秒表"

```cpp
// 编译: g++ -std=c++20 sleep_awaiter.cpp -pthread && ./a.out
#include <coroutine>
#include <iostream>
#include <thread>
#include <chrono>

struct Task {
    struct promise_type {
        Task get_return_object() {
            return Task{std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        std::suspend_never initial_suspend() { return {}; }
        std::suspend_always final_suspend() noexcept { return {}; }
        void return_void() {}
        void unhandled_exception() { std::terminate(); }
    };
    using H = std::coroutine_handle<promise_type>;
    H h_;
    explicit Task(H h) : h_(h) {}
    Task(Task&& o) noexcept : h_(o.h_) { o.h_ = nullptr; }
    ~Task() { if (h_) h_.destroy(); }
    void resume() { if (h_ && !h_.done()) h_.resume(); }
    bool done() const { return !h_ || h_.done(); }
};

// ── awaiter: 让 co_await 睡上一会 ──
struct sleep_ms {
    int ms;
    bool await_ready() const { return false; }        // 总是挂起

    void await_suspend(std::coroutine_handle<> h) {
        // 教学版: 开线程定时唤醒; 生产版交给事件循环/线程池 (第 6、12 讲)
        std::thread([h, ms = ms] {
            std::this_thread::sleep_for(std::chrono::milliseconds(ms));
            h.resume();                                // 到点叫醒
        }).detach();
    }

    void await_resume() const {}
};

Task demo() {
    std::cout << "A\n";
    co_await sleep_ms{100};
    std::cout << "B (100ms 后)\n";
}

int main() {
    Task t = demo();                 // 急切启动: 打印 A 后挂在 sleep_ms 上
    // 注意: 挂起期间绝不能 resume 它! 主循环只能等待其完成
    while (!t.done())                // done() 为 false 说明还挂着
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    // 100ms 后后台线程 resume → 协程打印 B → final_suspend 挂起 → done() == true
}
```

> 这个例子每挂一次就开一个线程，只是教学演示。真正的生产方案是"一个事件循环统一等待"，见第 6 讲和第 12 讲。
> 注意主循环的写法：协程挂在 `sleep_ms` 上时，再次 `resume()` 它会破坏挂起点状态（UB）。驱动挂起中的协程只能"等"。

## 5.5 实战二：可等待的 Future（await_ready 快速路径）

```cpp
// 编译: g++ -std=c++20 future_demo.cpp && ./a.out
#include <coroutine>
#include <iostream>
#include <optional>

template <typename T>
struct Future {
    // 共享状态: 生产者在别处 set_value
    struct State { std::optional<T> value; bool ready = false; };
    std::shared_ptr<State> st_ = std::make_shared<State>();

    // awaiter 三件套 —— Future 自身就是 awaiter
    bool await_ready() const { return st_->ready; }          // 已就绪: 快速路径

    void await_suspend(std::coroutine_handle<> h) {
        st_->waiter = h;   // 简化: 单等待者
    }

    T await_resume() { return std::move(*st_->value); }
    // (完整版需要处理异常与多等待者, 见第 7、10 讲)
};

// 配合的 Task 略, 参见 5.4; 使用方式:
//   Future<int> f;
//   producer(f, 42);            // 某处 set_value
//   int x = co_await f;         // 就绪则零挂起
```

要点：`await_ready()` 让"结果已提前就绪"的等待零成本——这是事件驱动框架里 Future 设计的关键优化点。

## 5.6 awaiter 的生命周期与 const 细节

- awaiter 对象在 `co_await` 表达式处构造，**存放在协程帧**里（它跨越挂起存活）。
- 协程挂起时 awaiter 必须可访问——它的析构发生在表达式完整结束之后（第 14 讲讨论析构时机的坑）。
- 三件套通常都是 `const` 成员函数：编译器在 `const` 的 awaiter 对象上调用它们。如果你需要修改成员，把它们声明为非 const 或使用 `mutable`。

## 5.7 小结

1. `co_await` = 求值 → `await_transform` → `operator co_await` → 三件套。
2. `await_ready` = 快速路径；`await_suspend` = 注册唤醒；`await_resume` = 取结果/抛异常。
3. `await_suspend` 三种返回 = 三种控制流（挂起 / 反悔继续 / 对称传输）。
4. `await_suspend` 的参数是**被挂起的协程自己**的句柄。
5. awaiter 住在帧里，跨挂起存活。

## 思考题

1. 在 5.4 的 `sleep_ms` 中，如果 `await_ready()` 返回 `true` 会发生什么？（不挂起、不 sleep、直接继续——但 `await_resume` 是空操作，所以等价于无等待）
2. `await_suspend` 返回 `bool` 的"反悔"能力，在多线程场景解决什么问题？试着写出"条件变量等待 + 已通知"的竞态例子。
3. 为什么说 awaiter 必须"跨挂起存活"？如果把它设计成栈上临时对象会怎样？
