# 第 10 讲：错误处理与异常传播

## 核心要点

- 协程内的异常**不会**直接飞到等待者：先经过 `unhandled_exception()` 存储，等待者在 `await_resume()` 时重新抛出。
- `std::exception_ptr` 是异常在协程世界里的"行李"：捕获、存储、传递、稍后重抛。
- **未检索的异常 = 静默丢失**：fire-and-forget 协程的异常必须有报告渠道（全局回调/日志）。
- 并发聚合（gather 多个任务）的异常策略需要显式设计：先失败即取消？全部完成再抛第一个？
- 异常穿过挂起点的过程是**无缝**的——这是协程相对回调的又一大优势。

---

## 10.1 异常在协程中的完整旅程

```cpp
Task<int> producer() {
    throw std::runtime_error("boom");     // ① 协程体内抛出
    co_return 1;
}

Task<int> consumer() {
    try {
        int x = co_await producer();      // ⑤ 异常在这里被重新抛出
    } catch (const std::runtime_error& e) {
        std::cout << "caught: " << e.what();   // ← 像同步代码一样处理
    }
    co_return 0;
}
```

时间线：

```
① 协程体 throw
② 栈展开 (协程体内局部对象析构) —— 但没有东西 catch
③ promise.unhandled_exception():
      exception_ = std::current_exception();   // 捕获并存储
④ 协程以异常结束 → final_suspend → 结果(异常)移交外壳
⑤ 等待者 await_resume():
      std::rethrow_exception(exception_);      // 重新抛出
⑥ 等待者的 try/catch 接住 —— 就像异常从未离开过协程
```

**无缝之处**：步骤 ② 的栈展开在**协程自己的帧**里正常进行——RAII 对象该析构析构、锁该释放释放。这是协程异常处理比"回调 + 错误码"优雅的根本原因。

## 10.2 `std::exception_ptr`：异常的行李箱

协程挂起时，栈上"正在传播的异常"无处存放，所以必须装箱：

```cpp
std::exception_ptr ep;

// 捕获当前异常 → 装箱
try { risky(); }
catch (...) { ep = std::current_exception(); }

// 检查是否有内容
if (ep) { /* 有异常 */ }

// 重新抛出 (任何线程、任何时刻)
std::rethrow_exception(ep);

// 凭空制造一个
ep = std::make_exception_ptr(std::runtime_error("later"));
```

关键特性：**装箱的异常可以跨线程、跨挂起、跨任意时间传递**。整个协程异常机制就是"在 promise 里放一个 `exception_ptr`"。

## 10.3 三种异常策略

### 策略一：终止（教学）

```cpp
void unhandled_exception() { std::terminate(); }
```

异常无法被捕获，程序直接崩溃。**只用于教学或明确"协程体内不允许异常"的场景。**

### 策略二：存储转发（标准）

```cpp
void unhandled_exception() { exception_ = std::current_exception(); }
// 等待者 await_resume(): if (exception_) rethrow;
```

第 7 讲的 Task 就是这一策略。等待者不 await 时异常被"冷藏在行李箱里"。

### 策略三：立即报告（fire-and-forget）

```cpp
void unhandled_exception() {
    if (task_ == nullptr) {                    // 被 detach: 没人会来取
        report_unhandled(std::current_exception());   // 全局回调: 日志/统计
    } else {
        exception_ = std::current_exception();  // 有人等: 正常转发
    }
}
```

fire-and-forget 协程的异常**永远不会被检索**，必须主动报告。Python 的 `Task exception was never retrieved` 警告、Node.js 的 `unhandledRejection` 都是同一诉求。设计要点：给用户一个可替换的全局回调。

## 10.4 异常与取消的交互

`CancelledError`（第 9 讲）也是一种异常，但它有特殊语义：

```cpp
try { co_await task; }
catch (const CancelledError&) {
    // 已知的取消, 不是 bug → 通常安静处理或收尾
}
catch (const std::exception& e) {
    // 真实错误 → 记录日志
}
```

所以异常类型层级建议：

```
std::runtime_error
  ├── CancelledError    ← 取消专用: 报告系统可以过滤它
  ├── TimeoutError      ← 超时专用
  └── (用户异常)
```

## 10.5 并发聚合的异常策略

`gather(a, b, c)` 三个任务，两个抛异常——等待者收到什么？这是库必须明示的语义。常见三种：

| 策略 | 行为 | 类比 |
|---|---|---|
| **先完成者优先** | 第一个异常立即传播，其余任务继续跑 | — |
| **全部完成再抛第一个** | 等待所有任务结束，抛第一个异常（其余丢弃或记录） | Python `asyncio.gather` 默认 |
| **全部完成再抛聚合** | 所有异常打包成一个 ExceptionGroup | Python 3.11+ TaskGroup |

实现要点（以"全部完成再抛第一个"为例）：

```cpp
// 每个子任务挂一个监控协程:
Task<void> monitor(Task<T> t, shared_state& st) {
    try {
        st.results[i] = co_await std::move(t);
    } catch (...) {
        if (!st.first_exception)              // 只记第一个
            st.first_exception = std::current_exception();
    }
    if (--st.remaining == 0)
        st.notify_waiter();                   // 全部结束才唤醒 gather 调用者
}

// gather 调用者恢复后:
if (st.first_exception)
    std::rethrow_exception(st.first_exception);
```

> 注意：聚合场景下"子任务继续跑完"意味着异常不中断并发——这通常更安全（不会留下半截任务）。

## 10.6 常见错误与规避

### 错误一：在 `unhandled_exception` 里吞异常

```cpp
void unhandled_exception() {}     // ❌ 异常静默消失
```

调试时表现为"程序行为诡异但不报错"。除非有明确的报告渠道（策略三），否则不要空实现。

### 错误二：析构函数抛异常

协程帧销毁发生在 `final_suspend` 之后；若帧内对象的析构函数抛异常且没被捕获，行为是未定义的（很可能 `terminate`）。

### 错误三：异常后访问部分完成状态

`gather` 抛了第一个异常，但其它任务可能半途而废——调用者 catch 之后不能假设整体状态一致（要么全部完成，要么明确文档"其他任务继续运行"）。

### 错误四：`throw` 之后没有 `co_return`

协程体内用 `throw` 提前结束时，**必须**在 `throw` 之后补一个 `co_return;`：

```cpp
Task<int> failing() {
    throw std::runtime_error("boom");
    co_return 0;    // ✅ 需要补上 (即使永远执行不到)
}
```

实测：MSVC Debug 下若缺少该 `co_return`，异常会**绕过 `unhandled_exception` 直接逃逸**（协程体异常像普通函数一样传播，等待者收不到），GCC/Clang 则无此问题。这是编译器对协程状态机"异常路径的终止点"的处理差异，补一个 `co_return` 是零成本且跨编译器的防御性写法。

## 10.7 完整示例：异常穿过挂起

```cpp
// 编译: g++ -std=c++20 exception_flow.cpp && ./a.out
#include <coroutine>
#include <exception>
#include <iostream>

struct Task {
    struct promise_type {
        std::exception_ptr exception_;

        Task get_return_object() {
            return Task{std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        std::suspend_never initial_suspend() { return {}; }
        std::suspend_always final_suspend() noexcept { return {}; }
        void return_void() {}
        void unhandled_exception() { exception_ = std::current_exception(); }

        struct final_awaiter {
            bool await_ready() noexcept { return false; }
            void await_suspend(std::coroutine_handle<promise_type> h) noexcept {
                // 教学简化: 只打印; 生产版在此唤醒等待者
                std::cout << "  [final] 协程结束, 异常="
                          << (h.promise().exception_ ? "有" : "无") << "\n";
            }
            void await_resume() noexcept {}
        };
        final_awaiter final_suspend() noexcept { return {}; }
    };
    using H = std::coroutine_handle<promise_type>;
    H h_;
    explicit Task(H h) : h_(h) {}
    ~Task() { if (h_) h_.destroy(); }
    void resume() { if (h_ && !h_.done()) h_.resume(); }
    bool done() const { return !h_ || h_.done(); }
    void rethrow_if_exception() {
        if (h_ && h_.promise().exception_)
            std::rethrow_exception(h_.promise().exception_);
    }
};

Task failing() {
    struct Guard { ~Guard() { std::cout << "  [Guard] 析构 (清理执行了)\n"; } } g;
    std::cout << "  [协程] 即将抛异常\n";
    throw std::runtime_error("boom");
    co_return;   // 注意: throw 之后要补 co_return!
                 // MSVC Debug 下缺了它, 异常会绕过 unhandled_exception 直接逃逸 (见 10.6)
}

int main() {
    Task t = failing();                 // 急切启动, 挂起在 final_suspend
    try {
        t.rethrow_if_exception();       // 等待者取结果: 异常在这里重抛
    } catch (const std::exception& e) {
        std::cout << "[main] 捕获: " << e.what() << "\n";
    }
}
```

输出：

```
  [协程] 即将抛异常
  [Guard] 析构 (清理执行了)
  [final] 协程结束, 异常=有
[main] 捕获: boom
```

注意 `[Guard]` 的输出位置：**异常展开时清理已执行**，而异常本身"迟到了"——先存起来，等待者取结果时才重抛。

## 10.8 小结

1. 异常路径：协程体 throw → 栈展开 → `unhandled_exception` 存储 → `await_resume` 重抛。
2. `exception_ptr` 是跨挂起/跨线程传递异常的标准容器。
3. fire-and-forget 异常必须主动报告（全局回调）。
4. 聚合并发需要明确异常策略（先抛 / 全完成后抛 / ExceptionGroup）。
5. `CancelledError` 应独立成类，便于报告系统过滤。

## 思考题

1. 为什么说"协程的异常处理比回调优雅"？从清理逻辑和错误传递两个角度回答。
2. `unhandled_exception()` 里只存了 `exception_ptr` 而没有立即处理。等待者始终不来取，异常会怎样？如何防御？
3. 设计一个 `gather` 的"全部完成再抛第一个异常"语义：三个任务 A、B、C，A 抛异常时 B、C 是否继续运行？等待者何时恢复？
