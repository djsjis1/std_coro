# 第 4 讲：promise_type 深入

## 核心要点

- `promise_type` 是协程的**控制面板**：编译器在固定时机调用它的成员函数，你可以通过这些时机定制协程的一切行为。
- `promise_type` 的完整接口共 8 个成员，其中 5 个是"主干"（本讲逐一深挖），3 个是"扩展"（`yield_value`、`await_transform`、`operator new` 相关）。
- **promise 与协程同生共死**：它构造于帧创建时，析构于帧销毁前（最后一个）。
- 理解 promise 的关键是记住**每个成员被调用的确切时刻**——这是排查协程问题的第一现场。

---

## 4.1 完整接口清单

```cpp
struct my_promise_type {
    // ── 主干 5 件套 ──
    my_return_type get_return_object();      // ① 创建返回对象
    awaiter initial_suspend();               // ② 协程体前: 是否先挂起
    awaiter final_suspend() noexcept;        // ③ 协程体后: 是否挂起保留帧
    void return_void();                      // ④ co_return; (与 return_value 二选一)
    void return_value(T value);              // ④' co_return value;
    void unhandled_exception();              // ⑤ 异常逃逸时

    // ── 扩展 3 件套 ──
    awaiter yield_value(T value);            // ⑥ co_yield value;
    template<class A> A await_transform(A a); // ⑦ 改写每个 co_await
    static void* operator new(size_t size);  // ⑧ 定制帧分配
    static void operator delete(void*, size_t);
};
```

本讲先讲主干 5 件套 + `await_transform`（第 5 讲配合 awaiter 一起深挖）；`yield_value` 在第 8 讲；帧分配定制在第 13 讲。

## 4.2 完整生命周期时间线

把第 2 讲的改写模型展开为一张时间线图。设协程 `f()` 返回 `Task`：

```
时刻            发生的事                                      promise 的谁
─────────────────────────────────────────────────────────────────────────
T0  调用 f()     分配帧; 参数拷进帧; promise 构造(默认构造)     (构造函数)
T1              get_return_object() → Task 对象给调用者        ①
T2              initial_suspend()                             ②
                  ├─ 挂起 → f() 返回, 协程体等待外部 resume
                  └─ 不挂起 → 立即进入 T3
T3  协程体执行   用户代码逐行执行
T4  遇到 co_await 先 await_transform(操作数)                  ⑦ (若定义)
                  再按 awaiter 协议挂起/恢复 (第 5 讲)
T5  遇到 co_return v;                                        ④ 或 ④'
T6  或抛异常      异常逃逸 → unhandled_exception()             ⑤
T7  协程体结束    final_suspend()                              ③
                  ├─ 挂起 → 帧保留, 外部观察/销毁
                  └─ 不挂起 → 编译器自动销毁帧 → 帧内对象析构 → promise 析构(最后)
```

## 4.3 `get_return_object()`：返回对象的唯一起点

**调用次数：恰好一次，在协程体执行之前。**

它的任务：把"能控制这个协程的东西"交给调用者。最典型实现：

```cpp
Task get_return_object() {
    return Task{std::coroutine_handle<promise_type>::from_promise(*this)};
}
```

值得注意的细节：

- `from_promise(*this)` 之所以可行，是因为 promise 就在帧里，编译器知道帧地址。
- **此时协程体还没执行**。如果返回对象是空的（第 2 讲的 `pause{}`），调用者拿到的就是"联系不上的协程"。
- 返回值可以不是"外壳"本身——协议只要求返回类型构造出**协程的返回类型**的对象。例如你可以返回一个 `shared_ptr<Task>`，让生命周期自动管理。

## 4.4 `initial_suspend()`：惰性 vs 急切

**调用次数：恰好一次，在协程体执行之前。**

```cpp
// 惰性 (lazy): 创建后停在原地, 由外部 co_await / start() 启动
std::suspend_always initial_suspend() { return {}; }

// 急切 (eager): 创建后立刻执行, 直到第一个真正的挂起点
std::suspend_never initial_suspend() { return {}; }
```

选择惰性还是急切，是协程库设计的第一个分叉：

| | 惰性启动 | 急切启动 |
|---|---|---|
| 行为 | 创建 ≠ 执行 | 创建 = 开始执行 |
| 类比 | Python `async def`（调用只创建协程对象） | 立即返回的"已启动任务" |
| 优点 | 调用者可先创建后决定启动时机；支持"先 spawn 再 await"的并发模式 | 不需要显式启动步骤；不会"忘了启动" |
| 代价 | 忘了启动/等待的协程会静默丢弃 | 返回值构造前协程已在跑（递归/重入风险） |

> 补充：`initial_suspend` 返回的也可以是自定义 awaiter（第 5 讲），标准库的 `suspend_always`/`suspend_never` 只是两个现成工具。

## 4.5 `return_value` / `return_void`：结果的出口

**调用次数：至多一次。** `co_return v;` 调用 `return_value(v)`；`co_return;`（或函数自然走完）调用 `return_void()`。

一个 promise 只能定义二者之一（除非 `return_value` 是模板且参数能匹配 void——不推荐玩这个）。

```cpp
// 有值协程
template <typename T>
void return_value(T&& value) {
    result_ = std::forward<T>(value);     // 结果暂存在 promise 里
}

// void 协程
void return_void() noexcept {}
```

**注意**：结果暂存处（promise）的寿命与帧相同。所以库实现通常让"外壳对象"持有真正的结果存储，`return_value` 只做搬运的第一棒（第 7 讲详述）。

## 4.6 `unhandled_exception()`：异常的必经之路

**调用次数：至多一次。** 协程体内任何异常逃逸（没被协程体自己 catch）都会先经过这里。

```cpp
void unhandled_exception() {
    exception_ = std::current_exception();   // 捕获当前异常, 存起来
}
```

三种常见策略：

| 策略 | 代码 | 后果 |
|---|---|---|
| 终止进程 | `std::terminate();` | 教学版最简；异常不可恢复 |
| 存储后转发 | `exception_ = std::current_exception();` | 等待者在 `await_resume` 时重新抛出（生产标准做法，第 10 讲） |
| 吞掉 | 空函数体 | 危险：异常静默消失，极难排查 |

**重要**：异常逃逸到 `unhandled_exception` 后，协程体**不再继续执行**，直接进入 `final_suspend` 流程。所以"异常路径"与"正常返回路径"在时间线上是合并的。

## 4.7 `final_suspend()`：帧销毁权交接点

**调用次数：恰好一次，协程体结束后（无论正常还是异常）。**

这是最容易被低估的接口。它决定**谁、在什么时候销毁帧**：

```cpp
// 方案 A: 不挂起 → 编译器立即销毁帧 (最省心, 但销毁后句柄作废)
std::suspend_never final_suspend() noexcept { return {}; }

// 方案 B: 挂起 → 帧保留, 外部必须在 done() 后调用 destroy()
std::suspend_always final_suspend() noexcept { return {}; }
```

方案 B 存在的三个理由：

1. **观察完成状态**：外部通过 `done()` 得知协程结束（否则帧已销毁，`done()` 是 UB）。
2. **安全搬运结果**：`final_suspend` 挂起时 promise 还活着，可以在这个窗口把结果移交给"外壳"。
3. **对称传输**：`await_suspend` 返回 `coroutine_handle` 时，协程在 final_suspend 挂起状态下被外部接管（第 6 讲）。

一个容易混淆的点：**`final_suspend` 返回 `suspend_always` 后，帧不会自动释放**。谁最后持有句柄，谁负责 `destroy()`。忘了 destroy 就是内存泄漏。

## 4.8 `await_transform()`：协程的"中间人"

**调用次数：每次 `co_await 表达式` 求值时。** 它是 promise 的成员，所以能拿到 promise 的全部状态（这也是"协程级取消注入"的实现基础，见第 9 讲）。

```cpp
// 原样透传: 什么都不改
template <typename A>
A&& await_transform(A&& a) { return std::forward<A>(a); }

// 改写: 给每次 co_await 加一层"可取消包装"
template <typename A>
auto await_transform(A&& a) {
    return make_cancellable(std::forward<A>(a), this);
}
```

`co_await x` 的实际语义（含顺序）：

```
1. 求值 x
2. 若 promise 定义了 await_transform → 用 await_transform(x) 的结果替换 x
3. 若 x 定义了 operator co_await → 调用它再替换
4. 对最终对象执行 awaiter 三件套
```

> `await_transform` 只影响**协程体内的** `co_await`；`initial_suspend`/`final_suspend` 返回的 awaiter 不经过它。

## 4.9 一个观察 promise 调用时机的教学示例

给每个成员加打印，亲眼验证时间线：

```cpp
// 编译: g++ -std=c++20 promise_trace.cpp && ./a.out
#include <coroutine>
#include <iostream>

struct Task {
    struct promise_type {
        Task get_return_object() {
            std::cout << "[promise] get_return_object\n";
            return Task{std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        std::suspend_always initial_suspend() {
            std::cout << "[promise] initial_suspend (挂起)\n";
            return {};
        }
        std::suspend_always final_suspend() noexcept {
            std::cout << "[promise] final_suspend (挂起)\n";
            return {};
        }
        void return_void() { std::cout << "[promise] return_void\n"; }
        void unhandled_exception() { std::cout << "[promise] unhandled_exception\n"; }
    };
    using H = std::coroutine_handle<promise_type>;
    H h_;
    explicit Task(H h) : h_(h) {}
    ~Task() { if (h_) h_.destroy(); }
    void resume() { if (h_ && !h_.done()) h_.resume(); }
    bool done() const { return !h_ || h_.done(); }
};

Task demo() {
    std::cout << "  [协程体] 执行中\n";
    co_return;
}

int main() {
    std::cout << "=== 创建 ===\n";
    Task t = demo();
    std::cout << "=== 恢复 ===\n";
    t.resume();
    std::cout << "=== done = " << t.done() << " ===\n";
}
```

输出（注意 final_suspend 挂起，帧由 Task 析构销毁）：

```
=== 创建 ===
[promise] get_return_object
[promise] initial_suspend (挂起)
=== 恢复 ===
  [协程体] 执行中
[promise] return_void
[promise] final_suspend (挂起)
=== done = 1 ===
```

## 4.10 小结

1. promise 是"编译时机 → 行为"的映射表；每个成员都有**固定且唯一**的调用时刻。
2. `initial_suspend` 决定惰性/急切启动——库设计的第一个分叉。
3. `final_suspend` 决定帧销毁权——`suspend_never` 自动销毁，`suspend_always` 由外部销毁。
4. 异常必过 `unhandled_exception`；结果必过 `return_value`/`return_void`。
5. `await_transform` 是拦截所有 `co_await` 的中间人，取消、注入、计时等都靠它。

## 思考题

1. 4.9 示例中，把 `final_suspend` 改成 `suspend_never`，输出序列会怎样变化？`done()` 还能安全调用吗？
2. 为什么 `unhandled_exception` 之后协程体不会再执行？从状态机角度解释。
3. 设计题：如果你想实现"协程开始前打印日志、结束后打印日志"，不改协程体，你会在 promise 的哪些成员里做？
