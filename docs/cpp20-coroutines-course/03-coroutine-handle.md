# 第 3 讲：coroutine_handle 与协程帧

## 核心要点

- `std::coroutine_handle<P>` 是**协程的"遥控器"**：一个轻量指针，指到堆上的协程帧。
- 句柄的核心操作只有三个：`resume()`（恢复）、`done()`（是否结束）、`destroy()`（销毁帧）。
- `coroutine_handle<>`（无模板参数）是**类型擦除**版本：只知道"这是一个协程"，不知道它的 promise 类型。
- **所有权规则**：谁最后持有句柄，谁负责在协程结束后调用 `destroy()`；重复 `destroy()` 或 `destroy()` 未完成的协程都是未定义行为。
- 协程帧保存：参数副本、跨挂起点存活的局部变量、promise、当前状态（状态机编号）。

---

## 3.1 句柄的诞生：`from_promise` 与 `from_address`

在第 2 讲里，`get_return_object()` 返回了一个空壳，导致我们"联系不上"协程。句柄就是联系方式。

创建句柄的两种方式：

```cpp
#include <coroutine>

// 方式 1: 从 promise 对象反推句柄 (promise 就在帧里, 编译器知道帧在哪)
std::coroutine_handle<my_promise> h1 =
    std::coroutine_handle<my_promise>::from_promise(promise_ref);

// 方式 2: 从帧地址构造 (跨 ABI 传递协程时用)
void* frame_addr = ...;
auto h2 = std::coroutine_handle<>::from_address(frame_addr);
```

## 3.2 句柄的接口一览

```cpp
template <typename Promise = void>
struct coroutine_handle {
    // ── 构造/观察 ──
    static coroutine_handle from_promise(Promise&);   // promise → 句柄
    static coroutine_handle from_address(void*);      // 地址 → 句柄
    void* address() const noexcept;                   // 句柄 → 帧地址
    explicit operator bool() const noexcept;          // 是否有效
    bool done() const noexcept;                       // 是否已执行完(挂起在 final_suspend)

    // ── 控制 ──
    void resume() const;                              // 恢复执行 (或首次启动)
    void destroy() const noexcept;                    // 销毁帧 (只能对已完成的协程!)

    // ── 访问 promise (仅带模板参数版本) ──
    Promise& promise() const;                         // 帧里的 promise 对象
};
```

注意 `resume()` 是**同步**的：它让协程从现在的位置继续执行，直到协程再次挂起或结束，`resume()` 才会返回。所以：

```cpp
h.resume();   // 可能执行 1 微秒, 也可能执行 10 分钟; 这行返回时协程又停住了
```

## 3.3 第一个"能驱动的协程"：把句柄装进返回对象

把第 2 讲的空壳升级一下：

```cpp
// 编译: g++ -std=c++20 handle.cpp && ./a.out
#include <coroutine>
#include <iostream>

struct my_coro {
    struct promise_type;

    // 返回对象持有一个句柄 (注意类型用 promise_type 版本, 才能访问 promise)
    std::coroutine_handle<promise_type> h_;

    explicit my_coro(std::coroutine_handle<promise_type> h) : h_(h) {}
    ~my_coro() { if (h_) h_.destroy(); }        // RAII: 析构时销毁帧

    void resume() { if (h_ && !h_.done()) h_.resume(); }   // 对外暴露驱动接口
    bool done() const { return !h_ || h_.done(); }

    struct promise_type {
        my_coro get_return_object() {
            // 关键一行: 从 promise 反推句柄, 装进返回对象
            return my_coro{std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        std::suspend_always initial_suspend() { return {}; }
        // 注意: 这里必须用 suspend_always 保留帧。若用 suspend_never,
        // 协程结束后帧被编译器自动销毁, 而循环还在调用 c.done() → UB。
        std::suspend_always final_suspend() noexcept { return {}; }
        void return_void() {}
        void unhandled_exception() {}
    };
};

my_coro counter() {
    for (int i = 1; i <= 3; ++i) {
        std::cout << "  协程产出: " << i << "\n";
        co_await std::suspend_always{};   // 每轮循环后暂停
    }
    co_return;
}

int main() {
    std::cout << "[main] 创建协程\n";
    my_coro c = counter();                // 创建即挂起 (initial_suspend)

    while (!c.done()) {
        std::cout << "[main] 恢复协程...\n";
        c.resume();                       // 同步执行到下一个挂起点
    }
    std::cout << "[main] 协程结束\n";
}
```

输出：

```
[main] 创建协程
[main] 恢复协程...
  协程产出: 1
[main] 恢复协程...
  协程产出: 2
[main] 恢复协程...
  协程产出: 3
[main] 协程结束
```

到这里，你已经完成了**最核心的闭环**：创建 → 挂起 → 恢复 → 再挂起 → …… → 完成 → 销毁。所有复杂的协程库都是这个闭环的包装。

## 3.4 协程帧里有什么

协程暂停时，以下内容必须"有家可归"，它们全部住在协程帧里：

```cpp
task<int> example(int a, const std::string& s) {   // 参数: a 按值复制进帧
    int x = compute(s);                            // 局部变量 x: 跨挂起存活 → 进帧
    std::string tmp;                               // tmp 同样进帧
    co_await something(x);                         // 挂起点
    return x + 1;                                  // co_return
}
```

帧的逻辑布局（顺序是实现细节，各编译器不同）：

```
┌───────────────────────────────┐
│ 函数参数副本 (a)               │  ← 参数总是被"保存"到帧
│ promise_type 对象             │  ← 编译器与代码的桥梁 (第 4 讲)
│ 跨挂起点存活的局部变量 (x,tmp) │  ← 编译器做活跃性分析后决定
│ 挂起点处的 awaiter 临时对象    │  ← co_await 表达式的临时对象
│ 当前状态 (状态机编号/resume点) │  ← "执行到哪一行了"
└───────────────────────────────┘
```

**关键推论**：普通函数里"只用一下"的局部变量不需要进帧；只有**跨越挂起点仍然存活**的变量才必须进帧。编译器会做活跃性分析来最小化帧大小。

## 3.5 句柄的所有权：谁负责销毁？

协程帧是堆分配的资源，必须有人释放。规则：

- `final_suspend` 返回 `suspend_never` 时（第 2 讲示例）：协程一结束，**编译器自动销毁帧**。此后任何对句柄的 `destroy()` 都是二次释放；`done()` 也是未定义行为——**别再用这个句柄**（本讲 3.3 的示例因此改用 `suspend_always`，否则 `while (!c.done())` 会在已销毁的帧上崩溃）。
- `final_suspend` 返回 `suspend_always` 时：协程结束时挂在"终点"，帧还活着。此时必须由**持有句柄的一方**调用 `destroy()`。这个模式的用途（结果搬运、对称传输）见第 6、7 讲。

所有权设计上常见的两种方案：

| 方案 | 谁销毁 | 优点 | 缺点 |
|---|---|---|---|
| 返回对象 RAII 持有句柄（本讲示例） | 返回对象析构时 | 简单安全 | 协程完成时帧不立即释放（要等外壳析构） |
| 协程"自杀"（final_suspend 后由内部释放） | 协程自己 | 帧即刻释放 | 实现复杂，容易踩空指针 |

> 警告：对**未完成**的协程调用 `destroy()` 会跳过一切析构逻辑直接释放帧——帧内局部对象的析构函数不会执行。这通常只在"放弃协程"时才用（第 9 讲取消机制会涉及）。

## 3.6 `coroutine_handle<>` 的类型擦除

`coroutine_handle<P>` 带 promise 类型；而 `coroutine_handle<>` 不带。两者可以互相转换：

```cpp
std::coroutine_handle<my_promise> typed_h = ...;
std::coroutine_handle<> erased_h = typed_h;      // 隐式转换: 擦除类型
// erased_h.resume() / done() / destroy() 都可以用
// 但不能 erased_h.promise() —— 类型信息没了
```

类型擦除的意义：**调度器不需要知道协程的细节**。一个就绪队列可以这样存：

```cpp
std::vector<std::coroutine_handle<>> ready_queue;   // 混合存放各种协程
// 逐个恢复, 不管它们的 promise 是什么
```

这是第 6 讲"调度模型"和后续事件循环课程的基础设施。

## 3.7 小结

1. 句柄 = 指向协程帧的指针 + 三个控制操作（`resume`/`done`/`destroy`）。
2. `get_return_object()` 里用 `from_promise(*this)` 把句柄交给返回对象，是几乎所有协程库的标准起手式。
3. 帧保存参数、跨挂起存活的局部变量、awaiter 临时对象和状态机状态。
4. `final_suspend` 决定帧的销毁责任归属；`suspend_never` = 编译器自动销毁。
5. `coroutine_handle<>` 用于不需要访问 promise 的场景（调度队列）。

## 思考题

1. 3.3 示例中，如果把析构函数删掉会发生什么？（内存泄漏？还是崩溃？）
2. 为什么说对"未完成的协程"调用 `destroy()` 是危险的？什么场景下你**必须**这样做？
3. 把 3.3 示例的 `final_suspend` 改回 `suspend_never`，再运行——观察 `while (!c.done())` 会发生什么（预期崩溃，这就是本讲正文改用它为 `suspend_always` 的原因）。如果想继续用 `suspend_never`，驱动循环应该怎么改写才能安全？
