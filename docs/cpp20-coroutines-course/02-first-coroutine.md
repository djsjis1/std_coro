# 第 2 讲：第一个协程——编译器到底做了什么

## 核心要点

- 协程函数的返回类型必须内嵌一个 `promise_type`；编译器用它"远程控制"协程。
- 编译器把一个协程函数改写成**状态机 + 一次初始设置流程**，这就是全部魔法。
- 协程创建时**并不会立即执行函数体**——执行时机由 `initial_suspend` 决定。
- 只要返回类型满足协议，任何类都可以做协程返回类型（这就是各协程库差异的根源）。

## 回顾第 1 讲思考题

- `int g() { co_return 1; }` **不合法**：`int` 没有 `promise_type`。协程返回类型必须是类。
- 成员函数可以是协程，lambda 也可以是协程。

---

## 2.1 运行本课程的第一个协程

先写一个"真正会暂停"的最小协程。请把它完整复制到 `first.cpp`：

```cpp
// 编译: g++ -std=c++20 first.cpp && ./a.out   (GCC < 14 加 -fcoroutines)
#include <coroutine>
#include <iostream>

// ── 返回类型: 第 2.2 节逐行解释 ─────────────────────────────
struct pause {
    struct promise_type {
        pause get_return_object() { return {}; }
        std::suspend_always initial_suspend() { return {}; }   // 一开始就暂停
        std::suspend_never final_suspend() noexcept { return {}; }
        void return_void() {}
        void unhandled_exception() {}
    };
};

// ── 协程本体 ────────────────────────────────────────────────
pause countdown() {
    std::cout << "  第二步: 协程体开始\n";
    std::cout << "  第三步: 协程体结束\n";
    co_return;                     // 有了它, countdown 才是协程
}

int main() {
    std::cout << "第一步: 创建协程对象 (此刻函数体尚未运行)\n";
    pause p = countdown();         // ① 创建

    // ②③④: 现在通过"句柄"手动驱动它 —— 见第 3 讲
    std::cout << "第四步: main 结束 (协程体其实从未真正执行)\n";
}
```

运行输出：

```
第一步: 创建协程对象 (此刻函数体尚未运行)
第四步: main 结束 (协程体其实从未真正执行)
```

**惊讶吗？** 调用 `countdown()` 之后，协程体一行都没执行！这就是 `initial_suspend` 返回 `suspend_always` 的效果：协程一创建就挂起，函数体要等外部"恢复"它才会运行。

> 这就是"惰性启动"（lazy start）。Python 的 `async def` 也是类似的：调用协程函数只创建协程对象，不会立即执行。
> 后面第 7 讲会演示如何设计"立即启动"（eager start）的协程。

## 2.2 编译器为你生成了什么

要理解协程，必须理解编译器在背后做的一次"改写"。以下面这个协程为例：

```cpp
my_type my_coro(int arg) {
    std::string local = "hello";
    co_await some_awaiter{};
    std::cout << local;
    co_return 42;
}
```

编译器（概念层面）把它改写成大致如下的流程：

```cpp
my_type my_coro(int arg) {
    // ── ① 创建协程帧 (通常 new 一块堆内存) ──
    auto* frame = new coroutine_frame;
    // 把参数和(将来会用到的)局部变量放进帧
    frame->arg = arg;

    // ── ② 在帧内构造 promise_type 对象 ──
    auto& promise = frame->promise;

    // ── ③ 通过 promise 拿回给调用者的"返回对象" ──
    my_type result = promise.get_return_object();

    // ── ④ 执行 initial_suspend ──
    auto init = promise.initial_suspend();
    if (init.await_ready()) {
        // 不挂起 → 立即开始执行协程体
        goto coroutine_body;      // 实际是状态机跳转
    } else {
        // 挂起 → 直接返回给调用者, 函数体以后再说
        return result;            // ← 你看到的 "countdown() 立刻返回"
    }

coroutine_body:                    // 状态机的"状态 0"
    frame->local = "hello";        // 局部变量从栈上挪进了帧
    // co_await some_awaiter{} 展开成:
    {
        auto aw = some_awaiter{};                  // 求值 awaitable
        if (aw.await_ready()) {                    // 已就绪, 不挂起
            /* 直接往下走 */
        } else {
            aw.await_suspend(handle);              // 请求挂起
            // —— 若挂起, 控制权在此离开函数 ——
            return_to_caller_or_someone_else();
        }
    resume_point_1:                               // ← 恢复时从这行继续
        aw.await_resume();                        // 拿回结果
    }

    std::cout << frame->local;
    promise.return_value(42);                      // co_return 42 变成这样

final:                                             // 状态机"终态"
    auto fin = promise.final_suspend();
    if (fin.await_ready()) { /* 直接销毁 */ }
    else { fin.await_suspend(handle); /* 之后销毁 */ }
    delete frame;                                  // 释放协程帧
    return result;
}
```

> 这是**概念模型**，不是真实的编译产物（真实产物是 `switch`/标签跳转的状态机，变量搬进了结构体）。但概念模型精确描述了所有关键时机的调用顺序。

从这个模型里，你已经能看到四个将在后续章节展开的主角：

| 主角 | 对应代码 | 详细讲解 |
|---|---|---|
| **协程帧** | `new coroutine_frame` | 第 3 讲 |
| **promise_type** | `frame->promise` | 第 4 讲 |
| **awaiter / awaitable** | `some_awaiter{}` 的三个成员函数 | 第 5 讲 |
| **句柄 handle** | `await_suspend(handle)` 的参数 | 第 3 讲 |

## 2.3 返回类型协议：为什么是 `promise_type`

编译器不认识你的 `pause` / `my_type`，它只要求**返回类型里能找到 `promise_type`**：

```cpp
struct pause {
    struct promise_type {          // ← 必须是这个确切的名字, 嵌套在返回类型里
        // ... 若干固定接口
    };
};
```

`promise_type` 必须提供（大部分有默认行为）：

| 接口 | 何时被调用 | 作用 |
|---|---|---|
| `get_return_object()` | 协程创建时 | 返回给调用者的对象（通常是协程的"外壳"） |
| `initial_suspend()` | 协程体执行前 | 决定是否一开始就挂起 |
| `final_suspend()` | 协程体结束后 | 决定结束后是否挂起（影响帧销毁时机） |
| `return_void()` / `return_value(x)` | `co_return` 时 | 接收返回值（void / 非 void 二选一） |
| `yield_value(x)` | `co_yield x` 时 | 接收产出的值 |
| `unhandled_exception()` | 协程体抛异常时 | 捕获未处理的异常 |
| `await_transform(x)` | 每个 `co_await x` 前 | 可选，改写 awaitable（第 5 讲） |

## 2.4 `suspend_always` 与 `suspend_never`：两个标准小工具

`<coroutine>` 提供了两个随时可用的 awaiter：

```cpp
namespace std {
    struct suspend_always {         // 永远挂起
        bool await_ready() const noexcept { return false; }
        void await_suspend(coroutine_handle<>) const noexcept {}
        void await_resume() const noexcept {}
    };
    struct suspend_never {          // 永不挂起
        bool await_ready() const noexcept { return true; }
        void await_suspend(coroutine_handle<>) const noexcept {}
        void await_resume() const noexcept {}
    };
}
```

它们就是 awaiter 的最简形态：`await_ready` 返回 `true` 就不挂起，返回 `false` 就挂起。第 5 讲会深入这三个成员函数。

**initial_suspend 的两种经典选择：**

```cpp
// 惰性启动 (lazy): 创建后停在原地, 由外部手动启动
std::suspend_always initial_suspend() { return {}; }

// 立即启动 (eager): 创建后直接跑起来, 直到第一个挂起点
std::suspend_never initial_suspend() { return {}; }
```

## 2.5 亲手驱动协程：完整的手动恢复版本

2.1 的示例"创建了但没运行"。现在补上驱动代码，看协程体真正执行：

```cpp
// 编译: g++ -std=c++20 drive.cpp && ./a.out
#include <coroutine>
#include <iostream>

struct pause {
    struct promise_type {
        pause get_return_object() { return {}; }
        std::suspend_always initial_suspend() { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }
        void return_void() {}
        void unhandled_exception() {}
    };
};

pause countdown() {
    std::cout << "[协程] 开始\n";
    std::cout << "[协程] 结束\n";
    co_return;
}

int main() {
    std::cout << "[main] 创建协程\n";
    pause p = countdown();
    // p 只是"外壳", 真正的控制需要一个句柄 —— 下一讲的主角
    std::cout << "[main] 结束 (协程体仍未执行)\n";
}
```

输出依旧是协程体不执行。为什么我们"拿不到"驱动它的句柄？因为 `get_return_object()` 返回了一个**空壳**——没有把句柄存进去。下一讲就解决这个问题。

## 2.6 小结

1. 协程创建 = 分配帧 → 构造 promise → `get_return_object()` → `initial_suspend()`。
2. `initial_suspend` 返回 `suspend_always` 时，协程体要等外部恢复才执行（惰性启动）。
3. 返回类型只要内嵌 `promise_type` 就满足协程协议。
4. 编译器的改写模型（2.2 节）值得反复回看，它是后续所有章节的共同地图。

## 思考题

1. 把 2.1 的 `initial_suspend` 改成 `suspend_never`，输出会变成什么？试着预测再验证。
2. 协程帧里为什么要保存参数 `arg`？调用者传入的引用参数在协程里安全吗？（提示：第 14 讲）
3. 简述 `get_return_object()` 存在的理由：为什么编译器不直接把协程句柄交给调用者？
