# 第 8 讲：co_yield 与生成器

## 核心要点

- `co_yield v;` 是语法糖：等价于 `co_await promise.yield_value(v);`——**产出值然后挂起**。
- 生成器 = "可以多次产出的协程"：每次恢复，执行到下一个 `co_yield`。
- 实现要点：`yield_value` 把值暂存进 promise；消费端 `resume()` 后从 promise 读取。
- `co_yield` 与 `co_return` 可以出现在同一协程（产出若干值后返回最终结果）。
- C++23 的 `std::generator` 是本讲内容的标准库版本；理解手写版才能理解标准版。

---

## 8.1 语法糖的本质

标准规定：

```cpp
co_yield expr;
// 完全等价于:
co_await promise.yield_value(expr);
```

所以 `co_yield` 不需要新的语言机制——它就是"调用 `yield_value` 的 `co_await`"。`yield_value` 返回什么 awaiter，决定产出后是否挂起（生成器场景总是挂起）。

## 8.2 最小生成器（自然数序列）

```cpp
// 编译: g++ -std=c++20 generator.cpp && ./a.out
#include <coroutine>
#include <iostream>
#include <optional>

template <typename T>
class Generator {
public:
    struct promise_type {
        T current_value;                      // 产出值的暂存处

        Generator get_return_object() {
            return Generator{std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        std::suspend_always initial_suspend() { return {}; }   // 惰性: 等第一次 next

        // co_yield v → 走这里: 存值 + 挂起
        std::suspend_always yield_value(T v) noexcept {
            current_value = v;
            return {};
        }

        std::suspend_always final_suspend() noexcept { return {}; }
        void return_void() {}
        void unhandled_exception() { std::terminate(); }
    };

    using H = std::coroutine_handle<promise_type>;

    explicit Generator(H h) : h_(h) {}
    Generator(const Generator&) = delete;
    Generator(Generator&& o) noexcept : h_(std::exchange(o.h_, nullptr)) {}
    ~Generator() { if (h_) h_.destroy(); }

    // 拉取下一个值: resume 到下一个 co_yield, 然后从 promise 取
    std::optional<T> next() {
        if (!h_ || h_.done()) return std::nullopt;
        h_.resume();
        if (h_.done()) return std::nullopt;           // 协程结束, 没有更多值
        return h_.promise().current_value;
    }

private:
    H h_;
};

// ── 用法: 与 Python 生成器一模一样的拉取模型 ──
Generator<int> naturals() {
    int i = 0;
    while (true)
        co_yield i++;        // 产出 i, 挂起; 下次 next() 时从下一行继续
}

Generator<int> range(int from, int to) {
    for (int i = from; i < to; ++i)
        co_yield i;
    // 自然结束 → return_void → final_suspend → done() == true
}

int main() {
    auto g = naturals();
    for (int i = 0; i < 5; ++i)
        std::cout << *g.next() << " ";     // 0 1 2 3 4
    std::cout << "\n";

    auto r = range(10, 13);
    while (auto v = r.next())
        std::cout << *v << " ";            // 10 11 12
    std::cout << "\n";
}
```

执行时间线（以 `range(10,13)` 为例）：

```
next() #1: resume → 协程体开始 → co_yield 10 → yield_value 存 10 → 挂起 → 返回 10
next() #2: resume → 从 co_yield 之后继续 → co_yield 11 → 挂起 → 返回 11
next() #3: resume → co_yield 12 → 返回 12
next() #4: resume → 循环结束 → return_void → final_suspend 挂起 → done()==true
           → next() 返回 nullopt
```

## 8.3 惰性求值的力量

生成器的核心价值：**序列边生成边消费，内存占用 O(1)**。

```cpp
// 斐波那契: 不预先算任何数, 每次 next() 才算一个
Generator<long long> fib() {
    long long a = 0, b = 1;
    while (true) {
        co_yield a;
        long long c = a + b;
        a = b;
        b = c;
    }
}

// 只取前 20 个, 后面的永不计算
int main() {
    auto g = fib();
    for (int i = 0; i < 20; ++i)
        std::cout << *g.next() << " ";
}
```

对比传统做法（预生成 `vector<long long>`），无限序列、大序列、管道式处理（map/filter/take）都变得自然。

## 8.4 消费端进阶：支持 range-for

给生成器加上迭代器，就能用 `for (int x : gen)`：

```cpp
template <typename T>
class Generator {
    // ... promise_type 同上 ...

    // ── 迭代器 (最简版): 单趟、非拷贝 ──
    struct iterator {
        Generator* gen;
        std::optional<T> current;

        T operator*() const { return *current; }
        iterator& operator++() {
            current = gen->next();
            return *this;
        }
        bool operator!=(const iterator&) const { return current.has_value(); }
    };

    iterator begin() { return iterator{this, next()}; }
    iterator end() { return iterator{this, std::nullopt}; }
};

// 现在可以这样用:
//   for (int x : range(0, 10)) std::cout << x << " ";
```

## 8.5 与 `co_return` 共存

生成器也可以产出若干值后"带最终结果"结束：

```cpp
Generator<int> count_then_sum() {
    co_yield 1;
    co_yield 2;
    co_return;   // 结束 (return_void)
}
```

若想返回最终值，让 `return_value` 存进 promise 的另一个槽（如 `final_value`），消费端在 `done()` 后读取。主流生成器（包括 C++23 `std::generator`）只支持 `return_void`，保持简单。

## 8.6 C++23 标准库 `std::generator`

本讲的手写版正是 C++23 `<generator>` 的迷你原型。标准版要点：

```cpp
#include <generator>
#include <print>

std::generator<int> naturals() {
    int i = 0;
    while (true) co_yield i++;
}

// 高级能力: 递归产出 (co_yield 嵌套), 引用产出, 异常透传
// 注意: std::generator 是 move-only, 且同一时刻只能有一个活跃实例
```

标准版解决了手写版的大量边界问题（嵌套 yield、`co_yield` 左值引用的生命周期、异常后清理等）。手写过一遍之后，你会更清楚标准版每个约束背后的原因。

## 8.7 小结

1. `co_yield v` ≡ `co_await promise.yield_value(v)`。
2. 生成器 = 惰性启动 + `yield_value` 存值挂起 + 消费端 `resume` 拉取。
3. 核心收益：O(1) 内存的惰性序列、无限序列、管道式组合。
4. C++23 `std::generator` 是标准答案；手写版帮助你理解它。

## 思考题

1. `co_yield` 时 `current_value` 被下一次 `next()` 覆盖——如果消费端想保留历史值怎么办？（提示：消费端自己拷贝；或生成器产出智能指针）
2. 生成器的 `next()` 里 `h_.resume()` 如果让协程抛异常会发生什么？（本讲 `unhandled_exception` 是 terminate——第 10 讲给出生产级方案）
3. 为什么生成器通常是 move-only？拷贝一个"正在产出中"的生成器有什么问题？
