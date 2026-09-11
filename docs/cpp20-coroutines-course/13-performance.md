# 第 13 讲：性能——协程帧与堆分配优化

## 核心要点

- 协程的核心开销 = **一次堆分配（帧）+ 几次函数调用**，远小于线程，但并非零成本。
- 帧的大小 = 参数 + 跨挂起存活的局部变量 + promise + awaiter 临时对象；**编译器做活跃性分析**帮你压缩。
- **堆分配消除（HALO, Heap Allocation eLOsion）**：编译器在能证明"协程生命周期短于调用者"时，把帧内联进调用者栈——非强制，不同编译器激进程度不同。
- 你可以通过 promise 的 `operator new` **接管帧分配**：对象池、自定义分配器、统计分配次数。
- 优化清单：缩小跨挂起变量的存活范围 → 减少进帧的变量 → 复用 awaiter → 对称传输 → 帧池。

---

## 13.1 协程到底贵在哪

把一个"普通函数改成协程"的增量成本：

| 成本项 | 数量级 | 说明 |
|---|---|---|
| 帧堆分配 | 数十纳秒 ~ 微秒 | 每次创建协程一次 |
| 帧释放 | 数十纳秒 | 协程结束时 |
| 挂起/恢复 | 各 ~ 1 次函数调用 + 状态机跳转 | 非常便宜 |
| 局部变量拷贝进帧 | 取决于变量大小 | 编译器只搬"跨挂起存活"的 |

对比：线程创建 ~ 数十微秒 + 栈分配（KB~MB 级）+ 内核调度开销。**协程比线程便宜 2~3 个数量级**——这就是"百万协程"的现实基础。

## 13.2 帧里装了什么（决定帧大小）

第 3 讲讲过帧布局，现在从"体积"角度再看：

```cpp
Task<int> heavy(int big_param /* 4KB 结构体 */) {
    std::array<char, 4096> big_local;     // 4KB
    std::array<char, 4096> small_use;     // 只用一次, 不跨挂起

    use_once(small_use);                  // 编译器发现它不跨挂起 → 可留在栈上
    co_await something();                 // 挂起点
    touch(big_local);                     // big_local 跨挂起存活 → 进帧
    co_return 0;
}
```

帧大小 ≈ `sizeof(big_param) + sizeof(big_local) + promise + awaiter 临时 + 状态机状态`。

**优化原则一：缩短变量的存活范围。**

```cpp
// ❌ 大对象全程存活 → 进帧
Task<> bad() {
    std::string huge = load_big_string();   // 跨挂起存活
    co_await something();
    process(huge);
}

// ✅ 用作用域限制存活期 → 不需要进帧
Task<> good() {
    {
        std::string huge = load_big_string();   // 在挂起前就用完
        process(huge);
    }
    co_await something();
}
```

## 13.3 堆分配消除（HALO）

概念：**如果编译器能证明协程帧的生存期被严格包含在调用者函数内，它可以把帧直接内联到调用者的栈上**，完全消灭堆分配。

条件（编译器必须**能证明**）：

- 协程的帧大小在编译期可知
- 所有挂起点（`initial_suspend` 等）都不能逃出调用者作用域
- 例如：`initial_suspend` 和 `final_suspend` 都返回 `suspend_never`，协程内所有 `co_await` 都不实际挂起 → 编译器常能内联

```cpp
Task<int> leaf() {           // 从不挂起的"叶子协程"
    co_return 42;            // 没有 co_await → 编译器很可能直接内联帧
}

void caller() {
    auto t = leaf();         // 帧可能在 caller 的栈上, 零堆分配
    use(t);
}
```

重要现实：**HALO 是"允许"而非"强制"**。三个主流编译器都实现了，但激进程度和触发条件不同（MSVC 较保守，Clang/GCC 更积极）。**不要写依赖 HALO 的性能代码**——用 13.4 的显式控制。

## 13.4 接管帧分配：`operator new` 定制

promise 可以定义 `operator new`/`operator delete` 接管帧的分配：

```cpp
// 编译: g++ -std=c++20 alloc_count.cpp && ./a.out
#include <coroutine>
#include <cstdio>
#include <new>

struct Task {
    struct promise_type {
        Task get_return_object() {
            return Task{std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        std::suspend_never initial_suspend() { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }
        void return_void() {}
        void unhandled_exception() {}

        // ── 接管帧分配: 顺便统计 ──
        static void* operator new(std::size_t size) {
            ++alloc_count;
            return ::operator new(size);        // 可替换为对象池
        }
        static void operator delete(void* p, std::size_t size) {
            ::operator delete(p, size);
        }
        inline static int alloc_count = 0;      // 帧分配计数
    };
    // ... 其余 (句柄管理略)
};

Task demo() { co_return; }

int main() {
    demo();
    demo();
    std::printf("帧分配次数: %d\n", Task::promise_type::alloc_count);  // 2
}
```

用途：

1. **统计**：测量真实分配次数，验证 HALO 是否生效
2. **对象池**：高频创建/销毁协程时复用帧内存（注意：需要帧大小分桶）
3. **对齐定制**：帧需要特殊对齐时

> 细节：编译器调用的是 promise 类型的 `operator new(size)`；如果找不到，回退到全局 `::operator new`。`operator new` 必须是 `static`。

## 13.5 其他优化手段

### 对称传输（第 6 讲）

"任务完成 → 恢复等待者"用对称传输（`await_suspend` 返回句柄），避免"回到调度器再 resume"的往返：

```cpp
struct final_awaiter {
    bool await_ready() const noexcept { return false; }
    std::coroutine_handle<> await_suspend(std::coroutine_handle<> h) const noexcept {
        if (auto cont = h.promise().continuation)
            return cont;                    // 直接切过去
        return std::noop_coroutine();
    }
    void await_resume() const noexcept {}
};
```

### 减少 awaiter 体积

`co_await` 表达式里的 awaiter 对象存进帧。体积大的 awaiter（如内部有缓冲区的 IO 操作）应尽量共享/外置（存指针而非值）。

### 避免不必要的挂起

`await_ready()` 返回 `true` 走快速路径，**零挂起往返**（第 5 讲）。热路径上，"已就绪"的判断值得认真写。

### 帧复用与内存池

对"频繁创建、短生命周期"的协程，帧池能显著降低 malloc 压力。要点：帧大小不同（与协程函数的局部变量相关），池要按大小分桶，且注意帧内对象的构造/析构由编译器管理，池只需负责原始内存。

## 13.6 性能测量清单

优化前先测量。建议指标：

```cpp
// 1. 帧分配次数 (13.4 的 operator new 统计)
// 2. 挂起/恢复往返耗时:
auto t0 = now();
for (int i = 0; i < N; ++i) { co_await yield(); }   // 测 N 次让出
auto per_yield = (now() - t0) / N;
// 3. 对比基线: 相同逻辑的普通函数 + 回调
```

经验数值参考（桌面 CPU，Debug 与 Release 差异巨大，**务必用 Release 测**）：

| 操作 | 量级 |
|---|---|
| 一次协程挂起+恢复 | ~10~100 ns |
| 一次帧分配+释放 | ~50~200 ns |
| 线程上下文切换 | ~1~10 µs |

## 13.7 小结

1. 协程比线程便宜 2~3 个数量级，但仍有"帧分配"这个固定成本。
2. 缩小变量存活范围 = 缩小帧。
3. HALO 是编译器的"尽力而为"，不要依赖它。
4. promise 的 `operator new` 是接管帧分配的唯一正规入口。
5. 热路径三件套：快速路径 `await_ready`、对称传输、减少进帧变量。

## 思考题

1. 为什么"不跨挂起的局部变量"可以留在栈上？编译器如何证明？（提示：活跃性分析）
2. 用 13.4 的计数方法，测量一个"从不挂起的叶子协程"在你的编译器上是否触发 HALO（帧分配次数是否为 0）。
3. 帧池为什么通常按大小分桶？直接复用固定大小帧有什么问题？
