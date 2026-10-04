# 第 15 讲：并发组合器

## 核心要点

- 单个 `Task<T>` 只是"一个异步值"；**并发组合器** 让你把多个 Task 组织起来：同时跑、等最快、等全部、带超时。
- 五种基本模式，按需求递增：
  1. **spawn** — 立即启动后台任务，稍后再 await 结果
  2. **gather** — 并发等待多个 Task，结构化绑定取全部结果
  3. **wait_for** — 给一个 Task 设超时，超时抛 `TimeoutError`
  4. **wait_any** — 多个 Task 竞速，取第一个完成的结果
  5. **TaskGroup** — 结构化并发：任一失败立即取消其余，聚合所有异常
- 选择依据：**任务间是否有依赖** × **是否需要全部结果** × **失败时的期望行为**。

---

## 15.1 为什么需要组合器

第 7 讲实现了 `Task<T>`，第 9 讲讲了对单个 Task 的取消。但真实业务里，你很少只等一个 Task：

```cpp
// 场景：组装一个用户主页，需要同时拉取三类数据
auto users  = co_await fetch_users();   // 200ms
auto posts  = co_await fetch_posts();   // 300ms
auto photos = co_await fetch_photos();  // 100ms
// 串行总耗时: 600ms 😢
```

串行等待的问题在第 1 讲就提过：三个互不依赖的请求，白白浪费了 400ms。
我们需要一种方式把它们**并发启动、统一收集**。

---

## 15.2 spawn — 手动并发

最简单的并发方式：用 `start()` 立即启动 Task，稍后 `co_await` 收集结果。

```cpp
// 编译: g++ -std=c++20 spawn_demo.cpp && ./a.out
#include <coroutine>
#include <iostream>
#include <optional>
#include <queue>
#include <chrono>
#include <thread>

// ---- 最小事件循环 (复习第 6 讲) ----
struct EventLoop {
    std::queue<std::coroutine_handle<>> ready;
    static EventLoop& get() { static EventLoop loop; return loop; }
    void schedule(std::coroutine_handle<> h) { ready.push(h); }
    void run() {
        while (!ready.empty()) {
            auto h = ready.front(); ready.pop();
            h.resume();
        }
    }
};

// ---- 最小 Task<T> (复习第 7 讲, 惰性启动) ----
template <typename T>
struct Task {
    struct promise_type;
    std::coroutine_handle<promise_type> handle_;
    bool ready_ = false;

    Task(std::coroutine_handle<promise_type> h) : handle_(h) {}
    Task(Task&& o) noexcept : handle_(o.handle_), ready_(o.ready_) { o.handle_ = nullptr; }
    ~Task() { if (handle_) handle_.destroy(); }

    struct promise_type {
        std::optional<T> result_;
        std::coroutine_handle<> continuation_;

        Task<T> get_return_object() { return {std::coroutine_handle<promise_type>::from_promise(*this)}; }
        std::suspend_always initial_suspend() { return {}; }
        struct final_awaiter {
            bool await_ready() noexcept { return false; }
            std::coroutine_handle<> await_suspend(std::coroutine_handle<promise_type> h) noexcept {
                if (h.promise().continuation_) return h.promise().continuation_;
                return std::noop_coroutine();
            }
            void await_resume() noexcept {}
        };
        final_awaiter final_suspend() noexcept { return {}; }
        void unhandled_exception() { /* 省略: 见第 10 讲 */ }
        void return_value(T v) { result_ = v; }

        struct awaiter {
            std::coroutine_handle<promise_type> from_;
            bool await_ready() noexcept { return from_.done(); }
            std::coroutine_handle<> await_suspend(std::coroutine_handle<> h) noexcept {
                from_.promise().continuation_ = h;
                return from_; // 对称转移: 直接切入子协程
            }
            T await_resume() { return std::move(*from_.promise().result_); }
        };
        awaiter await_transform(Task<T>& t) { return {t.handle_}; }
    };

    void start() {
        EventLoop::get().schedule(handle_);
    }

    // co_await 支持 (省略 cancel 检查, 见第 7 讲完整版)
    bool await_ready() const noexcept { return ready_; }
    void await_suspend(std::coroutine_handle<> h) {
        handle_.promise().continuation_ = h;
        start();
    }
    T await_resume() { ready_ = true; return std::move(*handle_.promise().result_); }
};

// ---- 示例 ----
Task<int> compute(int id, int ms) {
    // 模拟耗时操作 (实际框架用 co_await sleep)
    co_return id * 100;
}

Task<> main_task() {
    // 1. spawn: 立即启动, 后台运行
    Task<int> t1 = compute(1, 300);
    t1.start();  // 后台开始跑

    Task<int> t2 = compute(2, 200);
    t2.start();  // 同时后台跑

    // ... 这里可以做别的事 ...

    // 2. 稍后收集结果
    int v1 = co_await t1;
    int v2 = co_await t2;
    std::cout << "v1=" << v1 << ", v2=" << v2 << std::endl;
}

int main() {
    auto t = main_task();
    t.start();
    EventLoop::get().run();
}
```

**spawn 的核心**：`start()` 把协程句柄放进事件循环的就绪队列，协程立刻开始执行。
你拿到的是一个 `Task<T>` 句柄，可以随时 `co_await` 取结果。

### spawn 的适用场景

```cpp
// 先启动数据库查询，同时查缓存
auto db_task = query_db();
db_task.start();

auto cache = co_await check_cache();  // 同时查缓存
if (cache.has_value()) return *cache; // 缓存命中直接返回

auto db = co_await db_task;           // 否则等数据库结果
```

关键优势：**启动和等待之间可以插入其他逻辑**。

---

## 15.3 gather — 并发等待，结构化绑定取结果

`spawn` 给你手动控制的灵活性，但大多数场景是"启动 N 个互不依赖的任务，全部完成后继续"。
这正是 `gather` 的用途：

```
Python:   a, b, c = await asyncio.gather(f(), g(), h())
C++:      auto [a, b, c] = co_await gather(f(), g(), h());
```

### 工作原理

```
调用方: co_await gather(taskA, taskB, taskC)
           │
           ├─ 为每个 Task 创建一个 monitor 协程
           │    monitor_0: co_await taskA → 写入 state.results[0]
           │    monitor_1: co_await taskB → 写入 state.results[1]
           │    monitor_2: co_await taskC → 写入 state.results[2]
           │
           ├─ 启动所有 monitor (并发执行)
           │
           └─ 挂起调用方, 等待最后一个 monitor 完成
              → 恢复调用方, 返回 tuple<results...>
```

每个 Task 只能被 `co_await` 一次，所以 gather 为每个 Task 创建一个轻量的 **monitor 协程**做中转。
monitor 的唯一职责就是 await 对应的 Task，然后把结果写入共享状态。

### 异常语义

gather 的异常策略是 **"全部完成再抛第一个"**（与 Python `asyncio.gather` 一致）：

```
taskA: 成功 ✓
taskB: 抛出异常 ✗ ← 保存第一个异常
taskC: 继续运行... 成功 ✓  (不会被取消!)
全部完成 → await_resume 重新抛出 taskB 的异常
```

```cpp
try {
    auto [a, b, c] = co_await gather(
        fetch_users(),    // 成功
        fetch_posts(),    // 失败!
        fetch_photos()    // 成功 (但不会被使用)
    );
} catch (const std::exception& e) {
    // 这里会捕获到 fetch_posts 的异常
    // 注意: fetch_photos 已经跑完了, gather 不会取消其他任务
}
```

> **关键区别**：gather **不取消**其余任务。一个失败后，其他任务继续跑完，
> 只是调用方最终会收到第一个异常。如果你需要"一个失败就取消全部"，用 TaskGroup（15.6 节）。

---

## 15.4 wait_for — 带超时的等待

网络请求可能永远不回来。`wait_for` 给一个 Task 设时间上限：

```
Python:   result = await asyncio.wait_for(fetch(), timeout=5.0)
C++:      result = co_await wait_for(fetch(), 5s);
```

### 工作原理

```
wait_for(task, timeout):
  1. 启动一个定时器协程: sleep(timeout) → task.cancel()
  2. co_await task (正常等待)
  3. 两条竞速路径:
     - task 先完成 → 取消定时器, 返回结果 ✓
     - 定时器先到 → task 被 cancel → 抛 TimeoutError ✗
```

实现的关键细节：**定时器对 task 调用 `cancel()`**，而不是粗暴销毁帧。
这保证了 task 的清理逻辑（析构函数）正常执行（见第 9 讲）。

```cpp
try {
    std::string data = co_await wait_for(fetch_remote(), std::chrono::seconds(5));
    std::cout << "获取成功: " << data << std::endl;
} catch (const TimeoutError&) {
    std::cout << "超时! 5 秒内未响应" << std::endl;
    // fetch_remote 的协程帧已被 cancel, 其内部 RAII 资源已释放
}
```

### 区分 TimeoutError 和 CancelledError

`wait_for` 内部必须区分两种情况：

| 谁先唤醒 | 含义 | 抛什么 |
|---|---|---|
| 定时器先到 | 超时 | `TimeoutError` |
| 外部调用者 cancel | 被取消 | `CancelledError`（原样传播） |

实现上用 `shared_ptr<atomic<bool>> timed_out` 标志区分：
定时器先到时置 `true` 再 cancel task；wait_for 捕获 `CancelledError` 后检查标志，
`true` 则转换为 `TimeoutError`，`false` 则原样 rethrow。

---

## 15.5 wait_any — 竞速取第一个

有时你只关心**最快完成**的那个结果（冗余请求、多路探测）：

```
Python:   done, pending = await asyncio.wait(tasks, return_when=FIRST_COMPLETED)
C++:      result = co_await wait_any(task1, task2);
```

### 两路竞速

```cpp
// 同时查询多个 DNS 服务器, 用最快返回的那个
auto result = co_await wait_any(
    dns_query("8.8.8.8", "example.com"),
    dns_query("1.1.1.1", "example.com")
);
```

### 实现要点

wait_any 为每个 Task 创建一个 **monitor 协程**（和 gather 类似），
但区别是：**第一个完成的 monitor 直接唤醒调用方**，其余 monitor 继续在后台运行（结果丢弃）。

```
wait_any(task1, task2):
  monitor_1: co_await task1 → state.complete(result) ← 第一个到达!
  monitor_2: co_await task2 → state.complete(result) ← 到达时 done=true, 忽略
  调用方: 被 monitor_1 唤醒, 拿到 result
```

> **注意**：落选的 Task 不会自动取消，它们继续在后台运行直到自然完成。
> 如果你需要取消落选者，应使用 TaskGroup 或手动 cancel。

### N 路竞速（vector 版本）

```cpp
std::vector<Task<int>> tasks;
for (auto& server : servers)
    tasks.push_back(ping(server));

int fastest = co_await wait_any(std::move(tasks));
```

---

## 15.6 TaskGroup — 结构化并发

`gather` 的问题是"一个失败不取消其余"。如果你需要**一个失败就取消全部**的语义，
用 `TaskGroup`（对标 Python 3.11 `asyncio.TaskGroup`）：

```
Python:
  async with asyncio.TaskGroup() as tg:
      tg.create_task(fetch(a))
      tg.create_task(fetch(b))
  # 退出时自动等待; 任一失败 → 取消其余 → 抛 ExceptionGroup

C++:
  TaskGroup group;
  group.spawn(fetch(a));
  group.spawn(fetch(b));
  co_await group.wait();  // 显式收尾
```

### 状态机

TaskGroup 有显式的三态生命周期：

```
open ──── wait()/析构 ──→ closing ──── 全部完成 ──→ finished
  │                         │
  │ 可 spawn                │ 拒绝新 spawn
  │ 可 wait                 │ 拒绝重复 wait
  └─────────────────────────┘
```

- **open**：正常接收 `spawn()`，可以 `wait()`。
- **closing**：进入收尾，拒绝新的 `spawn()` 和重复 `wait()`。
- **finished**：所有子任务结束，终态。

### 异常策略：ExceptionGroup

与 gather 的"只保存第一个"不同，TaskGroup **收集所有异常**，
并在 `wait()` 时打包为 `ExceptionGroup` 抛出：

```
taskA: 成功 ✓
taskB: 抛出 "连接超时" ✗ → 触发组取消
taskC: 被组取消 → CancelledError (不聚合!)
taskD: 抛出 "权限不足" ✗

wait() 完成 → 抛 ExceptionGroup(["连接超时", "权限不足"])
```

```cpp
try {
    TaskGroup group;
    group.spawn(fetch_a());
    group.spawn(fetch_b());
    group.spawn(fetch_c());
    co_await group.wait();
} catch (const ExceptionGroup& eg) {
    for (auto& e : eg.exceptions()) {
        try {
            std::rethrow_exception(e);
        } catch (const std::exception& ex) {
            std::cerr << "子任务失败: " << ex.what() << std::endl;
        }
    }
}
```

### CancelledError 不被聚合

这是 TaskGroup 的重要约定：**取消不是错误，是控制流**。

组内因取消引起的 `CancelledError` 不会进入 `ExceptionGroup`。
只有"真正的业务异常"才会被聚合。这让调用方可以统一处理失败，
而不必在异常列表里过滤取消信号。

### 析构兜底

如果你忘记 `wait()`，析构函数会**自动取消残留子任务**（防孤儿），
但不会假装完成异步等待。这是安全网，不是推荐用法——
始终显式 `wait()` 收尾。

---

## 15.7 选择指南

```
                        ┌─ 需要全部结果? ─┐
                        │                 │
                       是                否
                        │                 │
                   固定数量          wait_any
                        │           (取最快一个)
                   gather
                        │
              ┌─ 一个失败要取消其余? ─┐
              │                       │
             是                      否
              │                       │
         TaskGroup                gather
    (ExceptionGroup 聚合)    (其余继续跑完再抛)
```

| 模式 | 启动时机 | 等待策略 | 失败行为 | 返回类型 |
|---|---|---|---|---|
| `spawn` + `co_await` | `start()` 时 | 逐个等 | 正常传播 | `T` |
| `gather` | 调用时 | 等全部 | 保存第一个，其余继续 | `tuple<Ts...>` |
| `wait_for` | 调用时 | 等 task 或超时 | 超时抛 `TimeoutError` | `T` |
| `wait_any` | 调用时 | 等最快 | 第一个失败即抛出 | `T` |
| `TaskGroup` | `spawn` 时 | `wait()` 等全部 | 取消其余，聚合异常 | `void` |

---

## 15.8 组合使用

这些组合器可以自由嵌套：

```cpp
// 并发拉取三组数据，每组内部有超时保护
auto [users, posts, photos] = co_await gather(
    wait_for(fetch_users(),  3s),   // 超时 3 秒
    wait_for(fetch_posts(),  5s),   // 超时 5 秒
    wait_for(fetch_photos(), 2s)    // 超时 2 秒
);
```

```cpp
// TaskGroup 内部用 spawn 启动动态数量的任务
TaskGroup group;
for (auto& url : urls) {
    group.spawn(download(url));
}
try {
    co_await group.wait();
} catch (const ExceptionGroup& eg) {
    std::cerr << eg.exceptions().size() << " 个下载失败" << std::endl;
}
```

---

## 15.9 常见陷阱

### 陷阱 1：gather 的 Task 参数顺序 = 结果 tuple 顺序

```cpp
auto [a, b] = co_await gather(
    fast_task(),   // 虽然先完成，但结果在 a
    slow_task()    // 后完成，结果在 b
);
// a = fast_task() 的结果, b = slow_task() 的结果
// 顺序由参数位置决定，与完成先后无关
```

### 陷阱 2：wait_any 的落选任务仍在运行

```cpp
auto result = co_await wait_any(quick_task(), slow_task());
// quick_task 赢了, 但 slow_task 仍在后台运行!
// 如果 slow_task 持有资源, 它会一直持有到自然完成
// 需要取消时, 请手动管理或使用 TaskGroup
```

### 陷阱 3：TaskGroup 只能 wait() 一次

```cpp
TaskGroup group;
group.spawn(task_a());
co_await group.wait();    // 第一次: 正常
co_await group.wait();    // ❌ 抛 StructuredConcurrencyError
group.spawn(task_b());    // ❌ 抛 StructuredConcurrencyError (已进入 finished)
```

### 陷阱 4：wait_for 里不要 co_await 右值

```cpp
// ✅ 正确: wait_for 接受 Task 右值, 内部按引用等待
auto result = co_await wait_for(some_task(), 5s);

// ❌ 错误理解: 不要把 wait_for 当成"给 co_await 加超时"
// wait_for 的参数是 Task 对象, 不是 co_await 表达式
```

---

## 15.10 小结

并发组合器是协程框架从"能用"到"好用"的关键一步：

| 你需要的 | 用这个 |
|---|---|
| 后台启动一个任务 | `task.start()` |
| 后台启动，稍后取结果 | `spawn` + `co_await` |
| 同时等多个，取全部结果 | `gather` |
| 给操作加超时 | `wait_for` |
| 多路竞速，取最快 | `wait_any` |
| 结构化并发，失败取消全部 | `TaskGroup` |

它们的底层实现都遵循同一个模式：**monitor 协程 + 共享状态 + 计数器**。
理解了这个模式（第 7 讲 Task 设计的延伸），你就能自己实现任何新的组合器。

下一讲（第 16 讲）将介绍 **Context 与取消传播实战**——如何让取消信号沿协程调用链自动向下传播。
