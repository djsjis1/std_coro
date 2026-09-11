# 第 14 讲：常见陷阱与最佳实践

## 核心要点

- 绝大多数协程 bug 都是**生命周期**问题：谁持有谁、谁先死、谁还在用已销毁的东西。
- 四大高危区：悬空引用参数、销毁挂起中的帧、double-resume、异常静默丢失。
- 调试协程先查三件事：帧是否还活着、句柄是否重复 resume、等待者是否被唤醒。
- 编译器与 Debug 模式的差异可能掩盖/暴露 bug（MSVC Debug 的 lambda 协程捕获是知名案例）。
- 最佳实践可以浓缩为：**RAII 持有句柄、移动而非拷贝、fire-and-forget 必须报告异常、用命名函数而非 lambda 协程**。

---

## 14.1 陷阱一：悬空引用参数

```cpp
Task<> read_and_log(const std::string& s) {   // ❌ 按引用接收
    co_await something();
    std::cout << s;                            // 恢复时 s 可能已悬空!
}

void caller() {
    auto t = read_and_log(make_temp_string()); // 临时对象: 协程挂起后即析构
    t.start();
}
```

**规则**：协程的**引用参数**在挂起恢复后**不保证有效**（除非调用者显式保证生命周期覆盖整个协程）。

```cpp
// ✅ 安全: 按值接收 (拷进帧)
Task<> read_and_log(std::string s) { ... }

// ✅ 或明确约定: 调用者保证引用存活 (库文档必须写明)
```

> 技术细节：按值参数由编译器**复制进帧**；引用参数在帧里存的只是"引用本身"，指向的对象仍在调用者栈上。

## 14.2 陷阱二：销毁"挂起中"的协程

```cpp
Task<> background_work() {
    co_await long_sleep();       // 挂起在这里
    co_return;
}

void bug() {
    { Task<> t = background_work(); }   // ❌ 作用域结束 → t 析构 → destroy() 帧
    // 协程还挂在 long_sleep 上! 之后被唤醒 → resume 已销毁的帧 → UB
}
```

两个具体表现：

1. **析构销毁挂起帧**：帧内对象析构执行了，但**挂起点注册信息**（定时器条目、等待队列槽位）变成悬空 → 到期唤醒时访问已释放内存。
2. **双重 destroy**：协程完成后帧已被编译器销毁，外壳析构又 `destroy()` 一次。

**防御**：

- 析构前检查 `done()` / 帧存活标志（`frame_alive` 模式）
- 文档明确生命周期契约："Task 必须保持存活直到完成"
- fire-and-forget 用**自持有**（协程帧持有自己的外壳）：

  ```cpp
  void launch_fire_and_forget() {
      auto self = std::make_shared<Task<>>();
      *self = [self]() -> Task<> { co_await do_work(); }();  // 帧持有 self
      self->start();        // 协程结束前 self 引用计数不归零 → 帧不销毁
  }
  ```

## 14.3 陷阱三：double-resume 与重复调度

```cpp
auto t = spawn(work());
t.cancel();          // cancel 内部 schedule(t)
co_await std::move(t);   // await 内部又启动/调度
// 同一句柄在就绪队列里出现两次 → 第一次 resume 后帧销毁 → 第二次 resume 已销毁帧
```

**规则**：同一时刻，一个协程**至多被调度一次**。库内部必须保证 `schedule` 幂等或记录"在队列中"状态。

防御手段：promise 记录状态标志：

```cpp
bool suspended = false;      // await_suspend 置 true, await_resume 置 false
void cancel() {
    cancelled_ = true;
    if (suspended) scheduler.schedule(handle);   // 只在真正挂起时唤醒
    // 若在就绪队列中: 不重复调度, 协程跑到下一个 await 点自然响应取消
}
```

## 14.4 陷阱四：异常静默丢失

```cpp
coro::spawn([]() -> Task<> {
    throw std::runtime_error("boom");    // 没人 await 这个任务
    co_return;
}());                                    // 异常被冷藏, 永不报告 → 行为诡异
```

**规则**：所有 fire-and-forget 协程的异常**必须**有报告渠道（全局回调、日志、计数器）。Python 的 "Task exception was never retrieved" 警告就是为这个场景而生。

```cpp
// promise 侧:
void unhandled_exception() {
    if (task_ == nullptr)                       // detach 状态
        global_unhandled_handler(std::current_exception());   // 报告!
    else
        exception_ = std::current_exception();  // 有等待者: 正常转发
}
```

## 14.5 陷阱五：等待者队列里的僵尸句柄

第 11 讲的等待队列存句柄；如果等待中的协程**被取消或异常销毁**，队列里的句柄就指向已销毁的帧：

```cpp
struct Lock {
    std::deque<std::coroutine_handle<>> waiters;
    void release() {
        auto h = waiters.front(); waiters.pop_front();
        scheduler.schedule(h);        // 若 h 的协程已被销毁 → resume 野指针 → UB
    }
};
```

**防御**：等待者"上钩"时注册清理回调，销毁时从队列摘除：

```cpp
// awaiter 挂起时:
void await_suspend(std::coroutine_handle<> h) {
    my_handle = h;
    lock_.waiters.push_back(h);
}
// awaiter 析构 (帧销毁必然触发) 时:
~awaiter() {
    if (my_handle)
        lock_.remove_waiter(my_handle);     // 从队列摘除
}
```

## 14.6 陷阱六：`final_suspend` 返回 `suspend_never` 后继续用句柄

```cpp
struct Task {   // final_suspend = suspend_never
    void resume() { if (!h_.done()) h_.resume(); }
    bool done() const { return h_.done(); }
};

Task t = work();
t.resume();             // 协程跑完 → 帧自动销毁
t.done();               // ❌ done() 访问已销毁帧 → UB
```

**规则**：`final_suspend` 返回 `suspend_never` 时，协程一完成句柄立即失效。外壳必须记录"已完成"状态，之后**不再触碰句柄**：

```cpp
bool ready_ = false;                 // 外壳自己的完成标志
void resume() {
    if (!ready_ && !h_.done()) h_.resume();
    // final_suspend 里把 ready_ 置 true, 之后任何路径都不再碰 h_
}
```

## 14.7 陷阱七：MSVC Debug 下的 lambda 协程捕获

MSVC **Debug** 模式下，lambda 协程的捕获变量可能不被正确复制进协程帧，挂起恢复后读到错误值（Release 无此问题）：

```cpp
// ❌ Debug 下捕获的 i 可能出错
for (int i = 0; i < 3; ++i)
    tasks.push_back([i]() -> Task<int> { co_await sleep(10ms); co_return i; }());

// ✅ 命名函数: 参数进协程帧, 生命周期由标准保证
Task<int> make_task(int i) { co_await sleep(10ms); co_return i; }
for (int i = 0; i < 3; ++i)
    tasks.push_back(make_task(i));
```

同样，自持有（14.2）也优先用"命名函数 + `shared_ptr` 参数"而非 self-referencing lambda。

### 陷阱七·补充：`throw` 之后没有 `co_return`

协程体内用 `throw` 提前结束时，必须在 `throw` 之后补一个 `co_return;`（即使永远执行不到）：

```cpp
Task<int> failing() {
    throw std::runtime_error("boom");
    co_return 0;    // ✅ 必须补上
}
```

实测：MSVC Debug 下缺了这个 `co_return`，异常会**绕过 `unhandled_exception` 直接逃逸**（等待者收不到异常，程序行为像普通函数 throw），GCC/Clang 无此问题。这是编译器对协程状态机"异常路径终止点"的处理差异；补 `co_return` 是零成本的跨编译器防御写法。

## 14.8 陷阱八：递归协程与栈增长

`await_suspend` 里同步 `resume()` 子协程（第 7 讲教学版的做法），会让调用栈随"协程链"增长：

```
A 同步 resume B → B 同步 resume C → C 同步 resume D → ... 栈越来越深
深递归 + 长链 = 栈溢出
```

**规则**：生产级实现里，启动/唤醒一律经过调度器（不对称传输），完成传播用对称传输（第 6 讲）。**禁止在 await_suspend 里直接同步 resume 用户协程**。

## 14.9 调试技巧速查

| 症状 | 第一嫌疑 | 排查动作 |
|---|---|---|
| 协程体从不执行 | 惰性启动被遗忘 | 检查 `initial_suspend` / 是否 `start()` |
| 挂起后永远不恢复 | 等待者队列没唤醒它 | 检查唤醒路径是否 `schedule` |
| 恢复即崩溃 | 帧已销毁 / double-resume | 检查生命周期、析构、取消路径 |
| 变量值错乱 | Debug 下 lambda 捕获 / 悬空引用 | 换命名函数、按值传参 |
| 异常消失 | fire-and-forget 未报告 / `throw` 后缺 `co_return` | 检查 `unhandled_exception`、全局回调、补 `co_return` |
| 内存持续增长 | 忘了 destroy 挂起的帧 | 检查 `final_suspend` 后的销毁责任 |

## 14.10 最佳实践清单

**接口设计**

1. Task 只移动不拷贝；移动后更新一切回指。
2. 明确生命周期契约并写进文档：谁持有句柄、何时销毁。
3. 单等待者与多等待者分开设计（Task 单槽 vs Future 队列）。

**实现**
4. `await_suspend` 里绝不同步 resume 用户协程（防栈增长）。
5. `schedule` 对同一句柄幂等（或记录在队状态，防 double-resume）。
6. 等待队列支持"等待者销毁时摘除"（防僵尸句柄）。
7. 挂起在 IO/定时器上时，帧销毁必须联动失效底层登记信息。
8. fire-and-forget 异常必报告，`CancelledError` 单独成类便于过滤。

**编码习惯**
9. 优先命名函数 + 参数，避免 lambda 协程捕获（尤其 MSVC Debug）。
10. 引用参数想清楚生命周期；拿不准就按值。
11. 挂起前的清理用作用域块，挂起后的状态假设全部失效。

## 14.11 结语

学完 14 讲，你已经具备：读懂任何主流协程库源码的能力、独立实现 `Task`/`Generator` 的能力、设计事件循环与取消机制的方法论。

再往前的路：

- **实践**：用第 6 讲调度器 + 第 7 讲 Task + 第 9 讲取消 + 第 11 讲原语，拼出一个完整的单线程事件循环框架
- **进阶**：结构化并发（TaskGroup）、多线程调度器（work-stealing）、零拷贝 IO（io_uring 深入）
- **对照**：阅读 cppcoro / folly::coro / Boost.Cobalt 的源码，验证本课程的概念

## 思考题（综合）

1. 回顾全部 14 讲，画出"一次完整的协程调用"全生命周期图（创建 → 挂起 → 恢复 → 取消 → 异常 → 销毁）。
2. 设计一个 `TaskGroup`：管理 N 个子任务，作用域结束时自动等待全部完成，任一失败取消其余。写出接口与关键实现思路。
3. 你实现的协程库里，哪些地方是"教学简化"？把它们替换为生产级方案需要什么？
