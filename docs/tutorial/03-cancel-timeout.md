# 第 3 讲 — 取消与超时

> 本讲目标：掌握 coro 的**取消语义**（对标 Python `task.cancel()`）、
> **超时等待**（`wait_for`）、**竞速**（`wait_any` / `wait_tasks`）、
> 以及 **TaskGroup 结构化并发**（Python 3.11 同款）。
> 这是把原型代码变成可靠服务的分水岭。

---

## 3.1 取消：注入 CancelledError 的协作式取消

coro 的取消是**协作式**的，与 Python 完全同构：

1. 调用 `t.cancel()` → 一个取消标志被置位；
2. 目标协程在**下一个 await 点**被唤醒，并抛出 `coro::CancelledError`；
3. 异常沿协程体正常展开 —— **所有栈上对象的析构函数都会执行**
   （这就是 C++ 的 "finally"）；
4. 未捕获的 `CancelledError` 成为任务的失败结果；
   等待它的协程也在 `co_await` 处收到同一个异常。

```cpp
// cancel.cpp
#include <coro/coro.hpp>
#include <iostream>

using namespace std::chrono_literals;

// 一个"永远干活"的循环任务
coro::Task<> heartbeat()
{
    try {
        for (int i = 0; ; ++i) {
            std::cout << "心跳 " << i << std::endl;
            co_await coro::sleep(200ms);   // 取消在这里生效
        }
    } catch (const coro::CancelledError&) {
        std::cout << "收到取消, 清理资源..." << std::endl;
        // 在这里收尾: 关文件、发退出包、释放连接...
        // 不再抛出 = "取消保护": 任务可以正常结束甚至正常返回
    }
}

coro::Task<> main_task()
{
    auto t = coro::spawn(heartbeat());
    co_await coro::sleep(1s);      // 让它跳 4 次左右
    t.cancel();                    // 请求取消
    co_await std::move(t);         // 等待清理完成
    std::cout << "已优雅停止" << std::endl;
}

int main() { coro::run(main_task()); }
```

### 取消语义细则

- **生效点**：任何 `co_await`。正在 `sleep` / 等锁 / 等队列 / 等网络 IO
  的协程都会被立刻唤醒并注入异常——包括挂在 IOCP/io_uring 上的
  网络/文件操作（库会先取消底层系统调用，再唤醒协程）。
- **一次性注入**：如果协程体 `catch` 了 `CancelledError` 并且不再抛出，
  任务可以继续运行直至**正常完成**（上面的"取消保护"）。
  想"绝对不可取消"的临界区，catch 后继续即可（对标 `asyncio.shield` 的效果）。
- **循环任务也能停**：不需要在循环条件里写"停止标志"，
  取消就是停止机制。
- **在事件循环线程调用** `cancel()`（绝大多数场景你本来就在协程里）。
- 取消**尚未启动**的 Task：任务直接以 `CancelledError` 为结果，函数体不执行。

### 未捕获取消的传递

```cpp
auto t = coro::spawn(slow_task());
t.cancel();
try {
    co_await std::move(t);
} catch (const coro::CancelledError&) {
    // 等待者也会收到取消
}
```

---

## 3.2 wait_for — 超时等待

手写"取消 + 定时器"太啰嗦，库提供了封装：

```cpp
try {
    auto data = co_await coro::wait_for(fetch(url), 500ms);
    // 500ms 内完成 → 拿到结果
} catch (const coro::TimeoutError&) {
    // 超时: fetch 已经被自动 cancel 并清理完毕
}
```

- 超时后任务被**自动取消**（内部= 启动一个定时器协程，到点调 `cancel()`），
  你收到 `coro::TimeoutError`。
- 提前完成则定时器被撤销，零残留。
- `Task<void>` 也有对应重载：`co_await coro::wait_for(task_void, 1s);`

## 3.3 wait_any — 两路竞速

"主源 + 备源，谁先回用谁"：

```cpp
int price = co_await coro::wait_any(
    query_vendor_a(),     // Task<int>
    query_vendor_b());    // Task<int>, 同类型
// 先完成者胜出; 慢的那个继续在后台跑, 结果被丢弃
```

注意：落选者**不会被取消**，仍在后台运行（对标 `asyncio.wait`
的 `FIRST_COMPLETED`）。要回收它，自己 spawn 后统一取消，
或接受它自然跑完。

## 3.4 wait_tasks — N 路等待（对标 asyncio.wait）

运行时数量的 N 路等待，三种模式：

```cpp
std::vector<coro::Task<int>> tasks = {t1(), t2(), t3(), t4()};

using coro::WaitMode;

// ① 任意一个完成(成败均可)就返回: 结果 vector 里只有那一个
auto first = co_await coro::wait_tasks(std::move(tasks), WaitMode::FirstCompleted);

// ② 任一失败立刻抛异常(其余后台继续); 全成功则等全部完成
auto all  = co_await coro::wait_tasks(std::move(tasks2), WaitMode::FirstException);

// ③ 等全部完成, 返回全部结果 (等价 gather_all)
auto done = co_await coro::wait_tasks(std::move(tasks3), WaitMode::AllCompleted);
```

| 模式 | 返回 | 失败行为 |
|---|---|---|
| `FirstCompleted` | 只含第一个完成者结果的 vector | 第一个完成的是失败 → 抛其异常 |
| `FirstException` | 全部结果（vector） | 任一失败**立即**抛，不再等其余 |
| `AllCompleted` | 全部结果（vector） | 全部完成后抛第一个异常 |

三种模式的共同点：`wait_tasks` **不取消**任何任务，落选者继续后台运行。

---

## 3.5 TaskGroup — 结构化并发（本讲重点）

`spawn` 的自由是有代价的：后台任务和主流程没有"从属关系"，
忘了收割就泄漏、忘了取消就僵死。**结构化并发**把并发的生命周期
约束到一个作用域里——Python 3.11 的 `asyncio.TaskGroup`，coro 全同款：

```cpp
// task_group.cpp
#include <coro/coro.hpp>
#include <iostream>

using namespace std::chrono_literals;

coro::Task<std::string> fetch(const std::string& name, int ms, bool fail = false)
{
    co_await coro::sleep(std::chrono::milliseconds(ms));
    if (fail) throw std::runtime_error(name + " 失败");
    co_return name + " OK";
}

coro::Task<> main_task()
{
    {
        coro::TaskGroup group;              // ① 建组
        group.spawn(fetch("用户", 300));     // ② 丢子任务, 立即启动
        group.spawn(fetch("帖子", 200));
        group.spawn(fetch("头像", 100, /*fail=*/true));

        try {
            co_await group.wait();          // ③ 等全部结束
            std::cout << "全部成功" << std::endl;
        } catch (const coro::ExceptionGroup& eg) {
            std::cout << "组内有失败, 共 " << eg.exceptions().size()
                      << " 个异常" << std::endl;
            for (auto& ep : eg.exceptions())
                try { std::rethrow_exception(ep); }
                catch (const std::exception& e) { std::cout << "  - " << e.what() << std::endl; }
        }
    }   // ④ 离开作用域: 析构兜底取消所有未完成子任务 (不会泄漏孤儿)
    std::cout << "到达这里时, 组内所有子任务都已结束" << std::endl;
}

int main() { coro::run(main_task()); }
```

### TaskGroup 的四条保证

1. **失败自动取消**：任一子任务失败 → 其余子任务全部被自动取消 →
   全部结束后抛 `coro::ExceptionGroup`。你永远不用手写
   "一个失败，取消其他" 的逻辑。
2. **`ExceptionGroup` 聚合**：所有子任务的真实异常打包在
   `eg.exceptions()` 里（`std::vector<std::exception_ptr>`，
   按发生顺序）；**单个异常也打包**，处理路径统一；
   因组取消而产生的 `CancelledError` 不算失败，不聚合。
3. **作用域退出即收干净**：忘了 `wait()` 也没关系，析构函数兜底取消
   全部未完成子任务——不会有孤儿协程逃出作用域。
4. **子任务任意返回类型**：`group.spawn()` 接受 `Task<T>`（任意 T），
   但子任务的**结果值**不能直接从组里拿——需要结果就通过参数引用 /
   共享状态带回（见下方模式）。

### 带回结果的惯用模式

```cpp
coro::Task<> fetch_into(std::string& out, std::string name, int ms)
{
    co_await coro::sleep(std::chrono::milliseconds(ms));
    out = name + " 的数据";          // 通过引用写回
}

coro::Task<> main_task()
{
    std::string users, posts;
    coro::TaskGroup group;
    group.spawn(fetch_into(users, "users", 300));
    group.spawn(fetch_into(posts, "posts", 200));
    co_await group.wait();           // 全部成功后, users/posts 已被填好
    // 单线程模型: 等待期间没有别的协程在写, 这里读是安全的
}
```

> 因为同一线程内是协作式调度，`group.wait()` 期间不会有协程"同时"
> 写 `users`——恢复写回都发生在确定的调度点上。这是单线程模型的福利。

---

## 3.6 选择指南：并发等待 API 速查

| 场景 | 用什么 |
|---|---|
| 固定几个任务，全都要结果 | `coro::gather` |
| N 个同类型任务，全都要结果 | `gather_all`（或 `wait_tasks + AllCompleted`） |
| 只关心 void 任务是否都完成 | `gather_void` |
| 超时控制 | `wait_for(task, timeout)` |
| 两路竞速 | `wait_any(a, b)` |
| N 路竞速 / 任一失败立刻抛 | `wait_tasks(tasks, WaitMode::...)` |
| 子任务失败要**自动取消兄弟**、生命周期绑定作用域 | **`TaskGroup`**（默认首选） |
| 后台任务，主流程稍后手动收 | `spawn` + 稍后 `co_await std::move(t)` |

经验法则：**优先 TaskGroup**，生命周期最安全；需要个别结果时
再降级到 gather 系；spawn 只用于真正的"脱离主流程"场景。

---

## 3.7 练习

1. 写一个每 500ms 打印一次的循环任务，用 `cancel()` 在 2 秒后停掉，
   要求清理逻辑打印"bye"。
2. 用 `wait_for` 给 `fetch_page` 加 300ms 超时，超时打印"重试"再试
   一次（共两次机会）。
3. 用 `TaskGroup` 并发跑 5 个任务，其中第 2 个故意抛异常，
   验证：其余任务被取消（在协程里 catch `CancelledError` 打印证据）、
   `ExceptionGroup` 里只有 1 个异常。
4. 用 `wait_tasks + FirstCompleted` 实现"3 个镜像源下载，谁先完成用谁"，
   打印获胜者编号。

---

**下一讲**：[同步原语与队列](04-sync-queue.md) —— 多个并发协程
怎么安全地共享资源、怎么搭生产者-消费者流水线。
