# 第 4 讲 — 同步原语与队列

> 本讲目标：掌握 `Lock` / `Semaphore` / `Event` / `Condition` / `Queue`
> 五个协程同步原语。它们全部**只在事件循环内使用**（不是线程锁），
> 用于协调**同一事件循环里**的多个协程。

---

## 4.0 先搞清楚：为什么要"协程锁"？

第 1 讲说过"协程间共享数据不用加锁"——准确说是：
**不含 await 点的临界区天然原子**。一旦临界区里有 `co_await`，
别的协程就会插进来，数据就脏了：

```cpp
coro::Task<> unsafe_transfer(Account& from, Account& to, int amount)
{
    int balance = from.balance;          // 读
    co_await coro::yield();              // ← 挂起点! 其他协程在这里改了 from.balance
    from.balance = balance - amount;     // 写回了脏数据 (丢失更新)
    to.balance += amount;
}
```

需要跨 await 点保护状态时，用本讲的协程锁——它"锁住"的是
**协程的执行权**，`co_await lock.acquire()` 等锁期间协程挂起、
不占线程，这与 `std::mutex`（自旋/睡眠阻塞**整个线程**，
且绝不能在协程中跨 await 持有）有本质区别。

> ⚠️ `std::mutex` 等**线程**同步工具在协程里基本只能用于
> "永不跨 await 的短临界区"，而且不能与协程锁混用。
> 单线程事件循环里 99% 的互斥需求都应该用 `coro::Lock`。

---

## 4.1 Lock — 协程互斥锁

```cpp
coro::Lock lock;

// 写法一: RAII 守卫 (推荐, 对标 async with lock:)
{
    auto g = co_await lock.guard();      // 等锁 + 拿到守卫
    /* 临界区: 可以安全地跨 await 持有 */
    co_await do_something_slow();
}                                        // 离开作用域自动释放 (异常路径也释放)

// 写法二: 手动
co_await lock.acquire();
/* ... */
lock.release();                          // 忘了 release 就死锁, 推荐还是用 guard
```

特性：

- **FIFO 公平**：先来先得，不插队；
- **支持递归**：同一协程重复 acquire 会增加递归计数，必须对应次数 release；
- `lock.is_locked()` 可查询；
- 释放时若有等待者，锁**直接移交**队首等待者（无竞态窗口）。

---

## 4.2 Semaphore — 并发数限制

最常用的场景：**限制并发度**。比如"最多同时 3 个下载"：

```cpp
// semaphore.cpp
#include <coro/coro.hpp>
#include <iostream>
#include <vector>

using namespace std::chrono_literals;

coro::Semaphore sem(3);                  // 最多 3 个协程同时进入

coro::Task<> download(int id)
{
    co_await sem.acquire();              // 拿不到就挂起排队
    std::cout << "下载 " << id << " 开始 (并发中)" << std::endl;
    co_await coro::sleep(300ms);         // 模拟下载
    std::cout << "下载 " << id << " 完成" << std::endl;
    sem.release();                       // 用完归还
}

coro::Task<> main_task()
{
    std::vector<coro::Task<>> tasks;
    for (int i = 1; i <= 10; ++i)
        tasks.push_back(download(i));
    co_await coro::gather_void(std::move(tasks)...);   // 见下方完整写法
}

int main() { coro::run(main_task()); }
```

上面 `gather_void(std::move(tasks)...)` 的展开写法在真实代码中不成立，
vector 场景请用第 3 讲的 `wait_tasks` 或手动组织。完整可编译版本：

```cpp
coro::Task<> main_task()
{
    std::vector<coro::Task<>> tasks;
    for (int i = 1; i <= 10; ++i)
        tasks.push_back(download(i));
    co_await coro::wait_tasks(std::move(tasks), coro::WaitMode::AllCompleted);
    // 观察: 10 个下载始终只有 3 个在并发
}
```

Semaphore 也有 RAII 守卫（异常路径安全）：

```cpp
{
    auto g = co_await sem.guard();
    /* 并发受限区间 */
}
sem.available();   // 查询剩余许可数
```

---

## 4.3 Event — 一次性事件广播

"等某件事发生"，多个等待者同时被唤醒：

```cpp
coro::Event ready;

coro::Task<> consumer()
{
    std::cout << "等待数据就绪..." << std::endl;
    co_await ready.wait();               // 挂起直到 set
    std::cout << "数据来了, 开始处理" << std::endl;
}

coro::Task<> producer()
{
    co_await coro::sleep(500ms);         // 模拟准备数据
    ready.set();                         // 唤醒所有等待者
}
```

- `set()` 之后，**所有** `wait()` 立即通过（包括之后才 wait 的）——
  事件是"粘性"的；
- `clear()` 可复位；
- 多个协程等待同一个 Event 完全合法（广播语义）。
- 只需要"通知一个特定协程"时，用 `Promise`/`Future`（第 5 讲）更合适。

---

## 4.4 Condition — 条件变量

"等某个**条件**成立"（而不仅仅是某个事件发生过）。和 `Lock` 配对使用，
必须先持锁再 wait：

```cpp
coro::Lock lock;
coro::Condition cond(&lock);
bool queue_has_work = false;

// 等待方: 标准的 while 循环 (防虚假唤醒, 与线程条件变量相同的纪律)
coro::Task<> wait_for_work()
{
    auto g = co_await lock.guard();
    while (!queue_has_work)
        co_await cond.wait();            // 原子地: 释放锁 + 挂起; 唤醒后重新拿锁返回
    // 这里持着锁且条件成立
    queue_has_work = false;
}

// 通知方
coro::Task<> produce_work()
{
    auto g = co_await lock.guard();
    queue_has_work = true;
    cond.notify();                       // 唤醒 1 个等待者; notify_all() 唤醒全部
}
```

`cond.wait()` 的取消是安全的：被取消时会先重新拿回锁、再传播
`CancelledError`，保证守卫析构时锁恰好释放一次。

> 简单场景其实可以不用 Condition：轮询 + `yield`/`sleep` 也能写，
> 但忙等浪费调度次数；Condition 是零忙等的正确工具。

---

## 4.5 Queue — 生产者-消费者

协程库里使用频率最高的原语。有界队列自带**背压**（backpressure）：
队列满了生产者自动挂起，空了消费者自动挂起：

```cpp
// queue.cpp
#include <coro/coro.hpp>
#include <iostream>
#include <string>
#include <vector>

using namespace std::chrono_literals;

coro::Task<> producer(coro::Queue<int>& q)
{
    for (int i = 1; i <= 5; ++i) {
        co_await q.put(i);               // 满则挂起 (背压)
        std::cout << "生产 " << i << std::endl;
        co_await coro::sleep(50ms);
    }
    // 生产完毕。两种收尾方式:
    //   a) 关闭语义: 消费者循环直到 get 不到 → 用 sentinel 或下面 4.5.1 的模式
    //   b) join 协议: 生产者不关队列, 由等待方 join (本例用 join)
}

coro::Task<> consumer(coro::Queue<int>& q)
{
    while (true) {
        auto v = co_await q.get();       // 空则挂起
        std::cout << "  消费 " << v << std::endl;
        q.task_done();                   // ← 处理完一个, 记一笔
    }
}

coro::Task<> main_task()
{
    coro::Queue<int> q(3);               // 有界: 最多 3 个元素
    auto c = coro::spawn(consumer(q));

    co_await producer(q);                // 先生产
    co_await q.join();                   // 等所有已 put 的元素都被 task_done
    // join 不会唤醒卡在 get() 的消费者 → 结束消费者:
    c.cancel();                          // 消费者在下一个 await 点收到 CancelledError
    co_await std::move(c);
    std::cout << "流水线关闭" << std::endl;
}

int main() { coro::run(main_task()); }
```

### API 速览

| API | 语义 |
|---|---|
| `Queue<T> q(n)` | 有界队列（容量 n）；`Queue<T> q` 或 `q(0)` = 无界 |
| `co_await q.put(x)` | 入队；满则挂起 |
| `co_await q.get()` | 出队（返回 T）；空则挂起 |
| `q.get_nowait()` | 非阻塞取，空返回 `std::nullopt` |
| `q.put_nowait(x)` | 非阻塞放，满返回 false |
| `q.task_done()` | 标记一个已取出的元素处理完毕 |
| `co_await q.join()` | 挂起直到所有已 put 元素都被 task_done（ unfinished 归零） |
| `q.size()` / `q.empty()` / `q.full()` / `q.unfinished_count()` | 查询 |

### 4.5.1 消费者退出：两种模式

**模式 A：sentinel（哨兵值）**

```cpp
coro::Task<> consumer(coro::Queue<int>& q)
{
    while (true) {
        int v = co_await q.get();
        if (v == -1) break;              // 哨兵: 生产者 put(-1) 表示收工
        q.task_done();
    }
}
```

**模式 B：cancel（上一例的用法）**

消费者是纯循环时，`cancel()` 就是标准的退出方式——
它同样会执行消费者的清理路径。两种都符合 Python 社区惯例，
哨兵适合"数据流有自然结束"的场景，cancel 适合"服务型常驻消费者"。

### 4.5.2 流水线

Queue 天然适合多级流水线（每级之间一个队列，级与级解耦）：

```
[读取] → Queue → [解析] → Queue → [写库]
```

每一级内部还可以配合 `Semaphore` 控制并发度。

---

## 4.6 全部原语的取消安全与僵尸清理

五个原语都实现了**等待者销毁回调**：若一个协程在排队等待锁/信号量/
队列时被取消（帧被销毁），库会自动把它从等待队列里摘除，
不会出现"唤醒一个已销毁的协程"的 UB。你只管用 `cancel()` 和 RAII，
队列卫生是库的事。

---

## 4.7 练习

1. 用 `Lock` 修复 4.0 的 `unsafe_transfer`，让两个协程并发转账
   100 次后余额严格守恒。
2. 把 Semaphore 例子的并发度改成 1 和 5，观察吞吐变化。
3. 搭一条三级流水线：生成 1~20 的数字 → 平方 → 打印，
   每级一个协程、级间一个有界队列（容量 2），最后用 join + cancel 收尾。
4. （思考）Event 和 Condition 的区别是什么？
   各举一个"用错了会出 bug"的例子。
   （提示：Event 是"发生过"，Condition 是"当前状态成立"。）

---

**下一讲**：[Future 与线程池](05-future-thread.md) —— 怎么把老式
回调 API、阻塞库函数接进协程世界。
