# 第 8 讲 — 多核并行：loop-per-thread 与 Scheduler

> 本讲目标：前面所有协程都跑在**一个线程**上（吃满一个核）。
> 本讲给出两套多核方案：**手动 loop-per-thread**（对标 asyncio 的
> "每线程一个 loop"）和 **Scheduler 自动分发**（像丢 goroutine 一样
> 丢任务）。以及三者（事件循环 / to_thread / Scheduler）的选型。

---

## 8.1 先明确：什么时候需要多核？

| 负载类型 | 正确工具 | 是否需要本讲 |
|---|---|---|
| 网络代理 / API 网关 / 聊天服务器（IO 密集） | 单线程事件循环 | 通常不需要 |
| 大量阻塞系统调用（DB 驱动、加密） | `to_thread`（第 5 讲） | 通常不需要 |
| CPU 密集计算（哈希、压缩、AI 推理前后处理） | 本讲 | **需要** |
| 单进程万级活跃连接 + 每连接有真实计算 | 本讲 | **需要** |

单线程协程的吞吐上限 ≈ 一个核的指令吞吐。当 profile 显示
事件循环线程跑满 100% CPU 且协程队列积压，才升级到本讲。

---

## 8.2 方案一：loop-per-thread（手动多核）

每个线程运行**自己独立的事件循环**，各占一核，互不干扰：

```cpp
// workers.cpp
#include <coro/coro.hpp>
#include <iostream>
#include <thread>
#include <vector>

using namespace std::chrono_literals;

coro::Task<> cpu_worker(int id)
{
    for (int round = 0; round < 5; ++round) {
        // 模拟 CPU 计算 (协作式: 别忘了在真计算里周期性让出)
        long long sum = 0;
        for (int i = 0; i < 20'000'000; ++i) sum += i;
        std::cout << "worker " << id << " round " << round
                  << " sum=" << (sum % 100) << std::endl;
        co_await coro::yield();              // 让同线程其他协程有机会跑
    }
}

void thread_main(int id)
{
    auto t = cpu_worker(id);
    t.start();                               // 在"本线程的 loop"上启动
    coro::EventLoop::get().run();            // 运行本线程的事件循环 (惰性创建)
}

int main()
{
    std::vector<std::thread> threads;
    for (int i = 0; i < 4; ++i)
        threads.emplace_back(thread_main, i);
    for (auto& t : threads) t.join();
}
```

### 规则

- `EventLoop::get()` 返回**当前线程**的 loop（不存在则惰性创建）。
  每线程一个，互不共享。
- **协程不能跨线程迁移**：在哪个线程启动，就一辈子在那个线程跑。
  协程帧的内存归属也属于那个线程。
- 同一线程内仍是单线程协作语义——该线程内的共享数据无需加锁；
  跨线程的共享数据照常需要 `std::mutex` / atomic。
- `coro::sleep`、锁、队列等原语自动使用**当前线程**的 loop，
  线程内写法与单线程完全相同。

### 跨线程通信：Promise 自动路由

`Promise::set_value` 可以从任意线程调用，等待协程会被精确唤醒到
**它所在的** loop（而不是 set 的线程）：

```cpp
coro::Task<> main_task()
{
    coro::Promise<int> p;
    auto fut = p.get_future();

    // 线程 A 的协程等待; 线程 B set
    std::thread th([&p] { p.set_value(42); });   // 线程安全
    th.detach();

    int v = co_await fut;                        // 挂起; set 后本协程被路由回自己的 loop
    std::cout << v << std::endl;
}
```

同样的自动路由贯穿全库：任务完成的 continuation、跨线程 `cancel()`、
子进程退出通知——都会把唤醒送到"协程的家"。

### 与 Go 的区别（为什么不做 work-stealing？）

Go 的调度器会把 goroutine 在核间**迁移**（work-stealing），
代价是协程栈/数据必须支持被任意线程触碰。coro 选择
**线程亲缘**（任务不迁移）：实现简单、行为可预测、同线程内
不需要任何同步原语——与 asyncio 的心智模型一致。
需要并行时显式分线程（8.2）或用 Scheduler（8.3）。

---

## 8.3 方案二：Scheduler — 自动多核分发

不想手动管线程？`Scheduler` 开 N 个常驻 worker（默认 = 核数），
任务自动分发给最闲的 worker：

```cpp
// scheduler_demo.cpp
#include <coro/coro.hpp>
#include <iostream>
#include <vector>

using namespace std::chrono_literals;

coro::Task<int> handle(int req)
{
    co_await coro::sleep(50ms);          // 模拟每请求 50ms 计算
    co_return req * 2;
}

int main()
{
    coro::Scheduler sched;               // 默认 = CPU 核数个 worker 线程

    std::vector<int> requests(100);
    for (int i = 0; i < 100; ++i) requests[i] = i;

    for (int req : requests)
        sched.spawn_any([req] { return handle(req); });   // ① 丢进去就完事

    sched.wait_all();                    // ② 阻塞直到全部完成
    std::cout << "全部完成 (worker 数: " << sched.worker_count() << ")" << std::endl;
}
```

100 个 50ms 的任务，总耗时 ≈ 100/核数 × 50ms（而不是 5 秒串行）。

### 三条铁律

1. **spawn_any 接受"工厂"而非现成任务**：

   ```cpp
   sched.spawn_any([req] { return handle(req); });   // ✅ 工厂: 在 worker 线程内调用
   // sched.spawn(handle(req));                      // ❌ 没有这个 API, 也不该有
   ```

   原因：协程帧必须在**执行它的线程**上创建和销毁（无栈协程的帧
   分配/释放发生在调用工厂的线程）。跨线程传递已创建的 Task
   会破坏帧内存归属（MSVC Debug CRT 直接堆断言）。

2. **协程亲和**：任务绑定 worker 后不迁移。该 worker 内仍是
   单线程语义，同步原语零改动可用。

3. **worker 常驻**：Scheduler 构造即开线程跑各自的 loop，
   析构时 stop + join。适合"程序生命周期 = 服务生命周期"的场景。

### 高级：拿到 worker 的 loop

```cpp
coro::EventLoop* loop = sched.loop_at(0);   // 第 0 个 worker 的 loop
loop->dispatch([] { /* 在该 worker 线程执行的普通函数 */ });
```

### Windows 网络注意

socket 与创建它的 loop 绑定（IOCP 关联）。accept 出来的连接要丢给
Scheduler 的 worker 处理时，用 `accept_noattach()` accept，
在 worker 协程内 `reattach()` 到 worker 的 IOCP
（io_uring 平台 fd 即用，无此约束）。项目自带的
Web 框架就是这么做的（第 9 讲）。

---

## 8.4 三种并行工具选型

| 工具 | 模型 | 适用 | 注意 |
|---|---|---|---|
| 事件循环（默认） | 单线程协作 | IO 密集；一切默认场景 | 协程体内别写死循环 |
| `to_thread` | 线程池执行阻塞函数 | 阻塞 API、少量 CPU 任务 | 函数体别碰协程/loop；结果走返回值 |
| loop-per-thread / `Scheduler` | 每核一个 loop | CPU 密集、大规模并发计算 | 工厂模式；跨线程共享数据要加锁 |

组合拳（真实服务常见形态）：

```
线程 1        事件循环: accept + 协议解析 (IO)
线程池        to_thread: 阻塞 DB 查询
worker × N    Scheduler: CPU 密集处理
```

---

## 8.5 练习

1. 把 8.2 的 cpu_worker 跑在 4 个线程上，用 `std::chrono` 验证
   总耗时 ≈ 单线程的 1/4。
2. 用 Scheduler 并发计算 1000 个数的 `std::hash` 大循环，
   对比单线程（把循环直接写在协程里）的耗时。
3. （思考）两个 worker 各跑一个协程，同时 `++` 同一个全局 int
   会发生什么？该怎么修？（答案：数据竞争——线程亲缘不豁免
   **跨线程**共享，用 `std::atomic` 或互斥锁。）
4. （探索）读 `tests/test_scheduler.cpp` 和
   `tests/test_multithread.cpp`，看官方如何测试负载均衡与亲和性。

---

**下一讲**：[构建 HTTP 服务](09-web-server.md) —— 用项目自带的
Web 框架把协程网络能力变成可部署的 HTTP 服务。
