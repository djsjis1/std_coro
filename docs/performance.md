# 性能指南

> coro 的性能模型、实测参考数据、压测复现方法与调优建议。
> 所有数字来自 `tests/stress.cpp`（Release 构建，Windows/IOCP，
> 具体数值随机器浮动——重要的是数量级与相对关系）。

---

## 1. 数量级心智模型

做任何优化决策前，先记住这张成本表：

| 操作 | 量级 | 对比 |
|---|---|---|
| 创建 + 运行一个协程 | ~百 ns 级（一次堆分配 + 帧初始化） | 线程创建 ~10µs+，差 2~3 个数量级 |
| `co_await coro::yield()` | ~160 ns | 一次虚函数调用的几倍 |
| 队列 put/get 吞吐 | ~37 ns/元素 | 接近裸 mutex 队列 |
| `co_await sleep` 挂起+唤醒 | ~µs 级（定时器堆操作） | 线程 sleep ~10µs+ 系统调用 |
| 10 万协程同时 sleep(1ms) | ~150 ms 全部完成 | 同规模线程池无法企及 |
| OS 系统调用（IOCP/io_uring） | ~1 µs | IO 的真正成本在这里 |
| 网络往返（本机） | ~50 µs | 框架开销可忽略 |

**结论**：协程框架本身很少是瓶颈。协程程序的性能问题 90% 出在
①阻塞了事件循环线程、②多余的内存拷贝、③锁/系统调用滥用。

---

## 2. 参考实测数据（coro_stress）

构建与运行：

```bash
cmake --build build --config Release --target coro_stress
./build/Release/coro_stress.exe
```

| 场景 | 内容 | 参考结果* | 0.3 优化前* |
|---|---|---|---|
| 1 | 10 万协程同时 `sleep(1ms)` | ~150 ms 全部完成 | ~224 ms |
| 2 | 10 万定时器（1~10ms 分散 deadline） | ~165 ms | ~259 ms |
| 3 | yield 风暴：4 协程 × 100 万次 | ~160 ns/次 | ~190 ns/次 |
| 4 | 队列吞吐：100 万 put/get | ~37 ns/元素 | ~39 ns/元素 |
| 5 | spawn + await 往返 10 万次 | ~670 ns/次 | ~760 ns/次 |
| 6 | 4 线程 × 10 万协程（独立 loop） | ~250 ms | ~310 ms |
| 7 | Scheduler 分发 40 万协程（4 worker） | ~300 ms | ~430 ms |
| 8 | Scheduler 均衡性 | 4 worker 各 ~1000 | 同左 |

\* 开发机参考值（Release，Windows/IOCP）。你机器上的绝对值会不同，
跑一遍得到自己的基线。

场景 8 验证负载均衡：`spawn_any` 按"活跃协程数（主）+ 累计分发数
（辅）"选 worker，短任务场景退化为 round-robin，四个 worker
大致均分。

### 2.1 0.3 版本优化记录（协程创建热路径去堆分配）

上表"0.3 优化前"列是同一台机器上优化前的实测。两处改动：

1. **Task 帧存活标志去 `shared_ptr`**：旧版 promise 内嵌
   `shared_ptr<bool> frame_alive_`——每个协程创建一次堆分配 +
   Task 构造/析构各一次原子引用计数。逐路径审计确认
   「`Task::handle_` 非空 ⟺ 帧存活」不变量已由
   `release_handle()` 维持后整体移除
   （详见[架构文档 4.3 节](architecture.md)）。
   影响所有场景，协程创建密集型（场景 1/2/7）收益最大（-30%+）。
2. **组合子去自引用堆分配**：`wait_for` / `wait_any` /
   `gather_all` / `wait_tasks` / `gather_void` / `call_soon` /
   `call_later` / `call_at` 不再为每个 monitor/定时器协程
   `make_shared<Task>` 保活，改为协程帧局部对象 +
   `start()+detach()`（帧自持有运行到完成）。

### 2.2 0.4 版本优化记录（就绪队列去分配 + Web 层请求热路径）

1. **就绪队列 vector 化**：`std::queue<deque>` → `HandleQueue`
   （vector + 头索引 + 成员 batch 容量复用）。MSVC deque 对 8 字节的
   `coroutine_handle` 每个 16 字节块只装 2 个 —— 旧实现每 2 次 push
   一次堆分配; 现在 batch 与 ready_queue_ 经 swap 往复保留容量,
   **稳态调度零堆分配**。影响所有场景, yield 风暴 / 高频 spawn 收益最大。
2. **Web 层每请求约 -20 次堆分配**：
   - 请求头 `std::map` 深拷贝 → 整表 `std::move`（llhttp 在下一条
     message_begin 会清空容器, move 安全）;
   - `http_response::file()` 由 `istreambuf_iterator` 逐字节改为
     `tellg` 定长 + 单次 `read`（快约一个数量级; 异步路径已用
     `coro::fs::read_all`）。
3. **构建默认**：单配置生成器（Ninja/Makefile）默认 Release;
   `web_server` 目标开启 LTCG（header-only 库的跨 TU 内联）。

---

## 3. 压测覆盖了什么、没覆盖什么

**覆盖**：调度器热路径（就绪队列、定时器堆、幂等去重）、
队列吞吐、spawn 生命周期往返、多 loop 扩展性、Scheduler 分发。

**未覆盖**（需要按业务自行基准）：

- 网络吞吐/延迟（用 `examples/echo_server.cpp` 加统计改造）；
- 文件 IO（业务块大小差异巨大）；
- 真实协议的每连接内存（协程帧 + 缓冲区 + socket 内核缓冲）。

---

## 4. 常见性能反模式与解法

### 4.1 阻塞事件循环线程（最常见）

```cpp
// ❌ 全世界等你 50ms
coro::Task<> bad() {
    std::this_thread::sleep_for(50ms);
    heavy_hash(1GB);
}

// ✅ 协程等待 / 阻塞内核出桥
coro::Task<> good() {
    co_await coro::sleep(50ms);
    co_await coro::to_thread([] { heavy_hash(1GB); });
}
```

症状识别：其他协程"心跳"停跳、`stats` 端点无响应、
`active_task_count` 不降——都是单线程被卡住的信号。

### 4.2 同步原语用错层

- 协程间互斥用 `coro::Lock`（挂起，不占线程），不要用 `std::mutex`
  硬扛；
- 跨线程共享计数用 `std::atomic`（无需事件循环介入）；
- 高频跨线程一次性事件用 `Promise`（精确唤醒）而不是轮询 +
  `sleep(1ms)`。

### 4.3 无谓的 wake 风暴

轮询模式（`sleep(10ms)` + 检查标志）简单但烧调度次数。
有事件原语就别轮询：Event / Condition / Queue 都是零忙等挂起。
"事件 + 超时"混合场景用 `wait_for(ev.wait(), 200ms)`，
超时频率控制在你真正需要的检查频率。

### 4.4 大 buffer 进协程帧

`co_await` 挂起点存活的局部变量都住在**协程帧**（堆）里。
1MB 的 `char buf[1MB]` 会让每个连接协程占 1MB 堆——
万级连接直接 OOM。大缓冲用堆容器（vector/string 成员或
`unique_ptr<char[]>`），帧里只留指针：

```cpp
coro::Task<> handle(coro::net::TcpStream conn)
{
    auto buf = std::make_unique<char[]>(256 * 1024);  // 帧内只有 unique_ptr
    int n = co_await conn.read(buf.get(), 256 * 1024);
    ...
}
```

### 4.5 过细的并发粒度

一百万个"做一件事就结束"的微协程，帧分配/销毁开销占比会升高
（场景 5 的 spawn 往返是 µs 级）。批处理化（一个协程处理一批）
通常更快。先量化再优化：把 spawn 往返打点对比业务真实耗时。

### 4.6 gather 了不该并发的任务

对同一把 `coro::Lock` 保护下的操作做 `gather`，结果是串行 + 等待
开销。锁竞争烈的路径先考虑消除共享（分片、每协程私有缓冲）。

---

## 5. 调优工具箱

### 5.1 限制并发度（保护下游）

```cpp
coro::Semaphore sem(64);                       // 最多 64 并发
// 或令牌桶式: Semaphore + sleep 补充
```

压测时用 Semaphore 扫参数（32/64/128/256），找吞吐拐点。

### 5.2 多核扩展的时机

顺序：先确认单线程真的满了（任务管理器看事件循环线程 CPU 100%，
且 `to_thread` 已把阻塞工作挪走）→ 再上 `Scheduler`
（[第 8 讲](tutorial/08-multicore.md)）。Scheduler 的 worker 数
默认 = 核数；IO 与 CPU 混合负载可以调小 worker 数给内核留头。

### 5.3 观测手段

```cpp
// 编译时定义 CORO_TASK_REGISTRY 启用任务注册表
coro::EventLoop::get().active_task_count();    // 当前活跃协程数
coro::EventLoop::current_task();               // 当前协程句柄 (调试器里看)
```

- 每个 worker 的 `active_task_count()` 可以判断 Scheduler 是否倾斜；
- 打点用 `std::chrono::steady_clock`，别在热路径用系统时钟。

### 5.4 编译选项

- **Release + /O2 或 -O2**：协程内联优化影响巨大（Debug 差 5~10 倍
  很正常）；
- MSVC：`/utf-8`（本项目已在用）；LDLT 之外，
  关闭 /RTC 对 Debug 压测的干扰没有意义——压测永远用 Release；
- GCC/Clang：`-fcoroutines`（<14）+ `-O2`；
- MSVC + CMake：单配置生成器已默认 Release（根 CMakeLists）；
  `web_server` 已开 `INTERPROCEDURAL_OPTIMIZATION`（LTCG）——
  header-only 热路径分布在多个 TU，链接时内联收益明显。

---

## 6. 协程帧成本补充（想深入再读）

一次 `co_await` 挂起/恢复的路径：

```
挂起: awaiter 记录 continuation → 控制权返回事件循环  (无系统调用)
恢复: 定时器到期 / IO 完成包 / 别的协程 schedule
      → 就绪队列 → h.resume() → 从挂起点继续
```

单线程内挂起/恢复**没有任何原子操作与系统调用**（同线程
`schedule` 不 wake），这就是 ~160ns yield 的来源。跨线程唤醒才
涉及 atomic exchange + 一次 PostQueuedCompletionStatus/NOP SQE。

与线程对比：线程切换 ~1-10µs + 缓存污染；协程切换 ~tens of ns +
无栈帧（协程帧只在挂起点存活的部分进堆）。这就是"协程可以开
几十万个，线程只能开几千个"的物理基础。

更多帧布局与 HALO（堆分配消除）等语言层话题，见
[协程课程第 13 讲](cpp20-coroutines-course/13-performance.md)。
