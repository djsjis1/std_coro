# 第 5 讲 — Future 与线程池：桥接两个世界

> 本讲目标：掌握两个"边界"工具——**Promise/Future**（把回调式 API
> 接进协程）和 **to_thread**（把阻塞函数挪到线程池）。
> 再顺手学会 `call_soon` / `call_later` 定时回调。

---

## 5.1 Promise / Future — 一次性完成通知

`Promise<T>` 是"写结果的口"，`Future<T>` 是"等结果的口"，
一次 set，多处可等：

```cpp
coro::Promise<std::string> promise;
auto future = promise.get_future();     // 可以多次 get_future → 多个 Future 共享结果

// ... 在将来的某处:
promise.set_value("结果");              // 或 promise.set_exception(std::current_exception());

// ... 在任意协程里:
std::string s = co_await future;        // 已设置 → 不挂起; 未设置 → 挂起等待
```

### 经典用法 ①：包装回调式（老）API

把"传回调给老库"变成"co_await 一个 Future"：

```cpp
// legacy: void legacy_download(const char* url, void(*cb)(const char* data, void* ud), void* ud);
coro::Task<std::string> download(const std::string& url)
{
    // Promise 本体放 shared_ptr: 回调触发时协程帧可能已挂起, 生命周期要独立
    auto p = std::make_shared<coro::Promise<std::string>>();
    auto fut = p->get_future();

    legacy_download(url.c_str(),
        [](const char* data, void* ud) {
            static_cast<coro::Promise<std::string>*>(ud)->set_value(data);
        }, p.get());
    // 注意: 真实代码里还要管理 p 的生命周期直到回调触发 (例如把 p 一起塞进 ud)

    co_return co_await fut;
}
```

### 经典用法 ②：跨线程一次性通知

`set_value` 可以从**任意线程**调用——它会自动把"唤醒"路由到
等待协程所在的事件循环（精确路由，不惊动别的线程）：

```cpp
coro::Task<> main_task()
{
    coro::Promise<int> p;
    auto fut = p.get_future();

    std::thread th([&p] {
        int result = heavy_blocking_work();     // 在别的线程干活
        p.set_value(result);                    // 线程安全, 自动唤醒协程
    });
    th.detach();

    int v = co_await fut;                       // 协程在这里挂起, 结果好了自动继续
    std::cout << "跨线程结果: " << v << std::endl;
}
```

### Future 细则

| 特性 | 说明 |
|---|---|
| 多等待者 | 多个协程可以 `co_await` 同一个 Future，set 时全部被唤醒 |
| 快速路径 | 已 set 再 await，**不挂起**（`await_ready` 直接通过） |
| 异常 | `set_exception(ep)` 存异常，await 处重抛；重复 set 抛 `std::logic_error` |
| 线程安全 | `set_value` / `set_exception` 任意线程可调；`co_await` 须在事件循环内 |
| void 版本 | `Promise<void>` / `Future<void>`：`set_value()` 无参 |

> Promise/Future 与 Event 的分工：Event 是**广播**（set 后人人通过），
> Future 是**带值的一次性交付**。要传数据，用 Future。

---

## 5.2 to_thread — 把阻塞代码丢给线程池

事件循环线程是独木桥，任何阻塞调用都会卡住**所有**协程。
`to_thread` 把一个函数扔进内置线程池执行，协程挂起等结果，
事件循环照常转：

```cpp
// 对标 asyncio.to_thread
int v = co_await coro::to_thread([] { return blocking_read_config(); });   // 返回值跨线程带回

// 没有返回值也行
co_await coro::to_thread([] { compress_big_file("dump.bin"); });

// 异常同样跨线程传播: 工作线程抛的, 在 co_await 处重抛
try {
    co_await coro::to_thread([]() -> int { throw std::runtime_error("磁盘炸了"); });
} catch (const std::exception& e) { /* 在协程里接住 */ }
```

### 使用纪律

1. **lambda 捕获安全**：函数体跑在**另一个线程**，捕获引用要小心生命周期
   （等 to_thread 返回前引用对象必须活着——`co_await` 期间协程帧活着，
   所以捕获协程内的局部变量是安全的）。
2. **不要在工作线程里碰协程**：不要 resume 句柄、不要操作
   事件循环的对象。要与协程世界通信，走 `Promise::set_value`
   （线程安全）这条正道。
3. **线程池是进程级单例**，默认 `hardware_concurrency` 个工作线程，
   程序退出时自动 join。CPU 密集的长任务会占着池子，
   影响其他 to_thread 调用——超大计算考虑第 8 讲的 Scheduler。

### 典型组合：异步外壳 + 阻塞内核

```cpp
// 同步的 SQLite/压缩/图片解码... 全都这样包一层
coro::Task<User> load_user(int id)
{
    User u = co_await coro::to_thread([id] { return db_query_user(id); });  // 阻塞 DB 调用
    co_return u;     // 数据库线程池化之后, 1 万个并发请求也只占几个线程
}
```

---

## 5.3 call_soon / call_later / call_at — 定时回调

不占协程槽位的轻量定时器（回调可以是普通函数，也可以是协程工厂）：

```cpp
coro::call_soon([] { std::cout << "下一轮循环执行" << std::endl; });

coro::call_later(500ms, [] { std::cout << "500ms 后执行" << std::endl; });

coro::call_later(1s, []() -> coro::Task<> {       // 协程回调: 到点启动并等待完成
    co_await periodic_cleanup();
});

coro::call_at(std::chrono::steady_clock::now() + 2s, [] { /* 指定时间点 */ });
```

- 回调在**当前事件循环线程**执行（单线程纪律照旧）；
- 无需管理生命周期：内部协程自持有，完成自动释放
  （这就是"fire-and-forget 的正确姿势"，普通 `spawn` 做不到自动自持有）；
- 需要可取消的定时等待？在协程里用 `wait_for` 包装
  `sleep`，或者对返回的 Task 整体 `cancel`。

---

## 5.4 实战：带超时的回调桥接

把三样东西串起来：Future 桥接 + to_thread + wait_for 超时：

```cpp
#include <coro/coro.hpp>
#include <iostream>

using namespace std::chrono_literals;

// 模拟一个阻塞的老库函数: 忙 2 秒后把结果写进回调
void legacy_query(int (*cb)(void*, int), void* ud) { /* 真实老库 */ }

// 桥接: 回调 → Promise → Future → 协程
coro::Task<int> query_async()
{
    coro::Promise<int> p;
    auto fut = p.get_future();

    // 老库可能内部开线程; 这里简单起见用 to_thread 模拟"它不会立刻完成"
    co_await coro::to_thread([&p] {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));  // 模拟耗时
        p.set_value(42);
    });

    co_return co_await fut;
}

coro::Task<> main_task()
{
    try {
        int v = co_await coro::wait_for(query_async(), 1s);
        std::cout << "结果: " << v << std::endl;
    } catch (const coro::TimeoutError&) {
        std::cout << "超时!" << std::endl;
    }
}

int main() { coro::run(main_task()); }
```

> 注意桥接内部的 `to_thread` 拿到结果后 `set_value`，即使外层
> `wait_for` 超时取消了 `query_async`，Promise 照样能安全 set——
> 没人等的 Future 不会爆炸（结果写进共享状态，随 Future 析构释放）。

---

## 5.5 练习

1. 给一个同步函数 `int fib(int n)`（n=35，纯 CPU）做两个版本：
   a) 直接在协程里调用，观察事件循环被卡住（另一个 100ms 心跳协程停止跳动）；
   b) 用 `to_thread` 包装，心跳继续跳动。体会"阻塞内核必须出桥"的道理。
2. 写一个 `Task<std::string> getenv_async(std::string name)`：
   用 `to_thread` 调 `std::getenv`（模拟阻塞环境变量查询），
   找不到就抛 `std::runtime_error`。
3. 用 `call_later` 实现一个"5 秒后自动退出"的看门狗，
   配合第 3 讲的 `cancel()`：主流程 3 秒完成则看门狗无事发生，
   超时则看门狗取消主流程。
4. （思考）Promise 的 `set_value` 从线程 A 调用、协程在线程 B 的
   事件循环里等待——值是怎么"过去"的？哪一步加了锁？
   （答不上来可以先读 [架构剖析](../architecture.md) 4.x 节。）

---

**下一讲**：[TCP 网络编程](06-networking.md) —— 真正的异步 IO：
写一个能同时服务上千连接的 echo 服务器。
