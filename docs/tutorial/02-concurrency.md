# 第 2 讲 — 并发编程：spawn 与 gather

> 本讲目标：掌握三种并发形态——**spawn**（后台任务）、
> **gather**（并发等待一组）、**gather_all / gather_void**（动态数量）。
> 学完你能把"串行 600ms"改成"并发 300ms"。

---

## 2.1 串行 vs 并发

串行等待（上一讲）：

```cpp
auto a = co_await fetch("/api/users");    // 300ms
auto b = co_await fetch("/api/posts");   // 200ms
auto c = co_await fetch("/api/photos");  // 100ms
// 总耗时 600ms — 三个请求排队
```

但三个网络请求之间**没有依赖**，应该同时出发。总耗时取决于最慢的
那个（300ms），而不是总和。coro 提供三种并发组织方式，对应 Python：

| 需求 | Python | coro |
|---|---|---|
| 后台启动，稍后取结果 | `task = asyncio.create_task(f())` | `auto t = coro::spawn(f());` |
| 并发等待固定几个（类型可不同） | `await asyncio.gather(a(), b())` | `co_await coro::gather(a(), b())` |
| 并发等待运行时数量（同类型） | `await asyncio.gather(*coros)` | `co_await coro::gather_all(std::move(tasks))` |

---

## 2.2 gather — 并发等待固定数量

```cpp
// gather.cpp
#include <coro/coro.hpp>
#include <iostream>
#include <string>

using namespace std::chrono_literals;

coro::Task<std::string> fetch(const std::string& name, int delay_ms)
{
    std::cout << "  请求 " << name << " 出发" << std::endl;
    co_await coro::sleep(std::chrono::milliseconds(delay_ms));
    co_return "[" + name + " 的数据]";
}

coro::Task<> main_task()
{
    auto t0 = std::chrono::steady_clock::now();

    // 三个请求同时出发, 结构化绑定接收结果 (顺序 = 参数顺序)
    auto [users, posts, photos] = co_await coro::gather(
        fetch("users", 300),
        fetch("posts", 200),
        fetch("photos", 100));

    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                  std::chrono::steady_clock::now() - t0).count();
    std::cout << users << posts << photos << std::endl;
    std::cout << "总耗时 " << ms << "ms (不是 600ms)" << std::endl;
}

int main() { coro::run(main_task()); }
```

输出：

```
  请求 users 出发
  请求 posts 出发
  请求 photos 出发
  [photos 的数据][posts 的数据][users 的数据]
总耗时 300ms (不是 600ms)
```

### gather 的行为细则

1. **返回 `std::tuple<Ts...>`**，顺序与传入的任务一致，类型可以各不相同：

   ```cpp
   auto [name, count] = co_await coro::gather(
       fetch_name(),    // Task<std::string>
       fetch_count());  // Task<int>
   ```

2. **所有任务在 gather 处一起启动**（惰性 Task 的好处：先收集，再同时点火）。

3. **异常语义（对标 `asyncio.gather` 默认行为）**：
   任一任务抛异常 → **其余任务继续跑完**（不取消）→ 全部结束后，
   在 `co_await gather` 处重抛**第一个**异常：

   ```cpp
   try {
       auto [a, b] = co_await coro::gather(ok_task(), bad_task());
   } catch (const std::exception& e) {
       // bad_task 的异常; ok_task 已经跑完, 结果被丢弃
   }
   ```

4. **限制**：`Task<void>` 不能进 `gather`（void 没法放进 tuple）。
   void 任务用 `gather_void`（2.5 节）；`Task<>` 想混入 gather 也可以
   —— gather 支持 `Task<void>` 参与（该位置仅用于等待，无结果值）。

---

## 2.3 spawn — 后台任务

`gather` 是"一起等"，`spawn` 是"先让它跑着，我干别的，之后再来收"：

```cpp
coro::Task<> main_task()
{
    // 立即启动两个后台协程 (对标 asyncio.create_task)
    auto t1 = coro::spawn(worker(1));   // worker(1) 已经开始跑
    auto t2 = coro::spawn(worker(2));

    std::cout << "主协程先干点别的" << std::endl;
    co_await coro::sleep(50ms);

    int v1 = co_await std::move(t1);    // 注意 std::move: spawn 返回的是"活的"任务
    int v2 = co_await std::move(t2);
    std::cout << "结果: " << v1 << ", " << v2 << std::endl;
}
```

### spawn 三条纪律

1. **必须保存返回值**。`coro::spawn(worker(1));`（不接返回值）会让
   Task 临时对象立刻析构 → 协程帧被销毁 → 后台任务直接消失
   （这是 C++ 协程生命周期的经典坑，见 [FAQ](../faq.md)）。

2. **等待时用 `co_await std::move(t)`**。spawn 回来的任务已经启动，
   `co_await` 一个左值也可以（库内部处理了），但按惯例统一 move。

3. **`coro::run` 只等主协程**。主协程完成了，但 spawn 出去的后台任务
   还没完成时，事件循环会继续跑完所有活跃协程才退出
   （`run` 的退出条件是"没有工作可做"）；但**结果没人接收**，
   若抛异常只会打印一条警告。所以主协程应该负责收割它 spawn 的任务
   ——或者用第 3 讲的 `TaskGroup`。

### 什么时候用 spawn，什么时候用 gather？

- 结果之间**无先后依赖**、要"同时等完" → `gather`（最常用、最安全）。
- 需要"先启动、中间做别的事、稍后再收" → `spawn` + 稍后 `co_await`。
- 需要"谁先完成用谁"或"超时杀掉" → 第 3 讲的 `wait_any` / `wait_for`。

---

## 2.4 gather_all — 运行时数量的并发

`gather` 的参数个数是**编译期固定**的。数量在运行时才知道时
（比如 N 个文件、N 个 URL），用 `gather_all`：

```cpp
// 同类型任务的 vector
std::vector<coro::Task<int>> tasks;
for (int i = 0; i < 5; ++i)
    tasks.push_back(download_page(i));       // 命名协程函数, 见第 1 讲

// 等全部完成, 按原顺序返回 vector<int>
std::vector<int> results = co_await coro::gather_all(std::move(tasks));
```

行为与 `gather` 一致：全部并发启动；异常 = 等全部完成后重抛第一个。

注意 `std::move(tasks)`——gather_all 会接管任务的所有权。

---

## 2.5 gather_void — void 任务的并发

```cpp
co_await coro::gather_void(
    flush_cache(),
    save_config(),
    send_heartbeat());   // 三个 Task<> 并发执行, 全部完成后才继续
```

- 异常语义同 gather：等全部完成后重抛第一个。
- 编译期数量；动态数量 + void 没有专门的 API，可以用
  `coro::wait_tasks(std::move(tasks), coro::WaitMode::AllCompleted)`（第 3 讲）。

---

## 2.6 并发 != 并行（重要澄清）

本讲的所有"并发"都发生在**一个线程**上：三个 `fetch` 协程交替执行，
等待期间把线程让给对方。这适合 **IO 密集**任务（网络/磁盘等待重叠）。

- 想让 **CPU 密集**计算吃满多核 → 第 8 讲的 `Scheduler` / loop-per-thread。
- 想把阻塞函数挪出事件循环线程 → 第 5 讲的 `to_thread`。

---

## 2.7 综合示例：并发爬虫雏形

```cpp
#include <coro/coro.hpp>
#include <iostream>
#include <string>
#include <vector>

using namespace std::chrono_literals;

// 模拟抓取一个页面
coro::Task<std::string> fetch_page(int page, int delay_ms)
{
    co_await coro::sleep(std::chrono::milliseconds(delay_ms));  // 网络 IO
    if (page == 3) throw std::runtime_error("page 3 被墙了");
    co_return "第 " + std::to_string(page) + " 页内容";
}

coro::Task<> main_task()
{
    // 1) 动态构造任务列表
    std::vector<coro::Task<std::string>> tasks;
    for (int i = 1; i <= 5; ++i)
        tasks.push_back(fetch_page(i, 100 * i));

    // 2) 并发抓取
    try {
        auto pages = co_await coro::gather_all(std::move(tasks));
        for (size_t i = 0; i < pages.size(); ++i)
            std::cout << pages[i] << std::endl;
    } catch (const std::exception& e) {
        std::cout << "抓取失败: " << e.what() << std::endl;
    }

    // 3) 固定数量的混合并发
    auto [a, b] = co_await coro::gather(
        fetch_page(10, 300),
        fetch_page(11, 100));
    std::cout << a << " / " << b << std::endl;
}

int main() { coro::run(main_task()); }
```

---

## 2.8 练习

1. 把第 1 讲练习 2 的两个协程改成 `gather` 并发，对比总耗时
   （应该从 ~1s 变成 ~最大单个耗时）。
2. 写 10 个任务并发 `sleep` 随机时长（50~500ms），用 `gather_all`
   收集，打印完成顺序对应的值，验证结果**顺序 = 参数顺序**
   （与完成顺序无关）。
3. （思考）`gather` 里一个任务失败，其余任务继续跑完才抛异常。
   如果你希望"一个失败立刻全部取消"，应该用什么？
   （提示：第 3 讲 `TaskGroup`。）
4. （实验）把 `coro::spawn(worker(1));` 的返回值故意丢掉，
   观察后台任务是否消失，理解保存返回值的必要性。

---

**下一讲**：[取消与超时](03-cancel-timeout.md) —— 让任务能被叫停，
以及 Python 3.11 风格的结构化并发。
