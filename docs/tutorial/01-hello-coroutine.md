# 第 1 讲 — 第一个协程程序

> 本讲目标：搭好环境，跑通第一个协程程序，理解使用层面必须知道的
> 四件事：**`Task<T>` 是什么、惰性启动、`coro::run` 做了什么、
> `sleep` 为什么不阻塞线程**。

---

## 1.1 环境搭建

### 方式 A：使用本项目仓库（推荐初学者）

```bash
git clone <本项目地址>   # 或直接下载解压
cd coro
cmake -B build           # Windows: cmake -B build -A x64
cmake --build build --config Debug
```

项目自带一个**协程练习场** `main.cpp`，改完直接编译运行：

```bash
cmake --build build --config Debug --target coro_practice
./build/Debug/coro_practice.exe     # Linux: ./build/coro_practice
```

单元测试（验证环境是否正常）：

```bash
ctest --test-dir build -C Debug --output-on-failure
```

### 方式 B：把库引入你自己的项目

`coro/` 子目录是**独立可复制的库包**（header-only、零第三方依赖）：

```bash
cp -r /path/to/coro your_project/thirdparty/coro
```

```cmake
# 你的 CMakeLists.txt
cmake_minimum_required(VERSION 3.20)
project(my_app LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_subdirectory(thirdparty/coro)
add_executable(my_app main.cpp)
target_link_libraries(my_app PRIVATE coro::coro)
```

`coro::coro` 目标自动携带：include 路径、C++20 标准、平台链接库
（Windows: `ws2_32`；Linux: `pthread`，网络功能还需 `liburing`）。

### 环境要求

| 项目 | 要求 |
|---|---|
| 编译器 | MSVC 2022 / GCC 11+（11~13 需加 `-fcoroutines`）/ Clang 14+ |
| C++ 标准 | C++20（协程关键字 `co_await`/`co_return` 需要） |
| CMake | 3.20+ |

---

## 1.2 三个关键字的 3 分钟速成

一个函数体里出现 `co_return`、`co_await`、`co_yield` 任意一个，
它就是**协程函数**。使用层面你只需要知道：

- **`co_await x`** — "等 x 好了再继续往下走"。等待期间**不占用线程**，
  线程去跑别的协程。`x` 可以是另一个 `Task`、`coro::sleep(1s)`、
  一个 `Future`、一把锁……本库所有可等待的东西都叫 *awaitable*。
- **`co_return v`** — 协程的 return，把值交给等待者。
- **协程的返回类型是 `coro::Task<T>`**，`T` 是 `co_return` 的值类型。
  不返回值的协程写 `coro::Task<>`。

对照 Python，一一对应：

```python
async def compute() -> int:      # coro::Task<int> compute()
    await asyncio.sleep(1)       # co_await coro::sleep(1s);
    return 42                    # co_return 42;
```

> 深入：`Task` 内部靠编译器生成的"协程帧"在堆上保存挂起点的局部变量，
> 恢复时接着上次的位置继续执行。这就是"挂起后线程能去干别的、
> 恢复后还能接着写"的原理。完整机制见
> [C++20 协程课程](../cpp20-coroutines-course/README.md)。

---

## 1.3 第一个程序

```cpp
// hello.cpp
#include <coro/coro.hpp>
#include <iostream>

using namespace std::chrono_literals;

// 一个"耗时的"异步计算: 挂起 1 秒后返回 42
coro::Task<int> compute()
{
    std::cout << "开始计算..." << std::endl;
    co_await coro::sleep(1s);            // 挂起 1 秒, 不阻塞线程
    std::cout << "计算完成!" << std::endl;
    co_return 42;
}

// 主协程: 串行等待 compute
coro::Task<> main_task()
{
    int result = co_await compute();     // 等它完成, 拿到 42
    std::cout << "结果: " << result << std::endl;
}

int main()
{
    coro::run(main_task());              // 启动事件循环, 跑完自动退出
    // coro::run 返回主协程的 co_return 值:
    // 这里 main_task 是 Task<>, 所以返回 void
}
```

编译运行（以方式 B 的 CMake 工程为例）：

```bash
cmake --build build
./build/my_app          # 或 build/Debug/my_app.exe
```

输出（中间真的等了 1 秒）：

```
开始计算...
计算完成!
结果: 42
```

### 逐行拆解

| 代码 | 发生了什么 |
|---|---|
| `coro::Task<int> compute()` | 调用 `compute()` **不会执行函数体**！只创建了一个待启动的任务对象（见 1.4） |
| `co_await coro::sleep(1s)` | 向事件循环注册一个 1 秒的定时器，协程挂起；1 秒后事件循环把它叫醒 |
| `co_await compute()` | 启动 `compute` 协程并等待它完成；异常会在这里重新抛出 |
| `coro::run(main_task())` | 启动**事件循环**，运行 `main_task` 直到它完成，然后退出循环 |

---

## 1.4 必须理解的四个概念

### ① Task 是惰性的（lazy start）

```cpp
coro::Task<int> t = compute();   // 此刻函数体一行都没执行!
// ...
co_await compute();              // co_await 会启动它
t.start();                       // 或者显式 start()
auto t2 = coro::spawn(compute());// 或者 spawn 后台启动 (第 2 讲)
```

这和 Python 的 coroutine 对象一样：`compute()` 只是"创建任务"，
"启动"需要 `await`/`start`/`spawn` 之一。

好处：创建和启动分离，库才能统一决定启动时机（比如 `gather`
把一串任务都准备好再一起启动）。

### ② coro::run — 事件循环的入口

`coro::run(task)` 做了三件事：

1. `task.start()` — 启动你的主协程；
2. `EventLoop::get().run()` — 驱动**事件循环**直到没有任何工作可做；
3. 返回主协程的 `co_return` 值（`Task<T>` 返回 `T`）；
   主协程抛的异常在 `run` 调用处重新抛出——所以 `main()`
   里可以用普通 try/catch 兜底：

```cpp
int main()
{
    try {
        coro::run(main_task());
    } catch (const std::exception& e) {
        std::cerr << "fatal: " << e.what() << std::endl;
        return 1;
    }
}
```

**事件循环**（EventLoop）是整个库的心脏：一个"就绪协程队列 +
定时器堆 + IO 完成事件"的调度器，类似 Python 的事件循环
（`asyncio.run` 里的那个 loop）。你不需要手动操作它，
只要知道：**所有协程默认跑在调用 `run()` 的那个线程上**，
`co_await` 挂起时线程回到循环里去跑别的就绪协程。

### ③ sleep 挂起的是协程，不是线程

```cpp
co_await coro::sleep(1s);   // 协程睡 1 秒; 线程立刻去跑其他就绪协程
```

如果 10 个协程都 `sleep(1s)`，总耗时仍是 ~1 秒（它们重叠睡眠），
而 10 个线程各自 `Sleep(1000)` 也是 1 秒但占用了 10 个线程。
协程的成本接近"一个函数调用 + 一小块堆内存"，所以可以轻松开
**几十万**个（见 [性能指南](../performance.md)）。

对应的还有一个让步原语：

```cpp
co_await coro::yield();     // 等价 asyncio.sleep(0): 主动让出, 排到队尾
```

### ④ 单线程协作式纪律

同一时刻只有一个协程在跑（单线程事件循环）。这带来巨大简化：
**协程之间共享数据不需要加锁**（第 4 讲的锁用于别的场景），
但也有一条铁律：

> 协程体内不要写**不含 `co_await` 的死循环**——协作式调度下
> 没人能抢走 CPU，整个事件循环（包括所有其他协程）都会被卡死。
> 长计算要周期性 `co_await coro::yield()`，或丢给
> `co_await coro::to_thread(...)`（第 5 讲）。

这和 Python asyncio 完全一样（Go 的调度器可以在任意点抢占，所以没这条限制）。

---

## 1.5 命名函数 vs lambda 协程体

全教程的协程一律写成命名函数：

```cpp
// ✅ 教程约定: 命名协程函数, 参数进协程帧, 生命周期由标准保证
coro::Task<int> worker(int id)
{
    co_await coro::sleep(100ms);
    co_return id * 10;
}

// 使用
std::vector<coro::Task<int>> tasks;
for (int i = 0; i < 3; ++i)
    tasks.push_back(worker(i));
```

**为什么不写 lambda？**

```cpp
// ❌ MSVC Debug 下有坑: 捕获的 i 可能没被正确复制进协程帧,
//    挂起恢复后读到错误值 (数据错乱甚至崩溃)
for (int i = 0; i < 3; ++i)
    tasks.push_back([i]() -> coro::Task<int> { co_return i * 10; }());
```

MSVC **Debug** 模式的已知问题：lambda 协程的捕获变量可能不被复制进
协程帧。Release 无此问题，但代码要在两种配置下都对，所以本库与
所有示例/教程统一用"命名函数 + 参数传递"。需要携带状态时，
把状态作为参数传进去（需要共享所有权就用 `shared_ptr` 参数）。

---

## 1.6 完整的惯用程序骨架

以后每讲都长这样，先记住骨架：

```cpp
#include <coro/coro.hpp>
#include <iostream>
using namespace std::chrono_literals;

// ---- 你的业务协程 (命名函数) ----
coro::Task<> do_work(int id)
{
    std::cout << "worker " << id << " 开始" << std::endl;
    co_await coro::sleep(100ms);
    std::cout << "worker " << id << " 完成" << std::endl;
}

// ---- 主协程: 组织所有工作 ----
coro::Task<> main_task()
{
    co_await do_work(1);
    co_await do_work(2);
}

// ---- 入口 ----
int main()
{
    try {
        coro::run(main_task());
    } catch (const std::exception& e) {
        std::cerr << "fatal: " << e.what() << std::endl;
        return 1;
    }
}
```

---

## 1.7 常见第一次的错误

| 症状 | 原因 | 修正 |
|---|---|---|
| 调用 `compute()` 后"什么都没发生" | Task 惰性，没启动 | `co_await compute()` 或 `t.start()` |
| `co_await t` 之后 t 再也不可用 | Task 只能被等待一次 | 需要多次消费结果 → 用 `spawn` + `Future`，或重新创建任务 |
| 程序退出但协程没跑完 | `coro::run` 的主协程完成了，但 spawn 的后台任务还没完成（第 2 讲详述） | 在主协程里 `co_await` 所有 spawn 的任务 |
| 协程里调用阻塞函数（`std::this_thread::sleep_for`、同步文件/网络 IO）整个程序卡住 | 阻塞了唯一的事件循环线程 | 用 `co_await coro::sleep()` 代替；阻塞调用包进 `to_thread` |
| MSVC 报错 `co_await` 不能用 | 没开 C++20 | `set(CMAKE_CXX_STANDARD 20)`；GCC 11-13 加 `-fcoroutines` |

更多错误见 [FAQ](../faq.md)。

---

## 1.8 练习

1. 把 `compute` 的返回值改成 `std::string`，返回 `"hello coro"`，
   在 `main_task` 里打印它。
2. 写两个协程 A、B，各自打印 5 个数字（中间 `sleep(100ms)`），
   先串行 `co_await` 它们，观察总耗时；下一讲学并发后再回来对比。
3. （实验）故意在协程里写 `for(;;){}` 死循环，观察程序卡死的症状，
   然后修复为每 100 次迭代 `co_await coro::yield()`。
4. （实验）在 `co_await coro::sleep(1s)` 前后打印
   `std::this_thread::get_id()`，确认所有协程跑在同一个线程上。

---

**下一讲**：[并发编程 — spawn / gather](02-concurrency.md)，
让三个请求同时出发而不是排队。
