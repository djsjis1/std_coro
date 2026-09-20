# 第 9 讲 — 构建 HTTP 服务

> 本讲目标：使用项目自带的 Web 框架（`Web/` 目录）部署一个真实的
> HTTP 服务：路由、动态参数、静态文件、异步 handler、优雅停机、压测。
> 本框架基于 coro 网络层 + llhttp 协议解析，是学习"协程网络编程
> 如何组织成产品"的活教材。
>
> API 细节请配合 [Web 框架指南](../web-framework.md)。

---

## 9.1 最小可运行服务

`Web/main.cpp` 就是官方 demo。构建并运行：

```bash
cmake --build build --target web_server
./build/Debug/web_server.exe          # 默认监听 8080
```

浏览器访问 <http://127.0.0.1:8080/> 即可看到欢迎页。

## 9.2 框架速览

三层结构，每层都能独立使用：

```
web_server   监听/accept/线程池分发/keep-alive/优雅停机
   │
 router      方法 + 路径 → handler 协程 (radix 树, 支持 :param 与 *wildcard)
   │
 http_types  http_request (解析结果) / http_response (便捷工厂)
```

handler 的签名是**协程**——handler 内部可以 `co_await`
任何异步操作（文件、下游请求、sleep、子进程……）而不阻塞 worker：

```cpp
using handler_fn = std::function<coro::Task<http_response>(const http_request&)>;
```

## 9.3 写路由：静态、参数、通配

```cpp
#include "web_server.h"
#include "router.h"
#include "http_types.h"

web::web_server server;                  // 默认 worker 数 = 硬件并发
web::router& r = server.routes();

// ① 静态路由
r.get("/", [](const web::http_request&) -> coro::Task<web::http_response> {
    co_return web::http_response::text("hello");
});

// ② 动态参数: :id 捕获进 req.params
r.get("/user/:id", [](const web::http_request& req) -> coro::Task<web::http_response> {
    std::string id = req.param("id");
    co_return web::http_response::json("{\"id\": \"" + id + "\"}");
});

// ③ 通配: *rest 吞掉剩余整段路径
r.get("/files/*path", [](const web::http_request& req) -> coro::Task<web::http_response> {
    co_return web::http_response::text("你请求了 " + req.param("path"));
});

// ④ 异步 handler: 内部随便 co_await
r.get("/slow", [](const web::http_request&) -> coro::Task<web::http_response> {
    co_await coro::sleep(std::chrono::milliseconds(300));   // 模拟慢操作
    co_return web::http_response::text("终于好了 (300ms)");
});

// ⑤ POST + 读 body + 查询串
r.post("/echo", [](const web::http_request& req) -> coro::Task<web::http_response> {
    std::string who = req.query().count("who") ? req.query().at("who") : "world";
    co_return web::http_response::text("echo: " + req.body + " (from " + who + ")");
});

// ⑥ 静态文件目录: /static/* → ./www/*
r.static_dir("/static", "./www");

server.listen("0.0.0.0", 8080);
coro::run([&](web::web_server& s) -> coro::Task<> { co_await s.serve(); }(server));
```

匹配优先级：**静态段 > 参数 > 通配**（radix 树 DFS 回溯）；
`/user/` 与 `/user` 等价（尾斜杠归一）。

## 9.4 request / response 速查

**http_request**（llhttp 解析结果，只读）：

| 成员/方法 | 说明 |
|---|---|
| `method` / `url` / `version` | 基本行 |
| `body`（string） | 请求体（超过 `max_body` 会 400） |
| `headers` / `header(name)` | 大小写不敏感的头访问 |
| `path()` / `path_view()` | 去掉查询串的路径（view 版零分配） |
| `query()` | 查询串键值 |
| `param(name)` | 路由参数（`:id` / `*path` 捕获） |
| `keep_alive` | 是否 keep-alive |

**http_response**（便捷工厂 + 链式改头）：

```cpp
co_return web::http_response::text("plain text");
co_return web::http_response::json(R"({"ok": true})");
co_return web::http_response::html("<h1>hi</h1>");
co_return web::http_response::error(404, "not found");
co_return web::http_response::file("./www/logo.png");   // 按扩展名推 MIME

web::http_response resp = web::http_response::text("hi");
resp.header("X-Custom", "1").header("Cache-Control", "no-store");
co_return resp;
```

## 9.5 并发模型（它在替你做什么）

```
主线程     accept 循环 (serve())
   │  Scheduler::spawn_any (工厂模式: 协程帧在 worker 线程创建)
worker × N  每连接一个协程: llhttp 增量解析 → 路由 → handler → 序列化响应
            keep-alive: 一个连接多个请求复用协程; 支持 HTTP pipelining
```

- handler 抛异常 → 统一兜底 500（不会打崩 worker）；
- worker 内单线程语义：同一连接的状态无需加锁；
- 默认消息体上限 8MB（`server.set_max_body()` 可调）；
- 默认读空闲/单请求/响应写入超时均为 30s（`set_*_timeout()` 可调，`0ms` 关闭）；
- `set_verbose(true)` 打开访问日志。

## 9.6 优雅停机（把第 7 讲的信号用上）

```cpp
#include <coro/signal.hpp>

web::web_server server;
// ... 路由注册 ...
server.listen("0.0.0.0", 8080);

coro::run([&](web::web_server& s) -> coro::Task<> {
    auto srv = coro::spawn(s.serve());               // accept 循环

    auto stop = [&s]() -> coro::Task<> { s.stop(); co_return; };
    auto h1 = coro::signal::handle(SIGINT,  stop);   // Ctrl+C
    auto h2 = coro::signal::handle(SIGBREAK, stop);  // Ctrl+Break (Windows)

    co_await std::move(srv);                         // s.stop() 后 serve 正常返回
    s.wait_all();                                    // 等所有在途连接协程收尾
}(server));
std::cout << "已优雅退出" << std::endl;
```

`s.stop()` 是线程安全的：关闭监听 socket → 挂起的 `accept` 被唤醒 →
serve 循环退出；`wait_all()` 等全部在途连接处理完。
Ctrl+C 之后进程干净退出，正在处理的请求不会被拦腰截断。

## 9.7 自测与压测（框架自带）

```bash
# 内置自测: 路由/keep-alive/404/405/路径穿越防护, 全过后自动退出
./build/Debug/web_server.exe --selftest

# 高并发压测: 默认 200 客户端 × 50 轮
./build/Debug/web_server.exe --stress
```

安全细节（框架已处理，了解即可）：静态目录有**路径穿越防护**
（拒绝 `..`、绝对路径、盘符、`%2e` 编码），穿越尝试返回 403；
405 响应自动携带 `Allow` 头（RFC 7231）。

## 9.8 单独使用 radix_router（不要 HTTP 框架也要路由树）

`router/radix_router.h` 是零依赖的泛型路由树，可以装任何值类型：

```cpp
#include "radix_router.h"

radix_router<int> tree;
tree.insert("/user/:id", 1);
tree.insert("/files/*path", 2);
tree.insert("/user/list", 3);            // 静态段优先于 :id

radix_router<int>::params_view params;
const int* v = tree.lookup("/user/42", &params);
// v 指向 1; params[0] = string_view("42") — 指向 path 内部, 零分配
```

规则：pattern 必须以 `/` 开头；`:param`、`*wild` 必须占据完整一段
且通配必须在末尾；重复注册替换旧值。线程约定：insert 全部完成后
再并发 lookup（读并发安全）。

## 9.9 练习

1. 给 demo 服务加一个 `GET /stats`：返回累计请求数、当前活跃连接数
   （用 atomic 计数，中间件式地在 handler 外层包一层）。
2. 写一个 `POST /hash`：读 body，`to_thread` 里算 SHA（可用任意
   简单哈希模拟），返回 hex。用 `--stress` 压，观察 worker 占用。
3. 实现"限流中间件"：全局 `coro::Semaphore(100)` 包住 handler，
   超出排队；再做一个超过 50ms 排队直接 503 的版本。
4. （综合）把第 7 讲的目录监视接进来：`/static` 下文件变化时打印
   日志并清空内存缓存（如果你给 `http_response::file` 加了缓存）。

---

**下一讲**：[综合实战](10-final-project.md) —— 把 1~9 讲全部串起来，
做一个完整的并发文件处理服务。
