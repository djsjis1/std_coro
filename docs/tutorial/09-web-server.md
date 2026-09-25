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
cmake -S Web -B build-web
cmake --build build-web --config Release --target web_server
./build-web/web_server                # Linux, 默认监听 8080
# Windows: .\build-web\Release\web_server.exe
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

web_server server;                  // 默认 worker 数 = 硬件并发
router& r = server.routes();

// ① 静态路由
r.get("/", [](const http_request&) -> coro::Task<http_response> {
    co_return http_response::text("hello");
});

// ② 动态参数: :id 捕获进 req.params
r.get("/user/:id", [](const http_request& req) -> coro::Task<http_response> {
    std::string id = req.param("id");
    co_return http_response::text("id=" + id); // JSON 对象应交给序列化库转义
});

// ③ 通配: *rest 吞掉剩余整段路径
r.get("/files/*path", [](const http_request& req) -> coro::Task<http_response> {
    co_return http_response::text("你请求了 " + req.param("path"));
});

// ④ 异步 handler: 内部随便 co_await
r.get("/slow", [](const http_request&) -> coro::Task<http_response> {
    co_await coro::sleep(std::chrono::milliseconds(300));   // 模拟慢操作
    co_return http_response::text("终于好了 (300ms)");
});

// ⑤ POST + 读 body + 查询串
r.post("/echo", [](const http_request& req) -> coro::Task<http_response> {
    std::string query = req.query(); // 原始查询串, 不是 map
    co_return http_response::text("echo: " + req.body + " (query: " + query + ")");
});

// ⑥ 静态文件目录: /static/* → ./www/*
r.static_dir("/static", "./www");

server.listen("0.0.0.0", 8080);
coro::run(server.serve());
```

匹配优先级：**静态段 > 参数 > 通配**（radix 树 DFS 回溯）；
`/user/` 与 `/user` 等价（尾斜杠归一）。

### 按模块组织与中间件

每个模块通过工厂返回一个 `router`，主程序显式 `include(sub, "/api/users")`。
模块只保存路由，不单独启动 server 或 worker。示例见 [Web 指南](../web-framework.md)。

`r.use(mw)` 是全局链，`r.get(path, handler, {mw})` 是路由级链；中间件接收
`http_request&` 与 move-only `router::next_fn`，通过 `co_await next()` 进入内层。
重复调用 next 同步抛错，不允许把 next 移到请求生命周期之外。

include 会快照展平子路由和中间件：父全局 → 子全局 → 路由级 → handler。
它不是独立子应用挂载，子全局链不保护该前缀下未命中的 404/405，也不转移静态目录。
所有注册必须在 `serve()` 前完成；`serve()` 注册 `/__stats` 后冻结路由表。
非法路由/前缀、自包含及参数名冲突抛 `invalid_argument`，冻结后注册抛 `logic_error`。

## 9.4 request / response 速查

**http_request**（llhttp 解析结果，只读）：

| 成员/方法 | 说明 |
|---|---|
| `method` / `url` / `version` | 基本行 |
| `body`（string） | 请求体（超过 `max_body` 会 400） |
| `headers` / `header(name)` | 大小写不敏感的头访问 |
| `path()` / `path_view()` | 去掉查询串的路径（view 版零分配） |
| `query()` | 原始未解码的查询串 string |
| `param(name)` | 路由参数（`:id` / `*path` 捕获） |
| `keep_alive` | 是否 keep-alive |

**http_response**（便捷工厂 + 响应头修改）：

```cpp
co_return http_response::text("plain text");
co_return http_response::json(R"({"ok": true})");
co_return http_response::html("<h1>hi</h1>");
co_return http_response::error(404, "not found");
co_return http_response::file("./www/logo.png");   // 按扩展名推 MIME

http_response resp = http_response::text("hi");
resp.header("X-Custom", "1");
resp.header("Cache-Control", "no-store");
co_return resp;
```

## 9.5 并发模型（它在替你做什么）

```
主线程     accept 循环 (serve())
   │  Scheduler::spawn_any (工厂模式: 协程帧在 worker 线程创建)
worker × N  每连接一个协程: llhttp 增量解析 → 路由 → handler → 序列化响应
            keep-alive: 一个连接多个请求复用协程; 支持 HTTP pipelining
```

- 普通 handler 异常 → 兜底 500；`CancelledError` 原样传播；
- worker 内单线程语义：同一连接的状态无需加锁；
- 默认消息体上限 8MB（`server.set_max_body()` 可调）；
- 默认读空闲/单请求/响应写入超时均为 30s（`set_*_timeout()` 可调，`0ms` 关闭）；
- `set_verbose(true)` 打开访问日志。

## 9.6 优雅停机（把第 7 讲的信号用上）

```cpp
#include <coro/signal.hpp>

coro::Task<> stop_server(web_server& s) {
    s.stop();
    co_return;
}

web_server server;
// ... 路由注册 ...
server.listen("0.0.0.0", 8080);

coro::run([](web_server& s) -> coro::Task<> {
    auto srv = coro::spawn(s.serve());               // accept 循环

    auto stop = [&s] { return stop_server(s); };     // 普通工厂, 无临时协程闭包
    auto h1 = coro::signal::handle(SIGINT, stop);    // Ctrl+C
#ifdef _WIN32
    auto h2 = coro::signal::handle(SIGBREAK, stop);  // Ctrl+Break (Windows)
#endif

    co_await std::move(srv);                         // s.stop() 后 serve 正常返回
    s.wait_all();                                    // 等所有在途连接协程收尾
}(server));
std::cout << "已优雅退出" << std::endl;
```

`s.stop()` 是线程安全的：关闭监听 socket → 挂起的 `accept` 被唤醒 →
serve 循环退出；`wait_all()` 等全部在途连接处理完。
这是主动关闭连接后等待资源回收，不承诺正在处理的每个请求都能完整响应。

## 9.7 自测与压测（框架自带）

```bash
# 内置自测: 路由/keep-alive/404/405/路径穿越防护, 全过后自动退出
./build-web/web_server --selftest

# 高并发压测: 默认 200 客户端 × 50 轮
./build-web/web_server --stress
```

压测完整成功退出 0，连接/协议/超时/缺响应退出 1，非法参数退出 2。
连接每次尝试和每轮请求均有 2 秒时限；吞吐只统计实际成功响应。
Windows 将上述路径替换为 `build-web/Release/web_server.exe`。

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

1. 访问内建 `GET /__stats`，核对 requests/errors 与连接数 in_flight/peak；
   再用父全局中间件限制它的访问。统计请求在写出后计数，不包含在自身快照里。
2. 写一个 `POST /hash`：读 body，`to_thread` 里算 SHA（可用任意
   简单哈希模拟），返回 hex。用 `--stress` 压，观察 worker 占用。
3. 实现"限流中间件"：全局 `coro::Semaphore(100)` 包住 handler，
   超出排队；再做一个超过 50ms 排队直接 503 的版本。
4. （综合）把第 7 讲的目录监视接进来：`/static` 下文件变化时打印
   日志并清空内存缓存（如果你给 `http_response::file` 加了缓存）。

---

**下一讲**：[综合实战](10-final-project.md) —— 把 1~9 讲全部串起来，
做一个完整的并发文件处理服务。
