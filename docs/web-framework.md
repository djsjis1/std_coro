# Web 框架指南（Web/ + router/）

> 项目自带的多线程 HTTP 服务器框架（`Web/` 目录）与独立的泛型
> 路由树（`router/radix_router.h`）。基于 coro 网络层 + llhttp
> 协议解析，是"协程网络编程如何组织成产品"的参考实现。
>
> 上手教程见[教程第 9 讲](tutorial/09-web-server.md)；
> 本文是完整的架构与 API 说明。

---

## 1. 总览

```
┌─────────────────────────── web_server ───────────────────────────┐
│  listen() / serve() (accept 循环, 主线程)                         │
│      │ Scheduler::spawn_any(工厂) — 连接分发到 worker             │
│  ┌─────────────── worker × N (各自 EventLoop) ───────────────┐   │
│  │  每连接一个协程:                                           │   │
│  │   llhttp 增量解析 → http_request                           │   │
│  │   router.route(req) → handler 协程                         │   │
│  │   http_response::build() → socket 写回                     │   │
│  │   keep-alive / HTTP pipelining / 500 兜底                  │   │
│  └────────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────┘
           │ 依赖                        │ 依赖
     router (每方法一棵树)        http_types (请求/响应)
           │ 依赖
     radix_router<T> (独立, 零 HTTP 依赖)
```

| 组件 | 位置 | 行数 | 可独立使用 |
|---|---|---|---|
| `web_server` | `Web/src/web_server.h/.cpp` | 83+228 | 否（依赖 router/http_types） |
| `router` | `Web/src/router.h` | 234 | 可（header-only） |
| `http_request/response` | `Web/src/http_types.h/.cpp` | 75+238 | 可 |
| `radix_router<T>` | `router/radix_router.h` | 473 | **完全独立**（泛型、无 HTTP 概念） |

构建：根 CMake 已接好（`add_subdirectory(Web)`，链接 `coro::coro +
http::http + router::router`）。llhttp 以独立子目录
（`http.llhttp-release-v9.3.0`）引入，仅 Web 框架依赖它——
核心库与 router 不依赖。

---

## 2. web_server API

```cpp
class web_server {
public:
    explicit web_server(size_t workers = 0);   // 0 = 硬件并发数

    bool listen(const char* ip, unsigned short port);   // 同步, 启动前调用
    router& routes();                          // 路由表 (注册路由用)
    void set_max_body(size_t);                 // 消息体上限, 默认 8MB, 超限 400
    void set_verbose(bool);                    // 访问日志开关
    void set_idle_timeout(milliseconds);       // 读空闲超时, 默认 30s
    void set_request_timeout(milliseconds);    // 单请求总时限, 默认 30s
    void set_write_timeout(milliseconds);      // 单响应写时限, 默认 30s

    coro::Task<> serve();                      // accept 循环 (co_await 驱动)
    void stop();                               // 线程安全: 关监听和活动连接, 唤醒 I/O
    void wait_all();                           // 等全部在途连接协程收尾
    size_t worker_count() const;
};
```

三个超时都可设为 `0ms` 关闭。读空闲超时限制相邻两次数据到达的间隔；
单请求总时限从首字节开始计时，因此客户端持续慢速发字节也不会无限占用连接。

标准生命周期（含优雅停机）：

```cpp
coro::Task<> shutdown_task(web_server& server) {
    server.stop();
    co_return;
}

web_server server;
server.listen("0.0.0.0", 8080);
// ... 注册路由 ...

coro::run([&](web_server& s) -> coro::Task<> {
    auto srv = coro::spawn(s.serve());
    auto h = coro::signal::handle(SIGINT, [&s] { return shutdown_task(s); });
    co_await std::move(srv);    // stop() 后 serve 正常返回
    s.wait_all();               // 等在途请求处理完
}(server));
```

### 并发模型细节

- **主线程 accept，worker 处理**：`serve()` 在当前线程跑 accept 循环；
  每个新连接经 `Scheduler::spawn_any` 以**工厂**分发到最闲 worker
  （协程帧在 worker 线程创建/销毁——库的铁律，见
  [架构剖析第 9 节](architecture.md#9-多线程模型线程亲缘与跨线程路由)）；
- **Windows reattach**：accept 用 `accept_noattach()`，worker 协程
  入口先 `TcpStream::reattach()` 把 socket 关联到本 worker 的 IOCP
  （Linux io_uring 无此步骤）；
- **两级负载均衡**：Scheduler 选 worker（活跃连接数）→
  keep-alive 让同一连接的后续请求留在原 worker（亲缘）；
- **异常兜底**：handler 抛任何异常 → 该请求统一 500，连接协程存活，
  不打崩 worker；
- **流水线**：llhttp 增量解析支持一个连接上多个请求排队
  （HTTP pipelining），逐个分发。

---

## 3. router API

```cpp
class router {
public:
    using handler_fn = std::function<coro::Task<http_response>(const http_request&)>;

    void get(const std::string& path, handler_fn h);    // 快捷方法
    void post(const std::string& path, handler_fn h);
    void add(const char* method, const std::string& path, handler_fn h);

    void static_dir(const std::string& mount, const std::string& dir);

    coro::Task<http_response> route(http_request& req); // web_server 内部调用
};
```

### 路径语法

| 模式 | 匹配 | 捕获 |
|---|---|---|
| `/user/list` | 精确静态 | — |
| `/user/:id` | 单段参数 | `req.param("id")` |
| `/files/*path` | 剩余全部（含多段） | `req.param("path")` |

优先级：**静态 > 参数 > 通配**（radix 树 DFS 回溯）；
`/user/` ≡ `/user`（尾斜杠归一）。

### 分发流程（`route()` 四级）

1. radix 树匹配 → 命中则执行 handler（`req.params` 已填充）；
2. 路径存在但方法不对 → `405` + `Allow` 头（RFC 7231）；
   OPTIONS 请求自动应答 Allow；
3. 静态目录前缀匹配（仅 GET）→ 异步读文件（`coro::fs::read_all`）
   + MIME 推断；
4. 全部落空 → `404`。

### 静态目录安全（已内置）

`static_dir` 的路径穿越防护：反斜杠归一、拒绝绝对路径/盘符、
拒绝 `%2e` 编码、逐段拒绝 `..`，穿越尝试返回 **403**。
MIME 按扩展名推断（`mime_type(path)`）。

---

## 4. http_request / http_response

### http_request（llhttp 解析结果，handler 内只读）

| 成员/方法 | 说明 |
|---|---|
| `method` / `url` / `version` | 请求行 |
| `body`（`std::string`） | 请求体（超过 max_body 已被 400） |
| `headers` / `header(name)` | 大小写不敏感访问 |
| `path()` | 去掉查询串的路径 |
| `path_view()` | 同上，`string_view` 零分配（热路径） |
| `query()` | 查询串键值 |
| `param(name)` | 路由捕获（`:id` / `*path`） |
| `keep_alive` | 是否 keep-alive |

### http_response（便捷工厂 + 链式改头）

```cpp
co_return web::http_response::text("plain");
co_return web::http_response::json(R"({"ok":true})");
co_return web::http_response::html("<h1>hi</h1>");
co_return web::http_response::error(404, "not found");
co_return web::http_response::file("./www/a.png");   // 按扩展名推 MIME

web::http_response r = web::http_response::text("hi");
r.header("X-Request-Id", "42").header("Cache-Control", "no-store");
co_return r;    // build() 序列化时自动补 Content-Length
```

---

## 5. radix_router<T>（独立路由树）

不写 HTTP 也可以用——它是一个泛型的"路径模式 → 值"映射：

```cpp
#include "radix_router.h"

radix_router<int> tree;
tree.insert("/api/user/:id", 1);
tree.insert("/api/user/list", 2);        // 静态优先于 :id
tree.insert("/static/*rest", 3);

radix_router<int>::params_view params;   // 内联 8 槽 small buffer, 零堆分配
if (const int* v = tree.lookup("/api/user/42", &params)) {
    // *v == 1; params[0] == string_view("42")  (指向 path 内部)
}
```

### API

| API | 语义 |
|---|---|
| `bool insert(string_view pattern, Value v)` | 注册；重复注册**替换**旧值；非法模式返回 false 且**无副作用** |
| `const Value* lookup(string_view path, params_view* out = nullptr)` | 命中返回 value 指针；未命中 nullptr |
| `params_view` | 参数捕获容器：`size/begin/end/[]/emplace_back/pop_back/clear` |
| `size() / empty() / clear()` | clear 支持热重载（arena 池整体重置） |

### 非法模式（insert 返回 false）

不以 `/` 开头、空段 `//`、通配不在末尾、空参数名（`/:`）、
段中间出现 `:` 或 `*`、与已注册路由同位置参数名冲突
（`/:a/x` 与 `/:b/y` 冲突）。

### 实现要点

- **按段字典树**：节点含有序静态子表（兄弟 ≤8 线性扫，>8 二分）、
  至多一个 `:param`、至多一个 `*wild`；
- **节点 arena**（`std::deque<node>`）：指针稳定、缓存友好、
  clear 不递归析构；
- **DFS + 回溯**：参数分支深层失败时撤销捕获回溯到通配分支
  （如 `/files/:lang/readme` 失败后改试 `/files/*any`）；
- **线程约定**：insert 必须先于并发 lookup 完成（写后读）；
  lookup 只读、可任意并发。

---

## 6. 运维手册

### 自测

```bash
./build/Debug/web_server.exe --selftest [port]
```

内置验证：路由命中（静态/参数/通配）、keep-alive 复用、
404、405+Allow、路径穿越 403——全部通过后自动退出（可接 CI）。
默认端口是 8080；如果该端口被其他服务占用，可传入空闲端口，例如
`web_server.exe --selftest 18081`。Windows 使用独占绑定，不会覆盖已有监听者。

### 压测

```bash
./build/Debug/web_server.exe --stress          # 默认 200 客户端 × 50 轮
./build/Debug/web_server.exe --stress 500 100  # 自定义客户端数与轮数
```

### 常驻与关停

正常 Ctrl+C（SIGINT）/ Ctrl+Break（SIGBREAK）走 signal →
`stop()` → `wait_all()` 优雅退出（见第 2 节骨架）。
`tests/web_shutdown_check.cpp` 是"serve + signal + stop + wait_all"
接线的最小验证程序。

### 已知限制

- HTTP/1.1 only：无 TLS、无 HTTP/2、无 chunked 响应生成
  （请求侧 llhttp 支持 chunked 解析）；
- 无中间件机制——横切逻辑（日志/限流/鉴权）用"包装 handler"的
  高阶函数模式自行实现（教程第 9 讲练习 3 有示例）；
- 响应体整体缓存在 `http_response::body`——超大文件下载
  （流式传输）需自行基于 `coro::net` 直写；
- `http_response::file()` 当前为同步读（适合静态小文件），
  大文件建议改用 `co_await coro::fs::read_all` 分块或自行流式。
