// ============================================================================
// main.cpp — coro + llhttp 多线程 Web 服务器示例
// ============================================================================
//
// 运行:
//   web_server              常驻服务器, 监听 127.0.0.1:8080 (多 worker)
//   web_server --selftest   启动后由内置客户端验证路由/keep-alive/404,
//                           验证完毕自动优雅退出
//   web_server --stress [clients] [rounds]
//                           高并发压测: clients 个并发客户端 × rounds 轮
//                           keep-alive 请求, 输出吞吐统计后自动退出
//                           (默认 200 × 50 = 10000 请求)
//
// 演示点:
//   - 多线程架构: accept 循环 + Scheduler 多 worker 并行处理连接
//   - 路由注册(GET/POST + 异步 handler: handler 里可以 co_await)
//   - 静态文件服务(/static → www 目录)
//   - keep-alive 连接复用 + pipelining(一条连接连发多个请求)
//   - handler 异常 → 500 兜底
// ============================================================================

#include <coro/coro.hpp>
#include <coro/net.hpp>
#include <coro/signal.hpp>

#include <http_parse.h>
#include <http_protocol.h>

#include <atomic>
#include <cstdlib>
#include <ctime>
#include <iostream>
#include <string>
#include <vector>

#include "http_types.h"
#include "web_server.h"

using namespace std::chrono_literals;

// ---- 路由示例 ----

// GET / — 返回一个 HTML 页面
coro::Task<http_response> handle_home(const http_request &req)
{
    std::string html =
        "<!DOCTYPE html><html><head><title>coro web server</title></head><body>"
        "<h1>Hello from coro web server</h1>"
        "<p>method: " +
        req.method + "</p>"
                     "<p>path: " +
        req.path() + "</p>"
                     "<p>try: <a href=\"/greet?name=coro\">/greet?name=coro</a>, "
                     "<a href=\"/time\">/time</a>, "
                     "<a href=\"/static/hello.txt\">/static/hello.txt</a></p>"
                     "</body></html>";
    co_return http_response::html(std::move(html));
}

// GET /greet?name=xxx — query 参数示例
coro::Task<http_response> handle_greet(const http_request &req)
{
    std::string name = "world";
    auto q = req.query();
    if (q.rfind("name=", 0) == 0)
        name = q.substr(5); // 简化解析, 只取首个 name= 参数
    co_return http_response::text("hello, " + name + "!\n");
}

// GET /time — 异步 handler: 演示 sleep 挂起期间其他连接照常处理
coro::Task<http_response> handle_time(const http_request &req)
{
    co_await coro::sleep(100ms); // 挂起, 不占 CPU

    std::time_t now = std::time(nullptr);
    char buf[64];
    std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", std::localtime(&now));
    co_return http_response::json("{\"time\": \"" + std::string(buf) + "\"}\n");
}

// POST /echo — 消息体回显
coro::Task<http_response> handle_echo(const http_request &req)
{
    co_return http_response::json("{\"echo\": \"" + req.body + "\"}\n");
}

// GET /boom — 故意抛异常, 验证 500 兜底
coro::Task<http_response> handle_boom(const http_request &)
{
    throw std::runtime_error("intentional crash");
}

// 示例：GET /hello — 返回简单文本
coro::Task<http_response> handle_hello(const http_request &req)
{
    co_return http_response::text("Hello, World!\n");
}

// 示例：POST /api/data — 处理 JSON 数据
coro::Task<http_response> handle_api_data(const http_request &req)
{
    // req.body 包含请求体
    // req.headers 包含头部信息
    co_return http_response::json("{\"status\": \"ok\"}\n");
}

// 示例：带异步操作的 handler（可以 co_await）
coro::Task<http_response> handle_slow(const http_request &req)
{
    co_await coro::sleep(1s);  // 模拟耗时操作，不阻塞其他连接
    co_return http_response::text("Done after 1 second\n");
}

// ---- 动态路由示例 (radix_router: 参数/通配捕获) ----

// GET /user/admin — 静态路由: 优先级高于 /user/:id (静态优先规则)
coro::Task<http_response> handle_admin(const http_request &)
{
    co_return http_response::text("admin console\n");
}

// GET /user/:id — 参数路由: req.param("id") 取捕获的路径段
coro::Task<http_response> handle_user(const http_request &req)
{
    co_return http_response::json("{\"user_id\": \"" + req.param("id") + "\"}\n");
}

// GET /user/:id/posts/:post_id — 多参数路由
coro::Task<http_response> handle_user_post(const http_request &req)
{
    co_return http_response::json("{\"user\": \"" + req.param("id") +
                                  "\", \"post\": \"" + req.param("post_id") + "\"}\n");
}

// GET /download/*filepath — 通配路由: 吞掉剩余全部路径
coro::Task<http_response> handle_download(const http_request &req)
{
    co_return http_response::text("downloading: " + req.param("filepath") + "\n");
}

void register_routes(web_server &server)
{
    auto &r = server.routes();
    r.get("/", handle_home);
    r.get("/greet", handle_greet);
    r.get("/time", handle_time);
    r.post("/echo", handle_echo);
    r.get("/boom", handle_boom);
    r.get("/hello", handle_hello);
    r.post("/api/data", handle_api_data);
    r.get("/slow", handle_slow);
    // 动态路由: 匹配优先级 静态 > 参数 > 通配
    r.get("/user/admin", handle_admin);
    r.get("/user/:id", handle_user);
    r.get("/user/:id/posts/:post_id", handle_user_post);
    r.get("/download/*filepath", handle_download);
    // 静态目录(编译期注入绝对路径, 任何工作目录下都可用)
    r.static_dir("/static", WEB_WWW_DIR);
}



// ---- 内置 selftest 客户端 ----

// 发送一个完整请求并解析响应, 返回 (状态码, 响应体)
coro::Task<std::pair<int, std::string>> one_request(const std::string &raw)
{
    std::pair<int, std::string> result{0, ""};
    auto conn = co_await coro::net::TcpStream::connect("127.0.0.1", 8080);
    if (!conn.valid())
        co_return result;
    co_await conn.write(raw.data(), raw.size());

    http_parse parser(HTTP_RESPONSE);
    bool done = false;
    parser.message_complete = [&]()
    { done = true; };
    char buf[4096];
    while (!done)
    {
        int n = co_await conn.read(buf, sizeof(buf));
        if (n <= 0)
            break;
        parser.feed(buf, (size_t)n);
    }
    result.first = parser.status_code;
    result.second = parser.http_body;
    co_return result;
}

coro::Task<> selftest_client(web_server &server)
{
    int pass = 0, fail = 0;
    auto check = [&](const char *name, bool ok)
    {
        std::cout << "[selftest] " << (ok ? "PASS" : "FAIL") << ": " << name << std::endl;
        if (ok)
            ++pass;
        else
            ++fail;
    };

    co_await coro::sleep(50ms); // 等 accept 循环就绪

    // 1. pipelining + keep-alive: 一条连接连发两个请求, 期望两条 200
    {
        std::string wire = http_protocol::request("GET", "/", {{"Host", "127.0.0.1"}}) +
                           http_protocol::request("GET", "/greet?name=coro", {{"Host", "127.0.0.1"}});
        auto conn = co_await coro::net::TcpStream::connect("127.0.0.1", 8080);
        if (!conn.valid())
        {
            check("connect refused", false);
        }
        else
        {
            co_await conn.write(wire.data(), wire.size());
            http_parse parser(HTTP_RESPONSE);
            int completed = 0;
            std::string last_body;
            parser.message_complete = [&]()
            { ++completed; last_body = parser.http_body; };
            char buf[4096];
            while (completed < 2)
            {
                int n = co_await conn.read(buf, sizeof(buf));
                if (n <= 0)
                    break;
                parser.feed(buf, (size_t)n);
            }
            check("pipelined 2 requests -> 2 responses", completed == 2);
            check("greet body echoes query", last_body == "hello, coro!\n");
        }
    }

    // 2. 未知路径 → 404
    {
        auto [status, body] = co_await one_request(
            http_protocol::request("GET", "/nope", {{"Host", "127.0.0.1"}}));
        check("unknown path -> 404", status == 404);
    }

    // 3. POST body 回显
    {
        auto [status, body] = co_await one_request(
            http_protocol::json("POST", "/echo", "{\"x\":1}", {{"Host", "127.0.0.1"}}));
        check("POST /echo -> 200", status == 200);
        check("POST body echoed", body.find("{\"x\":1}") != std::string::npos);
    }

    // 4. 静态文件
    {
        auto [status, body] = co_await one_request(
            http_protocol::request("GET", "/static/hello.txt", {{"Host", "127.0.0.1"}}));
        check("static file -> 200", status == 200);
        check("static content", body.find("hello from www") != std::string::npos);
    }

    // 5. handler 异常 → 500
    {
        auto [status, body] = co_await one_request(
            http_protocol::request("GET", "/boom", {{"Host", "127.0.0.1"}}));
        check("handler exception -> 500", status == 500);
    }

    // 6. 动态路由: 参数捕获
    {
        auto [status, body] = co_await one_request(
            http_protocol::request("GET", "/user/42", {{"Host", "127.0.0.1"}}));
        check("dynamic route :id -> 200", status == 200);
        check("param id captured", body.find("\"user_id\": \"42\"") != std::string::npos);
    }

    // 7. 动态路由: 多参数
    {
        auto [status, body] = co_await one_request(
            http_protocol::request("GET", "/user/7/posts/99", {{"Host", "127.0.0.1"}}));
        check("multi-param route -> 200", status == 200);
        check("param post_id captured", body.find("\"post\": \"99\"") != std::string::npos);
    }

    // 8. 静态优先: /user/admin 命中静态路由 (而非被 :id 捕获)
    {
        auto [status, body] = co_await one_request(
            http_protocol::request("GET", "/user/admin", {{"Host", "127.0.0.1"}}));
        check("static wins over param", status == 200 && body == "admin console\n");
    }

    // 9. 通配路由: 吞掉剩余全部路径
    {
        auto [status, body] = co_await one_request(
            http_protocol::request("GET", "/download/a/b/c.txt", {{"Host", "127.0.0.1"}}));
        check("wildcard route -> 200", status == 200);
        check("wildcard captured", body.find("a/b/c.txt") != std::string::npos);
    }

    // 10. 405: 路径存在但方法不允许 (POST /greet 只注册了 GET)
    {
        auto [status, body] = co_await one_request(
            http_protocol::request("POST", "/greet", {{"Host", "127.0.0.1"}}));
        check("wrong method -> 405", status == 405);
    }

    // 11. OPTIONS: 自动应答允许的方法 (RFC 7231)
    {
        auto [status, body] = co_await one_request(
            http_protocol::request("OPTIONS", "/user/42", {{"Host", "127.0.0.1"}}));
        check("OPTIONS -> 200", status == 200);
    }

    // 12. 静态文件目录拒绝非 GET (POST /static/hello.txt → 405)
    {
        auto [status, body] = co_await one_request(
            http_protocol::request("POST", "/static/hello.txt", {{"Host", "127.0.0.1"}}));
        check("static dir POST -> 405", status == 405);
    }

    std::cout << "[selftest] " << pass << " passed, " << fail << " failed" << std::endl;
    server.stop(); // 优雅停止: serve() 随之退出
}

// ---- 高并发压测 ----

// 单个压测客户端: 一条 keep-alive 连接上连续发送 rounds 个请求,
// 每个请求都等对应响应解析完成后再发下一个 (非 pipelining, 模拟真实浏览器)
coro::Task<> stress_client(int rounds, std::atomic<long long> &ok,
                           std::atomic<long long> &conn_fail,
                           std::atomic<long long> &io_fail)
{
    auto conn = co_await coro::net::TcpStream::connect("127.0.0.1", 8080);
    // 高并发 connect 风暴可能撞上 TCP backlog 上限被拒绝 (正常拥塞控制),
    // 真实客户端会重试 → 压测工具同样重试几次
    for (int retry = 0; !conn.valid() && retry < 3; ++retry)
    {
        co_await coro::sleep(5ms);
        conn = co_await coro::net::TcpStream::connect("127.0.0.1", 8080);
    }
    if (!conn.valid())
    {
        ++conn_fail; // 重试后仍失败 (服务器过载等)
        co_return;
    }

    http_parse parser(HTTP_RESPONSE);
    int completed = 0;
    parser.message_complete = [&]()
    { ++completed; };

    std::string wire = http_protocol::request("GET", "/greet?name=stress",
                                              {{"Host", "127.0.0.1"}});
    char buf[4096];
    for (int i = 0; i < rounds; ++i)
    {
        int target = completed + 1;
        co_await conn.write(wire.data(), wire.size());
        while (completed < target)
        {
            int n = co_await conn.read(buf, sizeof(buf));
            if (n <= 0)
            {
                ++io_fail; // 连接中断
                co_return;
            }
            parser.feed(buf, (size_t)n);
        }
        ++ok;
    }
}

coro::Task<> stress_main(web_server &server, int clients, int rounds)
{
    std::cout << "[stress] clients=" << clients << " rounds=" << rounds
              << " workers=" << server.worker_count() << std::endl;
    co_await coro::sleep(50ms); // 等 accept 循环就绪

    std::atomic<long long> ok{0}, conn_fail{0}, io_fail{0};
    std::vector<coro::Task<>> tasks;
    tasks.reserve(clients);

    auto t0 = std::chrono::steady_clock::now();
    for (int i = 0; i < clients; ++i)
        tasks.push_back(coro::spawn(stress_client(rounds, ok, conn_fail, io_fail)));
    for (auto &t : tasks)
        co_await std::move(t);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                  std::chrono::steady_clock::now() - t0)
                  .count();

    long long expected = (long long)clients * rounds;
    std::cout << "[stress] completed=" << ok << "/" << expected
              << " elapsed=" << ms << "ms"
              << " (" << (ms ? expected * 1000 / ms : 0) << " req/s)" << std::endl;
    std::cout << "[stress] conn_fail=" << conn_fail << " io_fail=" << io_fail << std::endl;
    std::cout << "[stress] " << (ok == expected ? "ALL OK" : "MISSING RESPONSES") << std::endl;
    server.stop(); // 优雅停止
}






// ---- 入口 ----

// 信号到达时的关停协程: stop() 关监听 socket (非阻塞),
// 挂起的 accept 以错误完成包返回 → serve() 循环检查 running_ 退出
coro::Task<> shutdown_task(web_server &s)
{
    std::cout << "[web] shutdown signal received" << std::endl;
    s.stop();
    co_return;
}

int main(int argc, char **argv)
{
    bool selftest = argc > 1 && std::string(argv[1]) == "--selftest";
    bool stress = argc > 1 && std::string(argv[1]) == "--stress";

    web_server server;
    register_routes(server);
    if (!server.listen("127.0.0.1", 8080))
        return 1;

    if (selftest)
    {
        // 服务器后台运行; 客户端验证完毕后 stop() → serve() 退出,
        // wait_all() 确保所有连接协程结束后再退出 (帧无泄漏)
        coro::run([](web_server &s) -> coro::Task<>
                  {
            auto srv = coro::spawn(s.serve());
            co_await selftest_client(s);
            co_await std::move(srv);
            s.wait_all(); }(server));
        return 0;
    }

    if (stress)
    {
        int clients = argc > 2 ? std::atoi(argv[2]) : 200;
        int rounds = argc > 3 ? std::atoi(argv[3]) : 50;
        if (clients <= 0 || rounds <= 0)
        {
            std::cerr << "usage: web_server --stress [clients] [rounds]" << std::endl;
            return 1;
        }
        server.set_verbose(false); // 压测: 关闭访问日志, 减少输出开销
        coro::run([](web_server &s, int c, int r) -> coro::Task<>
                  {
            auto srv = coro::spawn(s.serve());
            co_await stress_main(s, c, r);
            co_await std::move(srv);
            s.wait_all(); }(server, clients, rounds));
        return 0;
    }

    // 常驻模式: Ctrl+C (SIGINT) / Ctrl+Break (SIGBREAK) 优雅关停 ——
    // 信号到达 → server.stop() (关监听 socket, accept 循环自然退出)
    // → wait_all() 等所有连接协程收尾 → 事件循环干净退出
    coro::run([](web_server &s) -> coro::Task<>
              {
        auto srv = coro::spawn(s.serve());
        // 工厂 lambda 捕获 s (普通函数非协程, 安全): 返回关停协程
        coro::signal::handler sig = coro::signal::handle(
            SIGINT, [&s] { return shutdown_task(s); });
#ifdef _WIN32
        coro::signal::handler sig2 = coro::signal::handle(
            SIGBREAK, [&s] { return shutdown_task(s); });
#endif
        co_await std::move(srv);
        s.wait_all(); }(server));
    return 0;
}
