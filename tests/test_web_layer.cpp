// test_web_layer.cpp — 中间件执行机制: 顺序/短路/next 最多一次/冻结/异常传播
//
// 门控: router.h 依赖 coro/fs.hpp (route_core 静态文件分支), 仅在
// I/O 后端可用的平台上编译 (与 test_net 系列一致)。
#if defined(_WIN32) || (defined(__linux__) && defined(CORO_HAS_URING) && CORO_HAS_URING)
#include <gtest/gtest.h>

#include <coro/coro.hpp>

#include "router.h"
#include "test_util.h"

#ifdef _WIN32
#include <coro/net.hpp>
#include "web_server.h"
#define CORO_WEB_LAYER_HAS_SERVER 1
#elif defined(__linux__)
#include <coro/net.hpp>
#include "web_server.h"
#define CORO_WEB_LAYER_HAS_SERVER 1
#endif

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <exception>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

using namespace std::chrono_literals;

namespace {

    // ---- 断言带回通道: gtest 宏只能在非协程函数使用, 协程通过指针带回结果 ----
    std::vector<std::string>* g_order = nullptr; // 执行顺序记录 (仅单线程用例设置)
    bool* g_flag = nullptr;
    bool* g_param_ok = nullptr;

    http_request make_req(std::string method, std::string url) {
        http_request req;
        req.method = std::move(method);
        req.url = std::move(url);
        req.keep_alive = false;
        return req;
    }

    // ---- 中间件/handler: 命名函数协程 (参数进帧, 规避临时闭包生命周期问题) ----

    coro::Task<http_response> mw_log(http_request& req, router::next_fn next) {
        (void)req;
        if (g_order)
            g_order->push_back("log:in");
        auto resp = co_await next();
        if (g_order)
            g_order->push_back("log:out");
        co_return resp;
    }

    coro::Task<http_response> mw_auth(http_request& req, router::next_fn next) {
        g_order->push_back("auth:in");
        if (req.header("Authorization").empty()) { // 短路: 不调用 next() 合法
            g_order->push_back("auth:short");
            co_return http_response::error(401, "unauthorized");
        }
        auto resp = co_await next();
        g_order->push_back("auth:out");
        co_return resp;
    }

    coro::Task<http_response> echo_handler(const http_request& req) {
        if (g_order)
            g_order->push_back("handler");
        co_return http_response::text("ok:" + req.param("id"));
    }

    coro::Task<http_response> mw_double_next(http_request&, router::next_fn next) {
        auto first = co_await next();
        try {
            (void)co_await next(); // 第二次调用: operator() 同步抛 logic_error
        } catch (const std::logic_error&) {
            *g_flag = true;
        }
        co_return first;
    }

    coro::Task<http_response> mw_move_next(http_request&, router::next_fn next) {
        auto consumed = std::move(next); // 按值接管: 源对象失效
        auto resp = co_await consumed();
        try {
            (void)next(); // moved-from: 抛 logic_error
        } catch (const std::logic_error&) {
            *g_flag = true;
        }
        co_return resp;
    }

    coro::Task<http_response> mw_read_param(http_request& req, router::next_fn next) {
        *g_param_ok = (req.param("id") == "42"); // 路由级: params 已填充
        co_return co_await next();
    }

    coro::Task<http_response> mw_mark(http_request&, router::next_fn next) {
        auto resp = co_await next();
        resp.header("X-Mark", "1"); // 响应后段: 改写响应
        co_return resp;
    }

    coro::Task<http_response> slow_handler(const http_request&) {
        co_await coro::sleep(200ms);
        co_return http_response::text("late");
    }

    coro::Task<http_response> mw_slow(http_request&, router::next_fn next) {
        // 超时不自行转换: TimeoutError 原样向上传播 (route 调用方可见)
        co_return co_await coro::wait_for(next(), 30ms);
    }

    coro::Task<http_response> mw_timeout504(http_request&, router::next_fn next) {
        // 推荐写法: 超时中间件自行转换为合适的响应, 不依赖兜底 500
        try {
            co_return co_await coro::wait_for(next(), 30ms);
        } catch (const coro::TimeoutError&) {
            co_return http_response::error(504, "gateway timeout");
        }
    }

    coro::Task<http_response> cancel_handler(const http_request&) {
        // 模拟取消型异常: 必须原样穿过中间件, 不被吞成普通响应
        throw coro::CancelledError();
        co_return http_response::text("");
    }

#ifdef CORO_WEB_LAYER_HAS_SERVER
    constexpr unsigned short kServerPorts[] = {19311, 19312, 19313};

    coro::Task<http_response> boom_handler_fn(const http_request&) {
        bool do_throw = true; // 规避编译器对 throw 后不可达代码的警告
        if (do_throw)
            throw std::runtime_error("boom");
        co_return http_response::text("");
    }

    unsigned short listen_for_test(web_server& server) {
        for (auto port : kServerPorts) {
            if (server.listen("127.0.0.1", port))
                return port;
        }
        return 0;
    }

    // 同一 keep-alive 连接串行收发, 完整读取响应体后才发送下一请求。
    coro::Task<std::string> fetch_response(coro::net::TcpStream& conn, std::string path) {
        const std::string request = "GET " + path + " HTTP/1.1\r\nHost: t\r\n\r\n";
        size_t sent = 0;
        while (sent < request.size()) {
            int n = co_await conn.write(request.data() + sent, request.size() - sent);
            if (n <= 0)
                throw std::runtime_error("test request write failed");
            sent += static_cast<size_t>(n);
        }
        std::string response;
        char buf[512];
        while (true) {
            int n = co_await conn.read(buf, sizeof(buf));
            if (n <= 0)
                throw std::runtime_error("test response incomplete");
            response.append(buf, static_cast<size_t>(n));
            auto end = response.find("\r\n\r\n");
            if (end == std::string::npos)
                continue;
            auto length = response.find("\r\nContent-Length: ");
            if (length == std::string::npos || length > end)
                throw std::runtime_error("test response missing Content-Length");
            auto size = std::stoull(response.substr(length + 18));
            if (response.size() >= end + 4 + size)
                co_return response;
        }
    }

    coro::Task<http_response> mw_deny_stats(http_request& req, router::next_fn next) {
        if (req.path_view() == "/__stats")
            co_return http_response::error(403, "forbidden");
        co_return co_await next();
    }

    coro::Task<> fetch_status(unsigned short port, int* status, bool* connected) {
        auto conn = co_await coro::net::TcpStream::connect("127.0.0.1", port);
        if (!conn.valid()) {
            *connected = false;
            co_return;
        }
        *connected = true;
        const char* req = "GET /boom HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n";
        co_await conn.write(req, std::char_traits<char>::length(req));
        std::string resp;
        char buf[512];
        while (resp.find("\r\n\r\n") == std::string::npos) {
            int n = co_await conn.read(buf, sizeof(buf) - 1);
            if (n <= 0)
                break;
            resp.append(buf, (size_t)n);
        }
        if (resp.size() >= 12)
            *status = std::atoi(resp.c_str() + 9); // "HTTP/1.1 500 ..." → 500
    }
#endif // CORO_WEB_LAYER_HAS_SERVER

} // namespace

// ── 执行顺序: 全局外层 → 路由级 → handler → 路由级后段 → 全局后段 ──
TEST(WebLayerTest, OrderGlobalOuterRouteInner) {
    std::vector<std::string> order;
    int status = 0;
    std::string body;
    g_order = &order;
    auto scenario = [&]() -> coro::Task<> {
        router r;
        r.use(mw_log);
        r.get("/u/:id", echo_handler, {mw_auth});
        auto req = make_req("GET", "/u/7");
        req.headers["Authorization"] = "Bearer x";
        auto resp = co_await r.route(req);
        status = resp.status;
        body = resp.body;
    };
    test_util::run_task(scenario);
    g_order = nullptr;
    EXPECT_EQ(status, 200);
    EXPECT_EQ(body, "ok:7");
    EXPECT_EQ(order, (std::vector<std::string>{"log:in", "auth:in", "handler", "auth:out", "log:out"}));
}

// ── 路由级短路 401: handler 不执行, 外层全局仍收尾 ──
TEST(WebLayerTest, RouteMiddlewareShortCircuits) {
    std::vector<std::string> order;
    int status = 0;
    g_order = &order;
    auto scenario = [&]() -> coro::Task<> {
        router r;
        r.use(mw_log);
        r.get("/u/:id", echo_handler, {mw_auth});
        auto req = make_req("GET", "/u/7"); // 无 Authorization
        auto resp = co_await r.route(req);
        status = resp.status;
    };
    test_util::run_task(scenario);
    g_order = nullptr;
    EXPECT_EQ(status, 401);
    EXPECT_EQ(order, (std::vector<std::string>{"log:in", "auth:in", "auth:short", "log:out"}));
}

// ── 404 也经过全局链 ──
TEST(WebLayerTest, GlobalChainCovers404) {
    std::vector<std::string> order;
    int status = 0;
    g_order = &order;
    auto scenario = [&]() -> coro::Task<> {
        router r;
        r.use(mw_log); // 不注册任何路由
        auto req = make_req("GET", "/nowhere");
        auto resp = co_await r.route(req);
        status = resp.status;
    };
    test_util::run_task(scenario);
    g_order = nullptr;
    EXPECT_EQ(status, 404);
    EXPECT_EQ(order, (std::vector<std::string>{"log:in", "log:out"}));
}

// ── next() 重复调用: 同步抛 logic_error ──
TEST(WebLayerTest, DoubleNextThrows) {
    bool threw = false;
    g_flag = &threw;
    auto scenario = [&]() -> coro::Task<> {
        router r;
        r.use(mw_double_next);
        r.get("/x", echo_handler);
        auto req = make_req("GET", "/x");
        auto resp = co_await r.route(req);
        EXPECT_EQ(resp.status, 200);
    };
    test_util::run_task(scenario);
    g_flag = nullptr;
    EXPECT_TRUE(threw);
}

// ── moved-from next_fn 调用: 抛 logic_error ──
TEST(WebLayerTest, MovedFromNextThrows) {
    bool threw = false;
    g_flag = &threw;
    auto scenario = [&]() -> coro::Task<> {
        router r;
        r.use(mw_move_next);
        r.get("/x", echo_handler);
        auto req = make_req("GET", "/x");
        auto resp = co_await r.route(req);
        EXPECT_EQ(resp.status, 200);
    };
    test_util::run_task(scenario);
    g_flag = nullptr;
    EXPECT_TRUE(threw);
}

// ── 路由级中间件可读取已填充的路由参数 ──
TEST(WebLayerTest, RouteMiddlewareReadsParams) {
    bool param_ok = false;
    g_param_ok = &param_ok;
    auto scenario = [&]() -> coro::Task<> {
        router r;
        r.get("/p/:id", echo_handler, {mw_read_param});
        auto req = make_req("GET", "/p/42");
        (void)co_await r.route(req);
    };
    test_util::run_task(scenario);
    g_param_ok = nullptr;
    EXPECT_TRUE(param_ok);
}

// ── 全局中间件后段可改写响应 ──
TEST(WebLayerTest, MiddlewareRewritesResponse) {
    auto scenario = [&]() -> coro::Task<> {
        router r;
        r.use(mw_mark);
        r.get("/x", echo_handler);
        auto req = make_req("GET", "/x");
        auto resp = co_await r.route(req);
        EXPECT_NE(resp.build().find("X-Mark: 1\r\n"), std::string::npos);
    };
    test_util::run_task(scenario);
}

// ── 无全局中间件快路径 + 路由级链共存 ──
TEST(WebLayerTest, FastPathStillRunsRouteMiddlewares) {
    std::vector<std::string> order;
    int status = 0;
    g_order = &order;
    auto scenario = [&]() -> coro::Task<> {
        router r; // 未 use(): route() 走快路径
        r.get("/u/:id", echo_handler, {mw_auth});
        auto req = make_req("GET", "/u/9");
        req.headers["Authorization"] = "Bearer x";
        auto resp = co_await r.route(req);
        status = resp.status;
    };
    test_util::run_task(scenario);
    g_order = nullptr;
    EXPECT_EQ(status, 200);
    EXPECT_EQ(order, (std::vector<std::string>{"auth:in", "handler", "auth:out"}));
}

// ── 超时: TimeoutError 从 route() 原样向上传播 ──
TEST(WebLayerTest, TimeoutPropagatesFromRoute) {
    bool timed_out = false;
    g_flag = &timed_out;
    auto scenario = [&]() -> coro::Task<> {
        router r;
        r.use(mw_slow);
        r.get("/slow", slow_handler);
        auto req = make_req("GET", "/slow");
        try {
            (void)co_await r.route(req);
        } catch (const coro::TimeoutError&) {
            timed_out = true;
        }
    };
    test_util::run_task(scenario);
    g_flag = nullptr;
    EXPECT_TRUE(timed_out);
}

// ── 推荐写法: 超时中间件自行转换为 504 ──
TEST(WebLayerTest, TimeoutMiddlewareConvertsTo504) {
    int status = 0;
    auto scenario = [&]() -> coro::Task<> {
        router r;
        r.use(mw_timeout504);
        r.get("/slow", slow_handler);
        auto req = make_req("GET", "/slow");
        auto resp = co_await r.route(req);
        status = resp.status;
    };
    test_util::run_task(scenario);
    EXPECT_EQ(status, 504);
}

// ── 取消型异常原样穿透中间件, 不被吞 ──
TEST(WebLayerTest, CancelledErrorPropagatesThroughMiddlewares) {
    bool saw_cancel = false;
    bool propagated = false;
    g_flag = &saw_cancel;
    auto scenario = [&]() -> coro::Task<> {
        router r;
        r.use([](http_request& req, router::next_fn next) -> coro::Task<http_response> {
            try {
                co_return co_await next();
            } catch (...) {
                *g_flag = true; // 中间件看到取消型异常
                throw;          // 但必须 rethrow, 不吞
            }
        });
        r.get("/cancel", cancel_handler);
        auto req = make_req("GET", "/cancel");
        try {
            (void)co_await r.route(req);
        } catch (const coro::CancelledError&) {
            propagated = true;
        }
    };
    test_util::run_task(scenario);
    g_flag = nullptr;
    EXPECT_TRUE(saw_cancel);
    EXPECT_TRUE(propagated);
}

// ── freeze() 后所有注册入口拒绝 (抛 logic_error, Release 生效) ──
TEST(WebLayerTest, FrozenRouterRejectsRegistration) {
    router r;
    r.get("/x", echo_handler);
    r.freeze();
    EXPECT_THROW(r.use(mw_log), std::logic_error);
    EXPECT_THROW(r.get("/y", echo_handler), std::logic_error);
    EXPECT_THROW(r.post("/z", echo_handler), std::logic_error);
    EXPECT_THROW(r.add("PUT", "/w", echo_handler), std::logic_error);
    EXPECT_THROW(r.static_dir("/s", "/tmp"), std::logic_error);
}

// ── 冻结后的 router 可被多线程只读并发使用 ──
TEST(WebLayerTest, FrozenRouterConcurrentReadOnly) {
    router r;
    r.use(mw_log);
    r.get("/u/:id", echo_handler);
    r.freeze();

    std::atomic<int> ok{0};
    auto worker = [&r, &ok](int id) {
        try {
            test_util::run_task([&]() -> coro::Task<> {
                for (int i = 0; i < 10; ++i) {
                    auto req = make_req("GET", "/u/" + std::to_string(id));
                    auto resp = co_await r.route(req);
                    if (resp.status == 200)
                        ok.fetch_add(1);
                }
            });
        } catch (...) {
        }
    };
    std::thread t1(worker, 1), t2(worker, 2);
    t1.join();
    t2.join();
    EXPECT_EQ(ok.load(), 20);
}

#ifdef CORO_WEB_LAYER_HAS_SERVER
// ── 真实服务器: handler 未捕获异常经 dispatch 兜底返回 500 ──
// (dispatch 是 web_server 私有方法, 500 兜底只能走真实请求验证;
//  等价集成验证也存在于 Web/main.cpp selftest 的 /boom 用例)
TEST(WebLayerTest, RealServerUnhandledExceptionBecomes500) {
    web_server server(1);
    server.routes().get("/boom", boom_handler_fn);
    bool listened = false;
    unsigned short port = 0;
    for (unsigned short p : kServerPorts) {
        if (server.listen("127.0.0.1", p)) {
            listened = true;
            port = p;
            break;
        }
    }
    if (!listened)
        GTEST_SKIP() << "no test port available";

    int status = 0;
    bool connected = false;
    test_util::run_task([&]() -> coro::Task<> {
        auto serve = coro::spawn(server.serve());
        co_await coro::sleep(50ms); // 等 accept 就绪
        co_await fetch_status(port, &status, &connected);
        server.stop();
        co_await std::move(serve);
    });
    EXPECT_TRUE(connected);
    EXPECT_EQ(status, 500);
}

// 真实请求验证注册早于冻结、全局链覆盖统计路由、成功/错误响应累计与连接计数。
TEST(WebLayerTest, StatsRouteRegisteredBeforeFreezeAndCountsResponses) {
    web_server server(2);
    server.set_verbose(false);
    server.routes().use(mw_mark);
    server.routes().get("/ok", echo_handler);
    server.routes().get("/boom", boom_handler_fn);
    const auto port = listen_for_test(server);
    if (!port)
        GTEST_SKIP() << "no test port available";

    bool frozen = false;
    std::vector<std::string> responses;
    test_util::run_task([&]() -> coro::Task<> {
        auto serve = coro::spawn(server.serve());
        std::exception_ptr error;
        try {
            co_await coro::sleep(50ms);
            try {
                server.routes().get("/late", echo_handler);
            } catch (const std::logic_error&) {
                frozen = true;
            }
            // 空闲连接也应计入 in_flight; 第二个连接串行处理请求。
            auto idle = co_await coro::net::TcpStream::connect("127.0.0.1", port);
            auto conn = co_await coro::net::TcpStream::connect("127.0.0.1", port);
            if (!idle.valid() || !conn.valid())
                throw std::runtime_error("test connect failed");
            const char* paths[] = {"/ok", "/missing", "/boom", "/__stats", "/__stats"};
            for (const auto* path : paths)
                responses.push_back(co_await fetch_response(conn, path));
        } catch (...) {
            error = std::current_exception();
        }
        server.stop();
        co_await std::move(serve);
        if (error)
            std::rethrow_exception(error);
    });
    server.wait_all();

    EXPECT_TRUE(frozen);
    ASSERT_EQ(responses.size(), 5u);
    EXPECT_EQ(responses[0].find("HTTP/1.1 200"), 0u);
    EXPECT_EQ(responses[1].find("HTTP/1.1 404"), 0u);
    EXPECT_EQ(responses[2].find("HTTP/1.1 500"), 0u);
    EXPECT_EQ(responses[3].find("HTTP/1.1 200"), 0u);
    EXPECT_NE(responses[3].find("Content-Type: application/json"), std::string::npos);
    EXPECT_NE(responses[3].find("X-Mark: 1\r\n"), std::string::npos);
    EXPECT_NE(responses[3].find("\"requests\":3,"), std::string::npos);
    EXPECT_NE(responses[3].find("\"errors\":2,"), std::string::npos);
    EXPECT_NE(responses[3].find("\"in_flight\":2,"), std::string::npos);
    EXPECT_NE(responses[3].find("\"peak\":2,"), std::string::npos);
    EXPECT_NE(responses[3].find("\"workers\":["), std::string::npos);
    EXPECT_NE(responses[4].find("\"requests\":4,"), std::string::npos);
    EXPECT_NE(responses[4].find("\"errors\":2,"), std::string::npos);

    // 停止并清空 worker 后直接读取快照, 所有连接计数必须归零。
    std::string body;
    test_util::run_task([&]() -> coro::Task<> {
        auto req = make_req("GET", "/__stats");
        auto resp = co_await server.routes().route(req);
        body = std::move(resp.body);
    });
    EXPECT_NE(body.find("\"requests\":5,"), std::string::npos);
    EXPECT_NE(body.find("\"errors\":2,"), std::string::npos);
    EXPECT_NE(body.find("\"in_flight\":0,"), std::string::npos);
    EXPECT_NE(body.find("\"peak\":2,"), std::string::npos);
    EXPECT_NE(body.find("\"workers\":[0,0]"), std::string::npos);
}

TEST(WebLayerTest, StatsRouteCanBeShortCircuitedByGlobalMiddleware) {
    web_server server(1);
    server.set_verbose(false);
    server.routes().use(mw_deny_stats);
    const auto port = listen_for_test(server);
    if (!port)
        GTEST_SKIP() << "no test port available";

    std::string response;
    test_util::run_task([&]() -> coro::Task<> {
        auto serve = coro::spawn(server.serve());
        std::exception_ptr error;
        try {
            co_await coro::sleep(50ms);
            auto conn = co_await coro::net::TcpStream::connect("127.0.0.1", port);
            if (!conn.valid())
                throw std::runtime_error("test connect failed");
            response = co_await fetch_response(conn, "/__stats");
        } catch (...) {
            error = std::current_exception();
        }
        server.stop();
        co_await std::move(serve);
        if (error)
            std::rethrow_exception(error);
    });
    server.wait_all();
    EXPECT_EQ(response.find("HTTP/1.1 403"), 0u);
    EXPECT_EQ(response.find("\"requests\":"), std::string::npos);
}
#endif // CORO_WEB_LAYER_HAS_SERVER

#endif // _WIN32 || __linux__ + uring
