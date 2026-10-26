// test_http_client.cpp — coro::HttpClient 的本地集成测试
//
// 对端全部是 127.0.0.1 上的可控脚本协程: 不访问公网, 不依赖 Web 框架, 也不需要 TLS
// (真实 HTTPS 握手在 test_tls.cpp 里用测试证书覆盖)。端口用 0 交给系统分配, 再读
// local_port(), 不用固定端口 + sleep 去猜"服务应该起来了"。
//
// 门控: 客户端是可选组件 (CORO_ENABLE_HTTP_CLIENT), 且需要原生 socket 后端。
#if defined(CORO_HAS_HTTP_CLIENT) && CORO_HAS_HTTP_CLIENT
#if defined(_WIN32) || (defined(__linux__) && defined(CORO_HAS_URING) && CORO_HAS_URING)

#include <gtest/gtest.h>

#include <coro/coro.hpp>
#include <coro/http_client.hpp>
#include <coro/net.hpp>

#include "test_util.h"

#include <chrono>
#include <random>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

using namespace std::chrono_literals;

namespace {

    const char* k_ok_response = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";

    // ── 对端基础设施 ──

    coro::Task<bool> send_all(coro::net::TcpStream& conn, std::string_view data) {
        std::size_t sent = 0;
        while (sent < data.size()) {
            const int n = co_await conn.write(data.data() + sent, data.size() - sent);
            if (n <= 0)
                co_return false;
            sent += static_cast<std::size_t>(n);
        }
        co_return true;
    }

    // 读一个完整请求 (头 + Content-Length 指定的体); 空串表示请求头没到连接就关了。
    coro::Task<std::string> read_request(coro::net::TcpStream& conn) {
        std::string request;
        char buf[512];
        while (request.find("\r\n\r\n") == std::string::npos) {
            const int n = co_await conn.read(buf, sizeof(buf));
            if (n <= 0)
                co_return request;
            request.append(buf, static_cast<std::size_t>(n));
        }
        const auto head_end = request.find("\r\n\r\n");
        std::size_t body_wanted = 0;
        if (const auto pos = request.find("Content-Length: "); pos != std::string::npos && pos < head_end)
            body_wanted = static_cast<std::size_t>(std::stoul(request.substr(pos + 16)));
        while (request.size() - (head_end + 4) < body_wanted) {
            const int n = co_await conn.read(buf, sizeof(buf));
            if (n <= 0)
                break;
            request.append(buf, static_cast<std::size_t>(n));
        }
        co_return request;
    }

    // 本地对端: 动态端口 + 已接受连接数 + 最后一次收到的请求原文。
    struct peer {
        coro::net::TcpListener listener;
        unsigned short port = 0;
        int accepts = 0;
        std::string last_request;

        bool start() {
            if (!listener.bind_listen("127.0.0.1", 0))
                return false;
            port = listener.local_port();
            return port != 0;
        }

        std::string url(std::string path = "/") const { return "http://127.0.0.1:" + std::to_string(port) + path; }
    };

    // 接受一次连接, 回一个固定响应, 然后关闭。
    coro::Task<> script_once(peer* p, std::string response) {
        auto conn = co_await p->listener.accept();
        if (!conn.valid())
            co_return;
        ++p->accepts;
        p->last_request = co_await read_request(conn);
        (void)co_await send_all(conn, response);
    }

    // 接受一次连接, 依次服务 n 个请求 —— 用于验证 keep-alive 复用。
    coro::Task<> script_keep_alive(peer* p, std::string response, int n) {
        auto conn = co_await p->listener.accept();
        if (!conn.valid())
            co_return;
        ++p->accepts;
        for (int i = 0; i < n; ++i) {
            std::string request = co_await read_request(conn);
            if (request.empty())
                break;
            p->last_request = std::move(request);
            if (!co_await send_all(conn, response))
                break;
        }
    }

    // 分段写出: 每片之间让出事件循环, 覆盖"一次 read 拿不到完整响应"的路径。
    coro::Task<> script_fragmented(peer* p, std::vector<std::string> pieces) {
        auto conn = co_await p->listener.accept();
        if (!conn.valid())
            co_return;
        ++p->accepts;
        p->last_request = co_await read_request(conn);
        for (const auto& piece : pieces) {
            if (!co_await send_all(conn, piece))
                co_return;
            co_await coro::sleep(5ms);
        }
    }

    // 第 1 条连接: 回一个"声明 10 字节但只发 5 字节"的响应后关闭; 第 2 条: 正常响应。
    // 用于验证坏连接不会被归还给下一个借用者。
    coro::Task<> script_bad_then_good(peer* p, std::string good_response) {
        {
            auto conn = co_await p->listener.accept();
            if (!conn.valid())
                co_return;
            ++p->accepts;
            p->last_request = co_await read_request(conn);
            (void)co_await send_all(conn, "HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nshort");
        } // 连接在这里关闭: 客户端会看到不完整响应
        {
            auto conn = co_await p->listener.accept();
            if (!conn.valid())
                co_return;
            ++p->accepts;
            p->last_request = co_await read_request(conn);
            (void)co_await send_all(conn, good_response);
        }
    }

    // 读到请求后先占住连接不响应, 之后才回复 —— 用于占满连接池 / 验证排队与取消。
    coro::Task<> script_stall(peer* p, std::chrono::milliseconds hold, std::string response) {
        auto conn = co_await p->listener.accept();
        if (!conn.valid())
            co_return;
        ++p->accepts;
        p->last_request = co_await read_request(conn);
        co_await coro::sleep(hold);
        (void)co_await send_all(conn, response);
    }

    // 同上, 但会再接受一次连接并立即回复: 验证取消/关闭后客户端还能继续工作。
    coro::Task<> script_stall_then_serve(peer* p, std::chrono::milliseconds hold, std::string response) {
        {
            auto conn = co_await p->listener.accept();
            if (!conn.valid())
                co_return;
            ++p->accepts;
            p->last_request = co_await read_request(conn);
            co_await coro::sleep(hold);
            (void)co_await send_all(conn, response);
        }
        {
            auto conn = co_await p->listener.accept();
            if (!conn.valid())
                co_return;
            ++p->accepts;
            p->last_request = co_await read_request(conn);
            (void)co_await send_all(conn, response);
        }
    }

    // ── 客户端调用助手 (协程内不能用 gtest 宏, 结果结构带回 TEST) ──

    struct call_result {
        int status = 0;
        coro::HttpHeaders headers;
        coro::HttpHeaders trailers;
        std::string body;
        bool keep_alive = false;
        bool timed_out = false;
        bool cancelled = false;
        std::string error; // 非空 = 失败

        bool ok() const { return error.empty(); }
    };

    coro::Task<call_result> perform(coro::HttpClient& client, coro::HttpRequest request) {
        call_result result;
        try {
            auto response = co_await client.request(std::move(request));
            result.status = response.status;
            result.headers = std::move(response.headers);
            result.trailers = std::move(response.trailers);
            result.body = std::move(response.body);
            result.keep_alive = response.keep_alive;
        } catch (const coro::TimeoutError& e) {
            result.timed_out = true;
            result.error = e.what();
        } catch (const coro::CancelledError& e) {
            result.cancelled = true;
            result.error = e.what();
        } catch (const std::exception& e) {
            result.error = e.what();
        } catch (...) {
            result.error = "non-std exception";
        }
        co_return result;
    }

    coro::Task<call_result> fetch(coro::HttpClient& client, std::string url) {
        coro::HttpRequest request;
        request.url = std::move(url);
        co_return co_await perform(client, std::move(request));
    }

    // 收回对端脚本: 最多等 5s。脚本若因为断言先失败而没等到连接, 这里只保证测试进程
    // 不挂死 —— 具体问题由 TEST 里的断言暴露。超时取消会连带取消挂起的 accept。
    coro::Task<void> join_peer(coro::Task<>& server) {
        try {
            co_await coro::wait_for(std::move(server), std::chrono::seconds(5));
        } catch (const coro::TimeoutError&) {
        }
    }

    // 取某个头/尾部的全部值 (字段名大小写不敏感, 顺序保留)
    std::vector<std::string> values_of(const coro::HttpHeaders& fields, std::string_view name) {
        std::vector<std::string> out;
        const auto lower = [](char c) { return c >= 'A' && c <= 'Z' ? static_cast<char>(c + ('a' - 'A')) : c; };
        for (const auto& [field, value] : fields) {
            if (field.size() != name.size())
                continue;
            bool equal = true;
            for (std::size_t i = 0; i < name.size(); ++i) {
                if (lower(field[i]) != lower(name[i])) {
                    equal = false;
                    break;
                }
            }
            if (equal)
                out.push_back(value);
        }
        return out;
    }

    // ── aclose / 背压 / 发送侧限额 (C03 回归) ──

    coro::Task<call_result> in_flight_request(coro::HttpClient* client, std::string url) {
        co_return co_await fetch(*client, std::move(url));
    }

    struct aclose_probe {
        bool request_done = false;
        bool request_ok = false;
        bool aclose_saw_request_done = false;
        std::string aclose_error;
    };

    coro::Task<> aclose_scenario(peer* p, aclose_probe* out) {
        coro::HttpClient client;
        auto server = coro::spawn(script_stall(p, 40ms, k_ok_response));
        auto req = coro::spawn(in_flight_request(&client, p->url()));
        // 确定性屏障: 对端已 accept 说明请求确实在途 (不能只 yield 一次)
        for (int i = 0; i < 2000 && p->accepts == 0; ++i)
            co_await coro::sleep(std::chrono::milliseconds(1));
        try {
            co_await client.aclose(); // 屏障: 必须等在途请求退出
            out->aclose_saw_request_done = true;
        } catch (const std::exception& e) {
            out->aclose_error = e.what();
        }
        try {
            auto result = co_await std::move(req);
            out->request_ok = result.ok() && result.status == 200;
            if (!result.ok())
                out->aclose_error = result.error;
        } catch (...) {
            out->request_ok = false;
        }
        out->request_done = true;
        co_await join_peer(server);
        co_return;
    }

    struct overload_probe {
        bool first_ok = false;
        bool second_rejected = false;
    };

    coro::Task<> overload_scenario(peer* p, overload_probe* out) {
        coro::HttpClient::Options options;
        options.max_total_requests = 1; // 只允许一个在途请求
        coro::HttpClient client(options);
        auto server = coro::spawn(script_stall(p, 40ms, k_ok_response));
        auto first = coro::spawn(in_flight_request(&client, p->url()));
        for (int i = 0; i < 2000 && p->accepts == 0; ++i)
            co_await coro::sleep(std::chrono::milliseconds(1));
        try {
            (void)co_await client.get(p->url());
        } catch (const coro::OverloadError&) {
            out->second_rejected = true;
        }
        try {
            auto result = co_await std::move(first);
            out->first_ok = result.ok() && result.status == 200;
        } catch (...) {
            out->first_ok = false;
        }
        co_await join_peer(server);
        co_return;
    }

    struct wire_limit_probe {
        bool rejected = false;
        std::string error;
    };

    coro::Task<> wire_limit_scenario(peer* p, wire_limit_probe* out) {
        coro::HttpClient::Options options;
        options.max_request_wire_bytes = 32; // 请求行 + 头就超了
        coro::HttpClient client(options);
        try {
            (void)co_await client.get(p->url());
        } catch (const coro::HttpError& e) {
            out->rejected = true;
            out->error = e.what();
        }
        co_return;
    }

    // 发一个固定响应, 收起对端脚本 (避免测试进程残留挂起的 accept)
    coro::Task<> fetch_scripted(peer* p, const char* response, call_result* out) {
        coro::HttpClient client;
        auto server = coro::spawn(script_once(p, response));
        *out = co_await fetch(client, p->url());
        co_await join_peer(server);
        co_return;
    }

    // ── 流式响应 (E05) ──
    struct stream_probe {
        int status = 0;
        int chunks = 0;
        std::string body;
        bool eof = false;
        std::string error;
    };

    coro::Task<> drain_stream(coro::HttpResponseStream* s, stream_probe* out) {
        try {
            out->status = s->status();
            for (;;) {
                auto chunk = co_await s->read_chunk();
                if (!chunk) {
                    out->eof = true;
                    break;
                }
                ++out->chunks;
                out->body += *chunk;
            }
            co_await s->aclose(); // 已读完后重复调用必须幂等
        } catch (const std::exception& e) {
            out->error = e.what();
        }
    }

    coro::Task<> stream_scenario(peer* p, std::vector<std::string> pieces, stream_probe* out) {
        coro::HttpClient client;
        auto server = coro::spawn(script_fragmented(p, std::move(pieces)));
        try {
            coro::HttpRequest request;
            request.url = p->url();
            auto stream = co_await client.stream(std::move(request));
            co_await drain_stream(&stream, out);
        } catch (const std::exception& e) {
            out->error = e.what();
        }
        co_await join_peer(server);
    }

    coro::Task<> stream_read_all(coro::HttpClient* client, std::string url, std::string* into) {
        coro::HttpRequest request;
        request.url = std::move(url);
        auto stream = co_await client->stream(std::move(request));
        *into += "<h";
        for (;;) {
            auto chunk = co_await stream.read_chunk();
            if (!chunk)
                break;
            *into += *chunk;
        }
        *into += ">";
        co_await stream.aclose();
    }

    coro::Task<> stream_keep_alive_scenario(peer* p, int* accepts, std::string* bodies, std::string* error) {
        coro::HttpClient client;
        auto server = coro::spawn(script_keep_alive(p, k_ok_response, 2));
        for (int i = 0; i < 2; ++i) {
            try {
                co_await coro::wait_for(stream_read_all(&client, p->url(), bodies), 2000ms);
            } catch (const std::exception& e) {
                *error = e.what();
                break;
            }
        }
        *accepts = p->accepts;
        co_await join_peer(server);
    }

    // ── 随机脏数据输入 (C05 fuzz-lite): 只要求“不挂死、不崩溃、可观察失败”──
    coro::Task<> script_garbage(peer* p, unsigned seed) {
        auto conn = co_await p->listener.accept();
        if (!conn.valid())
            co_return;
        ++p->accepts;
        p->last_request = co_await read_request(conn);
        std::mt19937 rng(seed);
        std::uniform_int_distribution<int> len_dist(0, 64);
        std::uniform_int_distribution<int> byte_dist(0, 255);
        for (int chunk = 0; chunk < 3; ++chunk) {
            const int n = len_dist(rng);
            std::string bytes;
            bytes.reserve(static_cast<std::size_t>(n));
            for (int i = 0; i < n; ++i)
                bytes.push_back(static_cast<char>(byte_dist(rng)));
            if (!bytes.empty() && !co_await send_all(conn, bytes))
                co_return;
            co_await coro::sleep(1ms);
        }
    }

    struct fuzz_probe {
        int iterations = 0;
        bool hung = false;
        bool crashed = false;
    };

    coro::Task<> garbage_case(peer* p, unsigned seed, fuzz_probe* out) {
        coro::HttpClient client;
        auto server = coro::spawn(script_garbage(p, seed));
        try {
            (void)co_await coro::wait_for(fetch(client, p->url()), 2000ms);
        } catch (const coro::TimeoutError&) {
            out->hung = true; // 真正的缺陷: 脏数据把客户端挂死
        } catch (...) {
            // 解析/连接错误: 可观察失败即可
        }
        co_await join_peer(server);
    }

} // namespace

// ── 基本往返: 响应体、重复头 ──

TEST(HttpClientTest, GetReturnsBodyAndDuplicateHeaders) {
    peer p;
    ASSERT_TRUE(p.start()) << "无法监听 127.0.0.1";
    coro::HttpClient client;
    call_result result;

    test_util::run_task([&]() -> coro::Task<> {
        auto server = coro::spawn(script_once(&p, "HTTP/1.1 200 OK\r\n"
                                                  "Content-Type: text/plain\r\n"
                                                  "Set-Cookie: a=1\r\n"
                                                  "Set-Cookie: b=2\r\n"
                                                  "Content-Length: 5\r\n\r\nhello"));
        result = co_await fetch(client, p.url("/greet"));
        co_await join_peer(server);
    });

    ASSERT_TRUE(result.ok()) << result.error;
    EXPECT_EQ(result.status, 200);
    EXPECT_EQ(result.body, "hello");
    EXPECT_EQ(values_of(result.headers, "Content-Type").size(), 1u);
    const auto cookies = values_of(result.headers, "Set-Cookie");
    ASSERT_EQ(cookies.size(), 2u) << "重复同名头必须全部保留";
    EXPECT_EQ(cookies[0], "a=1");
    EXPECT_EQ(cookies[1], "b=2");
    EXPECT_TRUE(result.keep_alive) << "HTTP/1.1 默认 keep-alive";
}

TEST(HttpClientTest, PostSendsManagedHeadersAndBody) {
    peer p;
    ASSERT_TRUE(p.start());
    coro::HttpClient client;
    call_result result;

    test_util::run_task([&]() -> coro::Task<> {
        auto server = coro::spawn(script_once(&p, k_ok_response));
        coro::HttpRequest request;
        request.method = "POST";
        request.url = p.url("/submit");
        request.body = "payload=1";
        result = co_await perform(client, std::move(request));
        co_await join_peer(server);
    });

    ASSERT_TRUE(result.ok()) << result.error;
    EXPECT_EQ(result.body, "ok");
    EXPECT_NE(p.last_request.find("POST /submit HTTP/1.1"), std::string::npos) << p.last_request;
    EXPECT_NE(p.last_request.find("Content-Length: 9"), std::string::npos) << p.last_request;
    EXPECT_NE(p.last_request.find("payload=1"), std::string::npos);
}

TEST(HttpClientTest, HeadResponseHasNoBody) {
    peer p;
    ASSERT_TRUE(p.start());
    coro::HttpClient client;
    call_result result;

    test_util::run_task([&]() -> coro::Task<> {
        auto server = coro::spawn(script_once(&p, "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\n"));
        coro::HttpRequest request;
        request.method = "HEAD";
        request.url = p.url("/head");
        result = co_await perform(client, std::move(request));
        co_await join_peer(server);
    });

    ASSERT_TRUE(result.ok()) << result.error;
    EXPECT_EQ(result.status, 200);
    EXPECT_TRUE(result.body.empty()) << "HEAD 响应按合同不读响应体";
}

TEST(HttpClientTest, EmptyBodyResponse) {
    peer p;
    ASSERT_TRUE(p.start());
    coro::HttpClient client;
    call_result result;

    test_util::run_task([&]() -> coro::Task<> {
        auto server = coro::spawn(script_once(&p, "HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n"));
        result = co_await fetch(client, p.url("/empty"));
        co_await join_peer(server);
    });

    ASSERT_TRUE(result.ok()) << result.error;
    EXPECT_EQ(result.status, 204);
    EXPECT_TRUE(result.body.empty());
}

// ── 报文形态: 分段、chunked、trailers、1xx ──

TEST(HttpClientTest, FragmentedResponseIsAssembled) {
    peer p;
    ASSERT_TRUE(p.start());
    coro::HttpClient client;
    call_result result;

    test_util::run_task([&]() -> coro::Task<> {
        std::vector<std::string> pieces = {"HTTP/1.1 200 ", "OK\r\nContent-Length: 11\r\n\r\nhel", "lo world"};
        auto server = coro::spawn(script_fragmented(&p, std::move(pieces)));
        result = co_await fetch(client, p.url("/fragmented"));
        co_await join_peer(server);
    });

    ASSERT_TRUE(result.ok()) << result.error;
    EXPECT_EQ(result.status, 200);
    EXPECT_EQ(result.body, "hello world");
}

TEST(HttpClientTest, ChunkedResponseWithTrailer) {
    peer p;
    ASSERT_TRUE(p.start());
    coro::HttpClient client;
    call_result result;

    test_util::run_task([&]() -> coro::Task<> {
        auto server = coro::spawn(script_once(&p, "HTTP/1.1 200 OK\r\n"
                                                  "Transfer-Encoding: chunked\r\n"
                                                  "Trailer: X-Sum\r\n\r\n"
                                                  "3\r\nabc\r\n"
                                                  "2\r\nde\r\n"
                                                  "0\r\nX-Sum: 7\r\n\r\n"));
        result = co_await fetch(client, p.url("/chunked"));
        co_await join_peer(server);
    });

    ASSERT_TRUE(result.ok()) << result.error;
    EXPECT_EQ(result.status, 200);
    EXPECT_EQ(result.body, "abcde") << "分块边界不进入响应体";
    const auto sums = values_of(result.trailers, "X-Sum");
    ASSERT_EQ(sums.size(), 1u) << "trailer 必须单独暴露";
    EXPECT_EQ(sums[0], "7");
}

TEST(HttpClientTest, InformationalResponseIsSkipped) {
    peer p;
    ASSERT_TRUE(p.start());
    coro::HttpClient client;
    call_result result;

    test_util::run_task([&]() -> coro::Task<> {
        auto server = coro::spawn(script_once(&p, "HTTP/1.1 100 Continue\r\n\r\n"
                                                  "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nhi"));
        result = co_await fetch(client, p.url("/continue"));
        co_await join_peer(server);
    });

    ASSERT_TRUE(result.ok()) << result.error;
    EXPECT_EQ(result.status, 200) << "1xx 不是最终响应";
    EXPECT_EQ(result.body, "hi");
}

// ── 失败路径 ──

TEST(HttpClientTest, TruncatedResponseFails) {
    peer p;
    ASSERT_TRUE(p.start());
    coro::HttpClient client;
    call_result result;

    test_util::run_task([&]() -> coro::Task<> {
        auto server = coro::spawn(script_once(&p, "HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nshort"));
        result = co_await fetch(client, p.url("/truncated"));
        co_await join_peer(server);
    });

    EXPECT_FALSE(result.ok()) << "正文没读满就 EOF 必须报错, 不能当成功返回";
    EXPECT_FALSE(result.timed_out);
    EXPECT_FALSE(result.cancelled);
}

TEST(HttpClientTest, MalformedResponseFails) {
    peer p;
    ASSERT_TRUE(p.start());
    coro::HttpClient client;
    call_result result;

    test_util::run_task([&]() -> coro::Task<> {
        auto server = coro::spawn(script_once(&p, "NOT-HTTP\r\n\r\n"));
        result = co_await fetch(client, p.url("/malformed"));
        co_await join_peer(server);
    });

    EXPECT_FALSE(result.ok());
    EXPECT_NE(result.error.find("parse failed"), std::string::npos) << result.error;
}

TEST(HttpClientTest, BodyLimitRejectsResponse) {
    peer p;
    ASSERT_TRUE(p.start());
    coro::HttpClient::Options options;
    options.max_body_bytes = 4; // 小于对端声明的 6 字节
    coro::HttpClient client(options);
    call_result result;

    test_util::run_task([&]() -> coro::Task<> {
        auto server = coro::spawn(script_once(&p, "HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nabcdef"));
        result = co_await fetch(client, p.url("/big-body"));
        co_await join_peer(server);
    });

    EXPECT_FALSE(result.ok()) << "超过 max_body_bytes 必须拒绝";
    EXPECT_TRUE(result.body.empty());
}

TEST(HttpClientTest, WireLimitRejectsResponse) {
    peer p;
    ASSERT_TRUE(p.start());
    coro::HttpClient::Options options;
    options.max_response_wire_bytes = 16; // 连响应头都装不下
    coro::HttpClient client(options);
    call_result result;

    test_util::run_task([&]() -> coro::Task<> {
        auto server = coro::spawn(script_once(&p, k_ok_response));
        result = co_await fetch(client, p.url("/too-much-wire"));
        co_await join_peer(server);
    });

    EXPECT_FALSE(result.ok()) << "超过 max_response_wire_bytes 必须拒绝";
    EXPECT_NE(result.error.find("wire limit"), std::string::npos) << result.error;
}

// 头部字节数与字段数上限: 两条都必须在解析阶段拒绝, 而不是先把数据收进内存。
TEST(HttpClientTest, HeaderLimitsRejectResponse) {
    // 头部字节上限: 8 字节连响应行都装不下
    peer tight;
    ASSERT_TRUE(tight.start());
    coro::HttpClient::Options bytes_options;
    bytes_options.max_header_bytes = 8;
    coro::HttpClient bytes_client(bytes_options);
    call_result bytes_result;

    // 头部数量上限: 只允许 1 个字段, 对端发 4 个
    peer crowded;
    ASSERT_TRUE(crowded.start());
    coro::HttpClient::Options count_options;
    count_options.max_header_count = 1;
    coro::HttpClient count_client(count_options);
    call_result count_result;

    test_util::run_task([&]() -> coro::Task<> {
        auto server_bytes = coro::spawn(script_once(&tight, k_ok_response));
        bytes_result = co_await fetch(bytes_client, tight.url("/header-bytes"));
        co_await join_peer(server_bytes);

        auto server_count = coro::spawn(script_once(&crowded, "HTTP/1.1 200 OK\r\nA: 1\r\nB: 2\r\nC: 3\r\n"
                                                              "Content-Length: 2\r\n\r\nok"));
        count_result = co_await fetch(count_client, crowded.url("/header-count"));
        co_await join_peer(server_count);
    });

    EXPECT_FALSE(bytes_result.ok()) << "超过 max_header_bytes 必须拒绝";
    EXPECT_FALSE(count_result.ok()) << "超过 max_header_count 必须拒绝";
}

// ── 连接池: 复用、隔离、坏连接归还 ──

TEST(HttpClientTest, KeepAliveReusesOneConnection) {
    peer p;
    ASSERT_TRUE(p.start());
    coro::HttpClient client;
    call_result first, second;

    test_util::run_task([&]() -> coro::Task<> {
        auto server = coro::spawn(script_keep_alive(&p, k_ok_response, 2));
        first = co_await fetch(client, p.url("/one"));
        second = co_await fetch(client, p.url("/two"));
        co_await join_peer(server);
    });

    ASSERT_TRUE(first.ok()) << first.error;
    ASSERT_TRUE(second.ok()) << second.error;
    EXPECT_EQ(p.accepts, 1) << "第二次请求应复用同一条 keep-alive 连接";
}

TEST(HttpClientTest, DifferentOriginsDoNotSharePools) {
    peer a;
    peer b;
    ASSERT_TRUE(a.start());
    ASSERT_TRUE(b.start());
    coro::HttpClient client;
    std::vector<call_result> results;

    test_util::run_task([&]() -> coro::Task<> {
        auto server_a = coro::spawn(script_keep_alive(&a, k_ok_response, 2));
        auto server_b = coro::spawn(script_keep_alive(&b, k_ok_response, 2));
        std::vector<std::string> urls = {a.url("/a1"), a.url("/a2"), b.url("/b1"), b.url("/b2")};
        for (const auto& url : urls)
            results.push_back(co_await fetch(client, url));
        co_await join_peer(server_a);
        co_await join_peer(server_b);
    });

    ASSERT_EQ(results.size(), 4u);
    for (const auto& r : results)
        ASSERT_TRUE(r.ok()) << r.error;
    EXPECT_EQ(a.accepts, 1) << "同一 origin 复用";
    EXPECT_EQ(b.accepts, 1) << "不同端口的池必须隔离";
}

TEST(HttpClientTest, FailedConnectionIsNotReused) {
    peer p;
    ASSERT_TRUE(p.start());
    coro::HttpClient client;
    call_result broken, retried;

    test_util::run_task([&]() -> coro::Task<> {
        auto server = coro::spawn(script_bad_then_good(&p, k_ok_response));
        broken = co_await fetch(client, p.url("/broken"));
        retried = co_await fetch(client, p.url("/retried"));
        co_await join_peer(server);
    });

    EXPECT_FALSE(broken.ok()) << "不完整响应必须失败";
    ASSERT_TRUE(retried.ok()) << retried.error;
    EXPECT_EQ(retried.body, "ok");
    EXPECT_EQ(p.accepts, 2) << "失败连接不能归还复用, 下一次必须重新建连";
}

// ── 连接池: 排队超时、关闭唤醒、取消 ──

TEST(HttpClientTest, QueueTimeoutWhenSingleConnectionIsBusy) {
    peer p;
    ASSERT_TRUE(p.start());
    coro::HttpClient::Options options;
    options.max_connections_per_origin = 1;
    options.max_idle_per_origin = 1;
    options.request_timeout = 100ms; // 远短于对端占住连接的 400ms
    coro::HttpClient client(options);
    call_result holding, queued;

    test_util::run_task([&]() -> coro::Task<> {
        auto server = coro::spawn(script_stall(&p, 400ms, k_ok_response));
        auto first = coro::spawn(fetch(client, p.url("/holding")));
        co_await coro::sleep(20ms); // 让第一个请求真正拿到那唯一的连接
        auto second = coro::spawn(fetch(client, p.url("/queued")));
        holding = co_await std::move(first);
        queued = co_await std::move(second);
        co_await join_peer(server);
    });

    EXPECT_TRUE(holding.timed_out) << holding.error;
    EXPECT_TRUE(queued.timed_out) << "排队中的请求超时后必须失败, 不能无限等待: " << queued.error;
}

TEST(HttpClientTest, CloseWakesPoolWaiterAndRejectsNewWork) {
    peer p;
    ASSERT_TRUE(p.start());
    coro::HttpClient::Options options;
    options.max_connections_per_origin = 1;
    options.request_timeout = 2000ms; // 安全网: 若 close 没唤醒等待者, 这里会超时而不是挂死
    coro::HttpClient client(options);
    call_result in_flight, queued, after_close;

    test_util::run_task([&]() -> coro::Task<> {
        auto server = coro::spawn(script_stall(&p, 300ms, k_ok_response));
        auto first = coro::spawn(fetch(client, p.url("/in-flight")));
        co_await coro::sleep(30ms);
        auto second = coro::spawn(fetch(client, p.url("/queued")));
        co_await coro::sleep(30ms);
        client.close(); // 唤醒排队者, 并拒绝新任务
        queued = co_await std::move(second);
        in_flight = co_await std::move(first); // 在途请求保留资源, 按自己的期限结束
        after_close = co_await fetch(client, p.url("/after-close"));
        co_await join_peer(server);
    });

    EXPECT_FALSE(queued.ok()) << "close 后排队请求必须失败";
    EXPECT_FALSE(queued.timed_out) << "等待者应当被 close 唤醒, 而不是等到超时";
    EXPECT_NE(queued.error.find("closed"), std::string::npos) << queued.error;
    EXPECT_TRUE(in_flight.ok()) << "在途请求不应被 close 打断: " << in_flight.error;
    EXPECT_NE(after_close.error.find("closed"), std::string::npos) << after_close.error;
}

TEST(HttpClientTest, InFlightRequestCanBeCancelled) {
    peer p;
    ASSERT_TRUE(p.start());
    coro::HttpClient::Options options;
    options.request_timeout = 2000ms; // 取消先于超时发生
    coro::HttpClient client(options);
    call_result cancelled;
    call_result after_cancel;

    test_util::run_task([&]() -> coro::Task<> {
        auto server = coro::spawn(script_stall_then_serve(&p, 300ms, k_ok_response));
        auto task = client.get(p.url("/slow"));
        task.start();
        co_await coro::sleep(40ms); // 让它挂起在读取响应上
        task.cancel();
        try {
            (void)co_await std::move(task);
            cancelled.error = "<no exception>";
        } catch (const coro::CancelledError&) {
            cancelled.cancelled = true;
        } catch (const std::exception& e) {
            cancelled.error = e.what();
        }
        // 取消应释放连接名额, 后续请求照常工作
        after_cancel = co_await fetch(client, p.url("/after-cancel"));
        co_await join_peer(server);
    });

    EXPECT_TRUE(cancelled.cancelled) << cancelled.error;
    EXPECT_TRUE(cancelled.error.empty()) << cancelled.error;
    ASSERT_TRUE(after_cancel.ok()) << after_cancel.error;
}

// ── 生命周期: close 后再 await、移动所有权 ──

TEST(HttpClientTest, CloseBeforeAwaitRejectsAndMoveKeepsWorking) {
    peer p;
    ASSERT_TRUE(p.start());
    coro::HttpClient closing;
    coro::HttpClient movable;
    call_result early;
    call_result moved;

    test_util::run_task([&]() -> coro::Task<> {
        // Task 是惰性的: 创建时不执行。close() 之后再 await 必须被拒绝, 而不是去碰
        // 已关闭的共享状态 (close 标记的是共享 impl, 移动也救不回来)。
        auto lazy = closing.get(p.url("/early"));
        closing.close();
        try {
            (void)co_await std::move(lazy);
            early.error = "<no exception>";
        } catch (const std::exception& e) {
            early.error = e.what();
        }

        // 移动一个仍然打开的客户端: 所有权转移后照常服务
        coro::HttpClient owner = std::move(movable);
        auto server = coro::spawn(script_once(&p, k_ok_response));
        moved = co_await fetch(owner, p.url("/moved"));
        co_await join_peer(server);
    });

    EXPECT_NE(early.error.find("closed"), std::string::npos) << early.error;
    ASSERT_TRUE(moved.ok()) << moved.error;
    EXPECT_EQ(moved.body, "ok");
}

// ── 无 TLS 构建: HTTPS 必须明确失败, 而不是明文发出去 ──

#if !defined(CORO_HAS_TLS) || !CORO_HAS_TLS
TEST(HttpClientTest, HttpsWithoutTlsIsRejected) {
    coro::HttpClient client;
    ASSERT_FALSE(coro::HttpClient::supports_tls());
    call_result result;

    test_util::run_task([&]() -> coro::Task<> { result = co_await fetch(client, "https://127.0.0.1:1/"); });

    EXPECT_FALSE(result.ok());
    EXPECT_NE(result.error.find("TLS"), std::string::npos) << result.error;
}
#endif

TEST(HttpClientTest, AcloseWaitsForInflightRequest) {
    peer p;
    ASSERT_TRUE(p.start());
    aclose_probe probe;
    test_util::run_task([&]() -> coro::Task<> { co_await aclose_scenario(&p, &probe); });
    EXPECT_TRUE(probe.aclose_error.empty()) << probe.aclose_error;
    EXPECT_TRUE(probe.aclose_saw_request_done) << "aclose 返回时在途请求必须已经结束";
    EXPECT_TRUE(probe.request_ok) << "在途请求不应因为 aclose 而失败";
}

TEST(HttpClientTest, OverloadIsRejectedNotQueued) {
    peer p;
    ASSERT_TRUE(p.start());
    overload_probe probe;
    test_util::run_task([&]() -> coro::Task<> { co_await overload_scenario(&p, &probe); });
    EXPECT_TRUE(probe.second_rejected) << "超过在途上限必须报 OverloadError, 不能静默排队";
    EXPECT_TRUE(probe.first_ok) << "已接受的在途请求不应被背压影响";
}

TEST(HttpClientTest, OversizedRequestWireIsRejected) {
    peer p;
    ASSERT_TRUE(p.start());
    wire_limit_probe probe;
    test_util::run_task([&]() -> coro::Task<> { co_await wire_limit_scenario(&p, &probe); });
    EXPECT_TRUE(probe.rejected) << "发送侧 wire 超限必须报错";
}

TEST(HttpClientTest, StreamReadsChunkedBodyInPieces) {
    peer p;
    ASSERT_TRUE(p.start());
    stream_probe probe;
    // 头部与每个分块分开到达, 验证“头部完成即可开始消费”
    std::vector<std::string> pieces{"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n",
                                    "5\r\nhello\r\n",
                                    "6\r\n world\r\n",
                                    "0\r\n\r\n"};
    test_util::run_task([&]() -> coro::Task<> { co_await stream_scenario(&p, std::move(pieces), &probe); });
    EXPECT_TRUE(probe.error.empty()) << probe.error;
    EXPECT_EQ(probe.status, 200);
    EXPECT_EQ(probe.body, "hello world");
    EXPECT_TRUE(probe.eof) << "读到底应返回 nullopt";
}

TEST(HttpClientTest, GarbageResponseNeverHangsOrCrashes) {
    // C05 fuzz-lite: 固定种子的随机字节流, 只断言“不挂死”。
    // 真正的 libFuzzer 套件需单独构建; 这里是每个 PR 都能跑的确定性版本。
    fuzz_probe probe;
    for (unsigned seed = 1; seed <= 24 && !probe.hung; ++seed) {
        peer p;
        ASSERT_TRUE(p.start());
        test_util::run_task([&]() -> coro::Task<> { co_await garbage_case(&p, seed, &probe); });
        ++probe.iterations;
    }
    EXPECT_EQ(probe.iterations, 24);
    EXPECT_FALSE(probe.hung) << "随机脏数据让 HTTP 客户端挂死";
}

TEST(HttpClientTest, StreamReturnsConnectionToPool) {
    peer p;
    ASSERT_TRUE(p.start());
    int accepts = -1;
    std::string bodies, error;
    test_util::run_task([&]() -> coro::Task<> { co_await stream_keep_alive_scenario(&p, &accepts, &bodies, &error); });
    EXPECT_TRUE(error.empty()) << error;
    EXPECT_NE(bodies.find("<hok>"), std::string::npos) << bodies;
    EXPECT_EQ(accepts, 1) << "读完的流必须把连接交还池中复用";
}

TEST(HttpClientTest, ConflictingContentLengthIsRejected) {
    peer p;
    ASSERT_TRUE(p.start());
    call_result result;
    test_util::run_task([&]() -> coro::Task<> {
        co_await fetch_scripted(
            &p, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nContent-Length: 3\r\n\r\nabc", &result);
    });
    EXPECT_FALSE(result.ok()) << "冲突的 Content-Length 必须拒绝, 不能默默选一个";
}

TEST(HttpClientTest, TransferEncodingWithContentLengthIsRejected) {
    peer p;
    ASSERT_TRUE(p.start());
    call_result result;
    test_util::run_task([&]() -> coro::Task<> {
        co_await fetch_scripted(
            &p, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nok\r\n0\r\n\r\n",
            &result);
    });
    EXPECT_FALSE(result.ok()) << "Transfer-Encoding 与 Content-Length 共存必须拒绝 (请求走私面)";
}

#endif // _WIN32 || __linux__ (io_uring)
#endif // CORO_HAS_HTTP_CLIENT
