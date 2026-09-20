// test_net.cpp — TCP 回环: accept / connect / read / write (IOCP / io_uring)
#if defined(_WIN32) || (defined(__linux__) && (!defined(CORO_HAS_URING) || CORO_HAS_URING))
#include <gtest/gtest.h>

#include <coro/coro.hpp>
#include <coro/net.hpp>

#include "test_util.h"

using namespace std::chrono_literals;

#ifdef _WIN32
TEST(NetTest, MissingIocpFailsListenerCreation) {
    bool created = true;
    bool valid = true;
    std::thread isolated([&] {
        auto& loop = coro::EventLoop::get();
        loop.set_event_source(std::make_shared<coro::CVEventSource>());
        coro::net::TcpListener listener;
        created = listener.bind_listen("127.0.0.1", 0);
        valid = listener.valid();
    });
    isolated.join();

    EXPECT_FALSE(created);
    EXPECT_FALSE(valid);
}
#endif

namespace {

    // ── 命名协程函数 ──

    // wait_for 超时取消挂起中的 IO: read 挂起 (对端不发数据) → 超时取消
    // → 底层 IOCP 读被 CancelIoEx 取消 → 协程收到 CancelledError
    coro::Task<> read_once(coro::net::TcpStream conn) {
        char buf[64];
        (void)co_await conn.read(buf, sizeof(buf)); // 对端不发数据 → 挂起
    }

    coro::Task<> server_read_once(coro::net::TcpListener* listener, bool* accepted, bool* read_cancelled) {
        auto conn = co_await listener->accept();
        if (!conn.valid())
            co_return;
        *accepted = true;

        try {
            co_await coro::wait_for(read_once(std::move(conn)), 50ms);
        } catch (const coro::TimeoutError&) {
            *read_cancelled = true; // 超时: 挂起的 read 已被取消 (CancelIoEx)
        }
    }

    coro::Task<> silent_client(bool* connected) {
        auto conn = co_await coro::net::TcpStream::connect("127.0.0.1", 18925);
        if (conn.valid()) {
            *connected = true;
            co_await coro::sleep(150ms); // 保持连接但不发数据
            conn.close();
        }
    }

    coro::Task<> io_cancel_scenario(bool* accepted, bool* read_cancelled, bool* connected) {
        coro::net::TcpListener listener;
        if (!listener.bind_listen("127.0.0.1", 18925))
            co_return;
        auto server = coro::spawn(server_read_once(&listener, accepted, read_cancelled));
        auto client = coro::spawn(silent_client(connected));
        co_await std::move(server);
        co_await std::move(client);
    }

    // echo 服务器: 接收一个连接, 把数据原样写回
    coro::Task<> echo_handler(coro::net::TcpStream conn, int* server_received) {
        char buf[256];
        int n = co_await conn.read(buf, sizeof(buf));
        if (n > 0) {
            *server_received = n;
            co_await conn.write(buf, (size_t)n);
        }
    }

    coro::Task<> server_side(coro::net::TcpListener* listener, int* server_received, bool* accepted) {
        auto conn = co_await listener->accept();
        if (conn.valid()) {
            *accepted = true;
            co_await echo_handler(std::move(conn), server_received);
        }
    }

    coro::Task<> client_side(std::string* echo, bool* connected, int* sent_out) {
        auto conn = co_await coro::net::TcpStream::connect("127.0.0.1", 18923);
        if (!conn.valid())
            co_return;

        *connected = true;
        const char* msg = "hello coro test";
        int sent = co_await conn.write(msg, strlen(msg));
        *sent_out = sent; // 断言放 TEST 里 (协程内不能用 EXPECT 宏)

        char buf[256];
        int n = co_await conn.read(buf, sizeof(buf));
        if (n > 0)
            *echo = std::string(buf, (size_t)n);
        conn.close();
    }

    coro::Task<> echo_scenario(std::string* echo, int* server_received, bool* accepted, bool* connected, int* sent_out,
                               bool* bind_ok) {
        coro::net::TcpListener listener;
        if (!listener.bind_listen("127.0.0.1", 18923)) {
            *bind_ok = false;
            co_return;
        }
        *bind_ok = true;

        auto server = coro::spawn(server_side(&listener, server_received, accepted));
        auto client = coro::spawn(client_side(echo, connected, sent_out));

        co_await std::move(server);
        co_await std::move(client);
    }

    // 服务器读端关闭检测: 客户端关闭后 read 返回 0
    coro::Task<> close_detection(coro::net::TcpListener* listener, int* read_result) {
        auto conn = co_await listener->accept();
        if (!conn.valid())
            co_return;
        char buf[16];
        // 客户端立即 close → read 应返回 0 (对端关闭)
        int n = co_await conn.read(buf, sizeof(buf));
        *read_result = n;
    }

    coro::Task<> closer_client(bool* connected) {
        auto conn = co_await coro::net::TcpStream::connect("127.0.0.1", 18924);
        if (conn.valid()) {
            *connected = true;
            conn.close(); // 连接后立即关闭
        }
    }

    coro::Task<> close_scenario(int* read_result, bool* connected, bool* bind_ok) {
        coro::net::TcpListener listener;
        if (!listener.bind_listen("127.0.0.1", 18924)) {
            *bind_ok = false;
            co_return;
        }
        *bind_ok = true;
        auto server = coro::spawn(close_detection(&listener, read_result));
        auto client = coro::spawn(closer_client(connected));
        co_await std::move(server);
        co_await std::move(client);
    }

#ifdef _WIN32
    coro::Task<> accept_once(coro::net::TcpListener* listener) {
        (void)co_await listener->accept();
    }

    coro::Task<> cancel_one_accept(coro::net::TcpListener* listener) {
        auto task = coro::spawn(accept_once(listener));
        co_await coro::yield(); // 让 AcceptEx 提交后再取消
        task.cancel();
        try {
            co_await std::move(task);
        } catch (const coro::CancelledError&) {
        }
    }

    coro::Task<> accept_cancel_handle_scenario(bool* bind_ok, bool* counted, DWORD* before, DWORD* after) {
        coro::net::TcpListener listener;
        for (unsigned short port = 19130; port < 19150; ++port) {
            if (listener.bind_listen("127.0.0.1", port)) {
                *bind_ok = true;
                break;
            }
        }
        if (!*bind_ok)
            co_return;

        // 先预热一次，排除 Winsock/AcceptEx 的一次性初始化句柄。
        co_await cancel_one_accept(&listener);
        if (!GetProcessHandleCount(GetCurrentProcess(), before))
            co_return;

        for (int i = 0; i < 64; ++i)
            co_await cancel_one_accept(&listener);

        co_await coro::yield();
        *counted = GetProcessHandleCount(GetCurrentProcess(), after) != FALSE;
    }
#endif

} // namespace

TEST(NetTest, TcpEchoRoundTrip) {
    std::string echo;
    int server_received = 0, sent_out = 0;
    bool accepted = false, connected = false, bind_ok = false;
    test_util::run_task(
        [&] { return echo_scenario(&echo, &server_received, &accepted, &connected, &sent_out, &bind_ok); });
    ASSERT_TRUE(bind_ok);
    EXPECT_TRUE(accepted);
    EXPECT_TRUE(connected);
    EXPECT_GT(sent_out, 0);
    EXPECT_EQ(server_received, 15); // strlen("hello coro test") = 15
    EXPECT_EQ(echo, "hello coro test");
}

TEST(NetTest, PeerCloseYieldsZero) {
    int read_result = -1;
    bool connected = false, bind_ok = false;
    test_util::run_task([&] { return close_scenario(&read_result, &connected, &bind_ok); });
    ASSERT_TRUE(bind_ok);
    EXPECT_TRUE(connected);
    EXPECT_EQ(read_result, 0); // read 返回 0 = 对端关闭
}

TEST(NetTest, WaitForTimeoutCancelsPendingIo) {
    bool accepted = false, read_cancelled = false, connected = false;
    test_util::run_task([&] { return io_cancel_scenario(&accepted, &read_cancelled, &connected); });
    EXPECT_TRUE(accepted);
    EXPECT_TRUE(connected);
    EXPECT_TRUE(read_cancelled); // 挂起中的读被超时取消 (CancelIoEx 联动), 无泄漏无挂死
}

#ifdef _WIN32
TEST(NetTest, CancelPendingAcceptDoesNotLeakHandles) {
    bool bind_ok = false;
    bool counted = false;
    DWORD before = 0;
    DWORD after = 0;
    test_util::run_task([&] { return accept_cancel_handle_scenario(&bind_ok, &counted, &before, &after); });
    ASSERT_TRUE(bind_ok);
    ASSERT_TRUE(counted);
    EXPECT_LE(after, before + 2); // 允许测试进程内部极小的瞬态波动
}
#endif
#endif // _WIN32 || __linux__
