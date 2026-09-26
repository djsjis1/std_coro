// test_server_lifecycle.cpp — 服务器生命周期与任务所有权回归 (计划 C2e)
//
// 覆盖场景: 空载关停、在途连接排空、宽限期到点取消、handler 卡在真实 read 上被唤醒、
// handler 抛异常不漏计数、重复关闭幂等、两个服务共用同一 loop 互不误停、
// Server 先于滞留 handler 析构、并发上限拒绝、UDP 满载丢弃与零长度数据报。
#include <gtest/gtest.h>

#include <coro/coro.hpp>
#include <coro/net.hpp>
#if defined(_WIN32) || (defined(__linux__) && defined(CORO_HAS_URING) && CORO_HAS_URING)
#include <tcp_udp/tcp_server.hpp>
#include <tcp_udp/udp_server.hpp>
#endif

#include "test_util.h"

#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

using namespace std::chrono_literals;

#if defined(_WIN32) || (defined(__linux__) && defined(CORO_HAS_URING) && CORO_HAS_URING)

namespace {

    // ---- 共享的测试客户端动作: 连上服务器, 可选发送一行, 可选等待回包 ----
    coro::Task<int> client_round_trip(unsigned short port, const char* payload) {
        auto conn = co_await coro::net::TcpStream::connect("127.0.0.1", port);
        if (!conn.valid())
            co_return -1;
        if (payload != nullptr) {
            const std::string message(payload);
            (void)co_await conn.write(message.data(), message.size());
        }
        char buf[64];
        int n = co_await conn.read(buf, sizeof(buf));
        co_return n;
    }

    coro::Task<> echo_handler(coro::net::TcpStream conn) {
        char buf[64];
        int n = co_await conn.read(buf, sizeof(buf));
        if (n > 0)
            (void)co_await conn.write(buf, static_cast<size_t>(n));
        co_return;
    }

    // 挂起在 read 上的 handler: 用来验证"卡住的在途工作"能否被取消唤醒
    coro::Task<> blocking_read_handler(coro::net::TcpStream conn) {
        char buf[64];
        (void)co_await conn.read(buf, sizeof(buf)); // 客户端不发数据 -> 一直挂起
        co_return;
    }

    coro::Task<> failing_handler(coro::net::TcpStream conn) {
        (void)conn;
        throw std::runtime_error("handler boom");
        co_return;
    }

    coro::Task<> slow_then_finish_handler(coro::net::TcpStream conn, std::atomic<int>* finished, int ms) {
        (void)conn;
        co_await coro::sleep(std::chrono::milliseconds(ms));
        finished->fetch_add(1);
        co_return;
    }

    // 只建立连接后立即结束: 用来触发 accept 与 handler 派生
    coro::Task<> connect_only(unsigned short port, bool* ok) {
        auto conn = co_await coro::net::TcpStream::connect("127.0.0.1", port);
        *ok = conn.valid();
        co_return;
    }

    // 建立连接、发送数据并等待回包
    coro::Task<> echo_client(unsigned short port, std::atomic<int>* replies) {
        auto conn = co_await coro::net::TcpStream::connect("127.0.0.1", port);
        if (!conn.valid())
            co_return;
        const char msg[5] = {'h', 'e', 'l', 'l', 'o'};
        (void)co_await conn.write(msg, sizeof(msg));
        char buf[16];
        if (co_await conn.read(buf, sizeof(buf)) == static_cast<int>(sizeof(msg)))
            replies->fetch_add(1);
        co_return;
    }

    // 场景协程: 空载关停
    coro::Task<> empty_shutdown_scenario(coro::TcpServer* server, bool* started, bool* drained) {
        *started = server->start("127.0.0.1", 0);
        if (!*started)
            co_return;
        auto report = co_await server->shutdown(std::chrono::milliseconds(50), std::chrono::milliseconds(50));
        *drained = report.drained;
        co_return;
    }

    // 场景协程: 有一个在途 handler 时优雅排空 (宽限期足够 -> 不该被取消)
    coro::Task<> drain_in_flight_scenario(coro::TcpServer* server, std::atomic<int>* finished, bool* port_resolved) {
        if (!server->start("127.0.0.1", 0))
            co_return;
        *port_resolved = server->port() != 0; // 端口 0 必须回查为实际值
        bool ok = false;
        co_await connect_only(server->port(), &ok);
        auto report = co_await server->shutdown(std::chrono::milliseconds(300), std::chrono::milliseconds(300));
        EXPECT_TRUE(report.drained);
        co_return;
    }

    // 场景协程: handler 卡在真实 read 上, 宽限期到点必须能被取消唤醒 (不挂死)
    coro::Task<> cancel_stuck_handler_scenario(coro::TcpServer* server, bool* stopped) {
        if (!server->start("127.0.0.1", 0))
            co_return;
        bool ok = false;
        co_await connect_only(server->port(), &ok);
        const auto begin = std::chrono::steady_clock::now();
        auto report = co_await server->shutdown(std::chrono::milliseconds(30), std::chrono::milliseconds(500));
        const auto cost =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - begin);
        *stopped = report.drained && server->current_phase() == coro::TcpServer::phase::stopped;
        // 取消应当生效, 不该等满 cancel_grace; 也不该远超 (挂死)
        EXPECT_LT(cost.count(), 400) << "挂起在 read 上的 handler 未被及时唤醒, 耗时 " << cost.count() << "ms";
        co_return;
    }

    // 场景协程: handler 抛异常后, 在途计数必须归零且错误回调被触发
    coro::Task<> handler_exception_scenario(coro::TcpServer* server, std::atomic<int>* errors) {
        if (!server->start("127.0.0.1", 0))
            co_return;
        bool ok = false;
        co_await connect_only(server->port(), &ok);
        auto report = co_await server->shutdown(std::chrono::milliseconds(200), std::chrono::milliseconds(200));
        EXPECT_TRUE(report.drained);
        EXPECT_EQ(server->connection_count(), 0u) << "handler 抛异常后在途计数未归还";
        EXPECT_GT(errors->load(), 0);
        co_return;
    }

    // 场景协程: 两个服务共用同一 loop, 关一个不能影响另一个
    coro::Task<> two_servers_share_loop(coro::TcpServer* a, coro::TcpServer* b, std::atomic<int>* replies_b) {
        if (!a->start("127.0.0.1", 0) || !b->start("127.0.0.1", 0))
            co_return;
        a->stop(); // 旧实现会顺手停掉全局 EventLoop, 使 b 再也收不到包
        bool ok = false;
        co_await connect_only(b->port(), &ok);
        co_await echo_client(b->port(), replies_b);
        b->stop(); // 收尾: 不留挂起的 accept, 否则事件循环不会返回
        co_await coro::sleep(std::chrono::milliseconds(30));
        co_return;
    }

    // 场景协程: Server 对象先于滞留 handler 析构, handler 仍须安全跑完
    coro::Task<> server_dies_before_handler_scenario(std::atomic<int>* finished) {
        std::unique_ptr<coro::TcpServer> owned = std::make_unique<coro::TcpServer>();
        coro::TcpServer* raw = owned.get();
        raw->set_handler(
            [finished](coro::net::TcpStream conn) { return slow_then_finish_handler(std::move(conn), finished, 60); });
        if (!raw->start("127.0.0.1", 0))
            co_return;
        bool ok = false;
        co_await connect_only(raw->port(), &ok);
        owned.reset(); // 服务器对象销毁, 在途 handler 仍持有 shared state
        co_await coro::sleep(std::chrono::milliseconds(150));
        EXPECT_GE(finished->load(), 1);
        co_return;
    }

    // 场景协程: 并发上限生效, 超限连接被拒绝并计数
    coro::Task<> capacity_limit_scenario(coro::TcpServer* server, int* rejected, int* in_flight) {
        if (!server->start("127.0.0.1", 0))
            co_return;
        // 连接保活到断言之后再释放: 客户端一旦析构, 服务端 read 收到 EOF 立即结束,
        // 就测不出"在途 handler 占满容量"这件事。
        std::vector<coro::net::TcpStream> keepalive;
        for (int i = 0; i < 4; ++i) {
            auto conn = co_await coro::net::TcpStream::connect("127.0.0.1", server->port());
            if (conn.valid())
                keepalive.push_back(std::move(conn));
            co_await coro::yield();
        }
        co_await coro::sleep(std::chrono::milliseconds(80));
        // 断言取的是"关停前"的快照: 先记录拒绝数, 再走收尾
        *rejected = static_cast<int>(server->rejected_connections());
        *in_flight = static_cast<int>(server->connection_count());
        (void)co_await server->shutdown(std::chrono::milliseconds(30), std::chrono::milliseconds(300));
        co_return;
    }

    coro::Task<> udp_count_handler(std::atomic<int>* seen) {
        seen->fetch_add(1);
        co_return;
    }

    // 场景协程: UDP 零长度数据报与满载丢弃
    coro::Task<> udp_scenario(coro::UdpServer* server, std::atomic<int>* seen, int* zero_seen) {
        if (!server->start("127.0.0.1", 0))
            co_return;
        coro::net::UdpSocket peer;
        if (!peer.bind_listen("127.0.0.1", 0))
            co_return;
        sockaddr_in target{};
        target.sin_family = AF_INET;
        target.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        target.sin_port = htons(server->port());

        const char one = 'x';
        (void)co_await peer.sendto(&one, 1, target); // 1 字节
        (void)co_await peer.sendto(&one, 0, target); // 合法零长度数据报 (有效指针 + 长度 0)
        co_await coro::sleep(std::chrono::milliseconds(60));
        *zero_seen = static_cast<int>(*seen);
        // 必须收尾: 否则 recv_loop 仍挂起在 recvfrom 上, 事件循环不会返回
        (void)co_await server->shutdown(std::chrono::milliseconds(30), std::chrono::milliseconds(300));
        co_return;
    }

} // namespace

// ---- 空载关停: 立即完成, 且重复关闭幂等 ----
TEST(ServerLifecycleTest, ShutdownWithNoTrafficIsImmediateAndIdempotent) {
    coro::TcpServer server;
    server.set_handler([](coro::net::TcpStream conn) { return echo_handler(std::move(conn)); });
    bool started = false, drained = false;
    test_util::run_task([&] { return empty_shutdown_scenario(&server, &started, &drained); });
    EXPECT_TRUE(started);
    EXPECT_TRUE(drained);
    EXPECT_EQ(server.current_phase(), coro::TcpServer::phase::stopped);
    server.stop();
    server.stop(); // 重复调用不得再次触发任何东西
    EXPECT_EQ(server.current_phase(), coro::TcpServer::phase::stopped);
}

// ---- 宽限期内的在途 handler 应自然跑完, 不被取消; 端口 0 必须回查 ----
TEST(ServerLifecycleTest, GraceWindowLetsInFlightHandlersFinish) {
    coro::TcpServer server;
    std::atomic<int> finished{0};
    server.set_handler(
        [&finished](coro::net::TcpStream conn) { return slow_then_finish_handler(std::move(conn), &finished, 40); });
    bool port_resolved = false;
    test_util::run_task([&] { return drain_in_flight_scenario(&server, &finished, &port_resolved); });
    EXPECT_TRUE(port_resolved) << "端口 0 未回查成系统实际分配的端口";
    EXPECT_EQ(finished.load(), 1) << "宽限期足够的在途 handler 不该被取消";
    EXPECT_EQ(server.connection_count(), 0u);
}

// ---- handler 卡在真实 read 上: 到点必须能被取消唤醒, 不能挂死 ----
TEST(ServerLifecycleTest, StuckHandlerIsCancelledAtGraceDeadline) {
    coro::TcpServer server;
    server.set_handler([](coro::net::TcpStream conn) { return blocking_read_handler(std::move(conn)); });
    bool stopped = false;
    test_util::run_task([&] { return cancel_stuck_handler_scenario(&server, &stopped); });
    EXPECT_TRUE(stopped) << "挂起在 io_uring/IOCP read 上的 handler 未被取消唤醒";
}

// ---- handler 抛异常: 在途计数必须归还, 错误回调必须被调用 ----
TEST(ServerLifecycleTest, HandlerExceptionReturnsCountersAndReports) {
    coro::TcpServer server;
    std::atomic<int> errors{0};
    server.set_error_handler([&errors](const std::string&) { errors.fetch_add(1); });
    server.set_handler([](coro::net::TcpStream conn) { return failing_handler(std::move(conn)); });
    test_util::run_task([&] { return handler_exception_scenario(&server, &errors); });
    EXPECT_GT(errors.load(), 0) << "handler 异常没有转交错误回调";
}

// ---- 两个服务共用同一 loop: 关一个不能误停另一个 (旧 stop() 会停全局循环) ----
TEST(ServerLifecycleTest, StoppingOneServerDoesNotStopAnother) {
    coro::TcpServer a, b;
    std::atomic<int> replies{0};
    a.set_handler([](coro::net::TcpStream conn) { return echo_handler(std::move(conn)); });
    b.set_handler([](coro::net::TcpStream conn) { return echo_handler(std::move(conn)); });
    test_util::run_task([&] { return two_servers_share_loop(&a, &b, &replies); });
    EXPECT_EQ(replies.load(), 1) << "关闭 a 之后 b 必须仍可正常收发 (stop 不该停掉所属事件循环)";
}

// ---- Server 对象可以先于滞留 handler 析构 (handler 只依赖 shared state) ----
TEST(ServerLifecycleTest, ServerMayDieBeforeItsHandlers) {
    std::atomic<int> finished{0};
    test_util::run_task([&] { return server_dies_before_handler_scenario(&finished); });
    EXPECT_GE(finished.load(), 1) << "Server 析构后 handler 协程访问了悬空 this 或未完成";
}

// ---- 并发上限: 超限连接被拒绝并计数, 不影响已受理的 ----
TEST(ServerLifecycleTest, ConcurrencyLimitRejectsExtraConnections) {
    coro::TcpServer server;
    coro::TcpServer::Config cfg;
    cfg.max_concurrent_handlers = 1;
    server.set_config(cfg);
    server.set_handler([](coro::net::TcpStream conn) { return blocking_read_handler(std::move(conn)); });
    int rejected = 0, in_flight = 0;
    test_util::run_task([&] { return capacity_limit_scenario(&server, &rejected, &in_flight); });
    EXPECT_GE(rejected, 1) << "max_concurrent_handlers 未生效";
    EXPECT_GE(in_flight, 1);
}

// ---- UDP: 零长度数据报是合法输入, 不能被静默吞掉 ----
TEST(ServerLifecycleTest, UdpAcceptsZeroLengthDatagram) {
    coro::UdpServer server;
    std::atomic<int> seen{0};
    server.set_handler([&seen](const char*, size_t, const sockaddr_in&) { return udp_count_handler(&seen); });
    int handled = 0;
    test_util::run_task([&] { return udp_scenario(&server, &seen, &handled); });
    EXPECT_GE(server.datagram_count(), 2u) << "零长度数据报被当作无事发生吞掉";
    EXPECT_GE(handled, 1) << "handler 未收到数据报";
    server.stop();
}

#endif // _WIN32 || io_uring
