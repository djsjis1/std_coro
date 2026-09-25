// test_net_advanced.cpp — 网络高级场景: 多连接/大数据/拒绝/多轮读写
#if defined(_WIN32) || (defined(__linux__) && (!defined(CORO_HAS_URING) || CORO_HAS_URING))
#include <gtest/gtest.h>

#include <coro/coro.hpp>
#include <coro/net.hpp>

#include "test_util.h"

#include <atomic>
#include <cstring>
#include <string>
#include <vector>

#ifdef CORO_URING_ENABLED
#include <fcntl.h>
#include <unistd.h>
#endif

using namespace std::chrono_literals;

namespace {

    // ── 多客户端并发连接 ──
    coro::Task<> multi_client_handler(coro::net::TcpStream conn, int id, std::atomic<int>* served) {
        char buf[64];
        int n = co_await conn.read(buf, sizeof(buf));
        if (n > 0) {
            co_await conn.write(buf, (size_t)n); // echo back
        }
        ++*served;
    }

    coro::Task<> multi_client_server(coro::net::TcpListener* listener, int expected, std::atomic<int>* served) {
        for (int i = 0; i < expected; ++i) {
            auto conn = co_await listener->accept();
            if (conn.valid()) {
                auto handler = coro::spawn(multi_client_handler(std::move(conn), i, served));
                handler.detach();
            }
        }
    }

    coro::Task<> multi_client_worker(int id, int* ok) {
        auto conn = co_await coro::net::TcpStream::connect("127.0.0.1", 19001);
        if (!conn.valid())
            co_return;

        char msg[32];
        int len = snprintf(msg, sizeof(msg), "client-%d", id);
        int sent = co_await conn.write(msg, (size_t)len);

        char buf[64];
        int n = co_await conn.read(buf, sizeof(buf));
        if (n == len && memcmp(buf, msg, (size_t)len) == 0)
            *ok = 1;
        conn.close();
    }

    coro::Task<> multi_client_scenario(int n_clients, std::vector<int>* results, std::atomic<int>* served) {
        coro::net::TcpListener listener;
        if (!listener.bind_listen("127.0.0.1", 19001))
            co_return;

        auto server = coro::spawn(multi_client_server(&listener, n_clients, served));

        // 预分配: 防止 push_back 重新分配导致之前 &results->back() 指针失效
        results->reserve(n_clients);
        results->clear();

        std::vector<coro::Task<>> clients;
        for (int i = 0; i < n_clients; ++i) {
            results->push_back(0);
            clients.push_back(multi_client_worker(i, &results->back()));
        }
        for (auto& c : clients)
            c.start();
        for (auto& c : clients)
            co_await std::move(c);

        co_await coro::sleep(100ms); // 等 server 处理完
        co_await std::move(server);
    }

    // ── 大数据传输: 发送/接收大块数据 ──
    coro::Task<> large_data_sender(coro::net::TcpStream conn, const std::string* data, bool* sent_ok) {
        size_t total = 0;
        while (total < data->size()) {
            int n = co_await conn.write(data->data() + total, data->size() - total);
            if (n <= 0)
                break;
            total += (size_t)n;
        }
        *sent_ok = (total == data->size());
        conn.close();
    }

    coro::Task<> large_data_receiver(coro::net::TcpListener* listener, std::string* received, bool* recv_ok) {
        auto conn = co_await listener->accept();
        if (!conn.valid())
            co_return;

        char buf[4096];
        while (true) {
            int n = co_await conn.read(buf, sizeof(buf));
            if (n <= 0)
                break;
            received->append(buf, (size_t)n);
        }
        *recv_ok = true;
    }

    coro::Task<> large_data_scenario(size_t data_size, bool* sent_ok, bool* recv_ok, std::string* received) {
        coro::net::TcpListener listener;
        if (!listener.bind_listen("127.0.0.1", 19002))
            co_return;

        std::string data(data_size, 'X');
        for (size_t i = 0; i < data_size; ++i)
            data[i] = static_cast<char>('A' + (i % 26));

        auto receiver = coro::spawn(large_data_receiver(&listener, received, recv_ok));
        co_await coro::sleep(10ms); // 等 accept 挂起

        auto conn = co_await coro::net::TcpStream::connect("127.0.0.1", 19002);
        if (!conn.valid())
            co_return;

        size_t total = 0;
        while (total < data.size()) {
            int n = co_await conn.write(data.data() + total, data.size() - total);
            if (n <= 0)
                break;
            total += (size_t)n;
        }
        *sent_ok = (total == data.size());
        conn.close();

        co_await std::move(receiver);
    }

    // ── 连接拒绝: 连接不存在的端口 ──
    coro::Task<> connect_refused(bool* got_invalid) {
        auto conn = co_await coro::net::TcpStream::connect("127.0.0.1", 19999);
        *got_invalid = !conn.valid();
    }

    // ── 多轮读写: 同一连接多次 request/response ──
    coro::Task<> multi_round_handler(coro::net::TcpStream conn, int rounds, int* received_rounds) {
        char buf[64];
        for (int i = 0; i < rounds; ++i) {
            int n = co_await conn.read(buf, sizeof(buf));
            if (n <= 0)
                break;
            ++*received_rounds;
            co_await conn.write(buf, (size_t)n);
        }
    }

    coro::Task<> multi_round_server(coro::net::TcpListener* listener, int rounds, int* received_rounds) {
        auto conn = co_await listener->accept();
        if (!conn.valid())
            co_return;
        co_await multi_round_handler(std::move(conn), rounds, received_rounds);
    }

    coro::Task<> multi_round_client(int rounds, int* sent_rounds) {
        auto conn = co_await coro::net::TcpStream::connect("127.0.0.1", 19003);
        if (!conn.valid())
            co_return;

        for (int i = 0; i < rounds; ++i) {
            char msg[32];
            int len = snprintf(msg, sizeof(msg), "round-%d", i);
            int sent = co_await conn.write(msg, (size_t)len);
            if (sent <= 0)
                break;
            ++*sent_rounds;

            char buf[64];
            int n = co_await conn.read(buf, sizeof(buf));
            if (n <= 0)
                break;
        }
        conn.close();
    }

    coro::Task<> multi_round_scenario(int rounds, int* sent, int* received) {
        coro::net::TcpListener listener;
        if (!listener.bind_listen("127.0.0.1", 19003))
            co_return;

        auto server = coro::spawn(multi_round_server(&listener, rounds, received));
        auto client = coro::spawn(multi_round_client(rounds, sent));
        co_await std::move(server);
        co_await std::move(client);
    }

    // ── 快速连接/断开循环: 检测资源泄漏 ──
    // acceptor 用命名函数协程 (临时 lambda 闭包销毁后 use-after-return)
    coro::Task<> quick_accept(coro::net::TcpListener* listener) {
        auto conn = co_await listener->accept();
        // 立即关闭 (conn 析构)
    }

    coro::Task<> rapid_connect_loop(int iterations, int* success_count) {
        for (int i = 0; i < iterations; ++i) {
            coro::net::TcpListener listener;
            if (!listener.bind_listen("127.0.0.1", 19010 + (i % 10)))
                continue;

            auto acceptor = coro::spawn(quick_accept(&listener));

            co_await coro::sleep(5ms);
            auto conn = co_await coro::net::TcpStream::connect("127.0.0.1", 19010 + (i % 10));
            if (conn.valid()) {
                ++*success_count;
                conn.close();
            }
            co_await std::move(acceptor);
        }
    }

} // namespace

// ── 多客户端并发连接 ──
TEST(NetAdvancedTest, MultipleConcurrentClients) {
    constexpr int N = 5;
    std::vector<int> results;
    std::atomic<int> served{0};
    test_util::run_task([&] { return multi_client_scenario(N, &results, &served); });

    ASSERT_EQ(results.size(), (size_t)N);
    int ok_count = 0;
    for (int r : results)
        if (r)
            ++ok_count;
    EXPECT_GE(ok_count, N - 1); // 至少 N-1 个成功
    EXPECT_EQ(served.load(), N);
}

// ── 大数据传输 ──
TEST(NetAdvancedTest, LargeDataTransfer) {
    bool sent_ok = false, recv_ok = false;
    std::string received;
    test_util::run_task([&] { return large_data_scenario(100000, &sent_ok, &recv_ok, &received); });
    EXPECT_TRUE(sent_ok);
    EXPECT_TRUE(recv_ok);
    EXPECT_EQ(received.size(), 100000u);
}

// ── 连接拒绝 ──
TEST(NetAdvancedTest, ConnectionRefused) {
    bool got_invalid = false;
    test_util::run_task([&] { return connect_refused(&got_invalid); });
    EXPECT_TRUE(got_invalid);
}

// ── 多轮读写 ──
TEST(NetAdvancedTest, MultiRoundReadWrite) {
    int sent = 0, received = 0;
    test_util::run_task([&] { return multi_round_scenario(10, &sent, &received); });
    EXPECT_EQ(sent, 10);
    EXPECT_EQ(received, 10);
}

// ── 快速连接/断开循环 ──
TEST(NetAdvancedTest, RapidConnectDisconnect) {
    int success = 0;
    test_util::run_task([&] { return rapid_connect_loop(20, &success); });
    EXPECT_GE(success, 15); // 至少 15 次成功 (允许少量端口冲突)
}
#ifdef CORO_URING_ENABLED
namespace {
    struct submit_fault_awaiter {
        coro::net::UringEventSource* source;
        int fd;
        bool* sanitized;
        coro::detail::uring_op op;
        char byte = 'X';

        ~submit_fault_awaiter() { source->untrack_op(&op); }
        bool await_ready() const noexcept { return false; }
        void await_suspend(std::coroutine_handle<> h) {
            op.continuation = h;
            auto* ring = source->handle();
            auto* sqe = io_uring_get_sqe(ring);
            if (!sqe) {
                op.result = -ENOBUFS;
                coro::EventLoop::get().schedule(h);
                return;
            }
            source->track_op(&op);
            if (fd >= 0) {
                io_uring_prep_write(sqe, fd, &byte, 1, -1);
                // 保留真实 SQ 映射，仅让 enter 失败：liburing 仍会发布 SQ tail。
                const int ring_fd = std::exchange(ring->ring_fd, -1);
                const int enter_fd = std::exchange(ring->enter_ring_fd, -1);
                coro::detail::uring_submit(source, sqe, &op);
                ring->ring_fd = ring_fd;
                ring->enter_ring_fd = enter_fd;
                *sanitized = sqe->opcode == IORING_OP_NOP && sqe->user_data == 0 && sqe->addr == 0;
            } else {
                io_uring_prep_nop(sqe);
                coro::detail::uring_submit(source, sqe, &op);
            }
        }
        int await_resume() const { return op.result; }
    };

    coro::Task<int> submit_with_fault(coro::net::UringEventSource* source, int fd, bool* sanitized) {
        submit_fault_awaiter aw{source, fd, sanitized, {}};
        co_return co_await aw;
    }
} // namespace

TEST(UringFailureTest, FailedSubmitCannotUseFreedBufferOnLaterSubmit) {
    auto& loop = coro::EventLoop::get();
    auto* source = loop.uring();
    if (!source)
        GTEST_SKIP() << "当前内核不支持 io_uring";
    int fds[2];
    ASSERT_EQ(::pipe2(fds, O_NONBLOCK | O_CLOEXEC), 0);
    bool sanitized = false;
    // 连续失败次数超过 ring 深度，仍须回收尾槽并允许下一次正常提交。
    for (int round = 0; round < 512; ++round) {
        auto failed = submit_with_fault(source, fds[1], &sanitized);
        failed.start();
        loop.run();
        EXPECT_EQ(failed.take_result(), -EBADF);
        EXPECT_TRUE(sanitized);
        EXPECT_FALSE(source->has_pending());
        EXPECT_EQ(io_uring_sq_ready(source->handle()), 0u);
    }
    // 上面的帧已经销毁；后续 submit 会刷新此前遗留的 SQE。
    auto success = submit_with_fault(source, -1, &sanitized);
    success.start();
    loop.run();
    EXPECT_EQ(success.take_result(), 0);
    EXPECT_FALSE(source->has_pending());
    char byte = 0;
    EXPECT_EQ(::read(fds[0], &byte, 1), -1);
    EXPECT_EQ(errno, EAGAIN);
    ::close(fds[0]);
    ::close(fds[1]);
}
#endif
#endif // _WIN32 || __linux__
