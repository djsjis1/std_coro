// test_unix.cpp — coro::net Unix Domain Socket 的收发与关停语义 (计划 M4c)
//
// 关键覆盖: 抽象名与文件路径两种绑定、端到端读写、EOF 语义, 以及最重要的
// "关闭监听器时挂起的 accept 必须被唤醒" —— 这是本项目在 TcpListener/UdpSocket 上
// 踩过的坑 (只 close 不 shutdown 会让事件循环永久等待)。
#if defined(__linux__) && defined(CORO_HAS_URING) && CORO_HAS_URING

#include <gtest/gtest.h>

#include <coro/coro.hpp>
#include <coro/unix.hpp>

#include "test_util.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <string>
#include <unistd.h>

using namespace std::chrono_literals;

namespace {

    std::string tmp_sock_path(const char* tag) {
        std::string p = "/tmp/coro_unix_";
        p += tag;
        p += std::to_string(::getpid());
        p += ".sock";
        return p;
    }

    /// 服务端: 接受一次连接并回写一行
    coro::Task<> echo_once(coro::net::UnixListener* listener, std::atomic<int>* accepted) {
        auto conn = co_await listener->accept();
        if (!conn.valid())
            co_return;
        accepted->fetch_add(1);
        const char msg[] = "pong\n";
        (void)co_await conn.write(msg, sizeof(msg) - 1);
        co_return;
    }

    /// 客户端: 连上后读一行
    coro::Task<> read_line(coro::net::UnixStream* peer, std::string* out, int* read_rc) {
        char buf[32] = {0};
        const int n = co_await peer->read(buf, sizeof(buf) - 1);
        *read_rc = n;
        if (n > 0)
            *out = std::string(buf, static_cast<size_t>(n));
        co_return;
    }

    coro::Task<> round_trip(const std::string& path, std::string* got, int* accepted_count, int* read_rc) {
        coro::net::UnixListener listener;
        if (!listener.bind(path))
            co_return;
        std::atomic<int> accepted{0};
        auto svc = coro::spawn(echo_once(&listener, &accepted));

        auto peer = co_await coro::net::UnixStream::connect(path);
        if (!peer.valid()) {
            *read_rc = -999;
            co_await std::move(svc);
            co_return;
        }
        const char msg[] = "ping";
        (void)co_await peer.write(msg, sizeof(msg) - 1);

        std::string line;
        int rc = 0;
        co_await read_line(&peer, &line, &rc);
        *got = line;
        *read_rc = rc;
        co_await std::move(svc);
        *accepted_count = accepted.load();
        // 不做 shutdown 收尾的话, 挂起的 accept 会永久等待, 事件循环不返回
        listener.close();
        co_return;
    }

    /// 挂起的 accept 必须被 listener.close() 唤醒 (shutdown + close 两步的意义所在)
    coro::Task<> close_wakes_pending_accept(bool* woke_up, int* accepted) {
        coro::net::UnixListener listener;
        if (!listener.bind(tmp_sock_path("wake")))
            co_return;
        std::atomic<int> count{0};
        auto svc = coro::spawn(echo_once(&listener, &count));
        co_await coro::sleep(20ms); // 让 accept 确实挂起
        listener.close();           // 必须唤醒它, 否则 svc 永不结束
        co_await std::move(svc);
        *woke_up = true;
        *accepted = count.load();
        co_return;
    }

    /// socketpair 版: 无路径、无监听, 验证同一套 awaiter 在配对端点上工作
    coro::Task<> pair_round_trip(std::string* got) {
        coro::net::UnixStream a, b;
        if (!coro::net::unix_pair(a, b))
            co_return;
        auto writer = coro::spawn([](coro::net::UnixStream* s) -> coro::Task<> {
            const char msg[] = "hi-from-pair";
            (void)co_await s->write(msg, sizeof(msg) - 1);
        }(&a));
        char buf[32] = {0};
        const int n = co_await b.read(buf, sizeof(buf) - 1);
        if (n > 0)
            *got = std::string(buf, static_cast<size_t>(n));
        co_await std::move(writer);
        co_return;
    }

    /// 对端关闭后读到 0 (EOF), 不能是错误或挂死
    coro::Task<> peer_close_reads_eof(int* rc) {
        coro::net::UnixStream a, b;
        if (!coro::net::unix_pair(a, b))
            co_return;
        auto closer = coro::spawn([](coro::net::UnixStream* s) -> coro::Task<> {
            co_await coro::sleep(10ms);
            s->close();
        }(&a));
        char buf[8] = {0};
        *rc = co_await b.read(buf, sizeof(buf));
        co_await std::move(closer);
        co_return;
    }

} // namespace

TEST(UnixTest, PathRoundTripWithAcceptAndWrite) {
    const std::string path = tmp_sock_path("rt");
    std::string got;
    int accepted = 0, read_rc = 0;
    test_util::run_task([&] { return round_trip(path, &got, &accepted, &read_rc); });
    EXPECT_EQ(read_rc, 5) << "应读到 5 字节 \"pong\\n\"";
    EXPECT_EQ(got, "pong\n");
    EXPECT_EQ(accepted, 1);
    ::unlink(path.c_str());
}

TEST(UnixTest, AbstractNameRoundTrip) {
    std::string got;
    int accepted = 0, read_rc = 0;
    test_util::run_task([&] { return round_trip("@coro-unix-abstract", &got, &accepted, &read_rc); });
    EXPECT_EQ(got, "pong\n") << "抽象名绑定/连接未走通";
    EXPECT_EQ(accepted, 1);
}

TEST(UnixTest, CloseWakesPendingAccept) {
    bool woke_up = false;
    int accepted = 0;
    test_util::run_task([&] { return close_wakes_pending_accept(&woke_up, &accepted); });
    EXPECT_TRUE(woke_up) << "listener.close() 没能唤醒挂起的 accept (必须先 shutdown 再 close)";
    EXPECT_EQ(accepted, 0) << "被取消的 accept 不该报告成功";
    ::unlink(tmp_sock_path("wake").c_str());
}

TEST(UnixTest, SocketPairDeliversData) {
    std::string got;
    test_util::run_task([&] { return pair_round_trip(&got); });
    EXPECT_EQ(got, "hi-from-pair");
}

TEST(UnixTest, PeerCloseYieldsEofZero) {
    int rc = -1;
    test_util::run_task([&] { return peer_close_reads_eof(&rc); });
    EXPECT_EQ(rc, 0) << "对端关闭应读到 0 (EOF), 而不是 -1 或挂死";
}

TEST(UnixTest, OverlongPathFailsWithoutTruncation) {
    // 静默截断会连到别的路径, 比报错更难查: 必须直接判失败
    coro::net::UnixListener listener;
    const std::string too_long(200, 'a');
    EXPECT_FALSE(listener.bind("/tmp/" + too_long));
    EXPECT_FALSE(listener.valid());
}

#endif // __linux__ && CORO_HAS_URING
