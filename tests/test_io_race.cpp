// test_io_race.cpp — I/O 完成通知与取消/帧销毁竞速的生命周期回归
//
// 与 LifecycleTest 的分工: 那边验证"销毁 Task 时能取消挂起的 IO";
// 这边验证更难的两件事:
//   1. 完成通知与取消同时到达时, 协程恰好结束一次 (不 double-resume, 也不永久挂起);
//   2. 内核仍在引用缓冲/协程状态时, 帧销毁不得让后续完成通知写入已释放内存;
//   3. 已取消定时器条目到期后, 不得唤醒地址已被复用的帧 (代次校验)。
#include <gtest/gtest.h>

#include <coro/coro.hpp>
#if defined(_WIN32) || (defined(__linux__) && defined(CORO_HAS_URING) && CORO_HAS_URING)
#include <coro/net.hpp>
#endif

#include "test_util.h"

#include <atomic>
#include <chrono>
#include <memory>

using namespace std::chrono_literals;

namespace {

#if defined(_WIN32) || (defined(__linux__) && defined(CORO_HAS_URING) && CORO_HAS_URING)

    unsigned short bind_free_port(coro::net::TcpListener* listener) {
        for (unsigned short port = 19300; port < 19320; ++port) {
            if (listener->bind_listen("127.0.0.1", port))
                return port;
        }
        return 0;
    }

    // accept 结果交给外部槽位, 便于主协程把服务端连接分派给 worker。
    coro::Task<> accept_into(coro::net::TcpListener* listener, coro::net::TcpStream* slot, bool* ok) {
        *slot = co_await listener->accept();
        *ok = slot->valid();
        co_return;
    }

    // 挂起在 read 上: 无论被完成通知唤醒还是被取消唤醒, 只登记一次结束。
    coro::Task<> read_once(coro::net::TcpStream* conn, std::atomic<int>* endings, int* bytes) {
        char buf[16];
        try {
            *bytes = co_await conn->read(buf, sizeof(buf));
        } catch (const coro::CancelledError&) {
        }
        endings->fetch_add(1);
        co_return;
    }

    coro::Task<> write_ping(coro::net::TcpStream* conn) {
        const char msg[4] = {'p', 'i', 'n', 'g'};
        (void)co_await conn->write(msg, sizeof(msg));
        co_return;
    }

    // 场景 1: 完成通知与取消竞速。cancel_first 控制两者到达顺序, 覆盖两种交错。
    coro::Task<> completion_versus_cancel(std::atomic<int>* endings, bool* connected, bool cancel_first) {
        coro::net::TcpListener listener;
        unsigned short port = bind_free_port(&listener);
        if (port == 0)
            co_return;

        coro::net::TcpStream client = co_await coro::net::TcpStream::connect("127.0.0.1", port);
        if (!client.valid())
            co_return;
        *connected = true;

        coro::net::TcpStream server;
        bool accepted = false;
        co_await accept_into(&listener, &server, &accepted);
        if (!server.valid())
            co_return;

        int bytes = -1;
        // reader 由主协程帧保活, 直到 co_await 结束才销毁。
        auto reader = std::make_shared<coro::Task<>>(coro::spawn(read_once(&server, endings, &bytes)));
        co_await coro::yield(); // 先让 reader 真正挂起在 read 上

        auto writer = coro::spawn(write_ping(&client));
        if (cancel_first)
            reader->cancel();
        co_await std::move(writer);
        if (!cancel_first)
            reader->cancel();
        co_await *reader;
        co_return;
    }

    // 帧销毁即计数: cancel() 只置位并投递唤醒, 紧随其后的 Task 析构会放弃协程体,
    // 所以判定依据只能是帧的局部对象是否被正确销毁, 不能指望代码跑到最后一行。
    struct FrameTracker {
        std::atomic<int>* count;
        explicit FrameTracker(std::atomic<int>* c) : count(c) {}
        ~FrameTracker() { count->fetch_add(1); }
    };

    coro::Task<> read_until_frame_destroyed(coro::net::TcpStream* conn, std::atomic<int>* frames) {
        FrameTracker tracker(frames);
        char buf[16];
        try {
            (void)co_await conn->read(buf, sizeof(buf));
        } catch (const coro::CancelledError&) {
        }
        co_return;
    }

    // 场景 2: 内核仍持有缓冲时反复销毁帧; 之后该环必须仍能正常完成一次读。
    coro::Task<> destroy_frames_under_pending_io(std::atomic<int>* frames, bool* io_still_works, int rounds) {
        coro::net::TcpListener listener;
        unsigned short port = bind_free_port(&listener);
        if (port == 0)
            co_return;
        for (int i = 0; i < rounds; ++i) {
            coro::net::TcpStream client = co_await coro::net::TcpStream::connect("127.0.0.1", port);
            if (!client.valid())
                co_return;
            coro::net::TcpStream server;
            bool accepted = false;
            co_await accept_into(&listener, &server, &accepted);
            if (!server.valid())
                co_return;
            {
                auto reader = std::make_shared<coro::Task<>>(coro::spawn(read_until_frame_destroyed(&server, frames)));
                co_await coro::yield();
                reader->cancel();
            } // 帧在此销毁, 完成通知可能还在路上
            co_await coro::sleep(2ms);
            server.close();
            client.close();
        }

        // 活性: 迟到的通知不得污染 SQ/op 状态, 正常读仍要拿到完整数据。
        coro::net::TcpStream client = co_await coro::net::TcpStream::connect("127.0.0.1", port);
        if (!client.valid())
            co_return;
        coro::net::TcpStream server;
        bool accepted = false;
        co_await accept_into(&listener, &server, &accepted);
        if (!server.valid())
            co_return;
        auto writer = coro::spawn(write_ping(&client));
        char buf[16];
        *io_still_works = (co_await server.read(buf, sizeof(buf))) == 4;
        co_await std::move(writer);
        co_return;
    }
#endif

    coro::Task<> sleep_then_mark(std::atomic<int>* wakes) {
        co_await coro::sleep(80ms);
        wakes->fetch_add(1);
        co_return;
    }

    // 场景 3: 取消并销毁挂在定时器上的帧, 随后用大量同尺寸帧诱导地址复用;
    // 原到期时刻到来时, 遗留条目不得唤醒任何无效句柄。
    coro::Task<> expired_timer_after_reuse(std::atomic<int>* wakes) {
        for (int round = 0; round < 30; ++round) {
            {
                auto t = std::make_shared<coro::Task<>>(coro::spawn(sleep_then_mark(wakes)));
                co_await coro::yield();
                t->cancel();
            }
            for (int i = 0; i < 20; ++i)
                co_await coro::sleep(1ms);
        }
        co_await coro::sleep(150ms); // 覆盖所有被取消定时器的原到期时刻
        co_return;
    }

} // namespace

#if defined(_WIN32) || (defined(__linux__) && defined(CORO_HAS_URING) && CORO_HAS_URING)

TEST(IORaceTest, CompletionAndCancelResumeTheTaskExactlyOnce) {
    for (bool cancel_first : {true, false}) {
        std::atomic<int> endings{0};
        bool connected = false;
        test_util::run_task([&] { return completion_versus_cancel(&endings, &connected, cancel_first); });
        ASSERT_TRUE(connected) << "无法监听 19300-19319, 环境不允许建连";
        EXPECT_EQ(endings.load(), 1) << "cancel_first=" << (cancel_first ? "true" : "false");
    }
}

TEST(IORaceTest, FramesDestroyedWhileKernelHoldsBufferAreSafe) {
    std::atomic<int> frames{0};
    bool io_still_works = false;
    test_util::run_task([&] { return destroy_frames_under_pending_io(&frames, &io_still_works, 60); });
    EXPECT_EQ(frames.load(), 60);
    EXPECT_TRUE(io_still_works) << "反复在挂起 read 上销毁帧后, 后续 I/O 失去进展";
}

#endif

TEST(TimerRaceTest, CancelledTimerDoesNotWakeReusedFrameAddress) {
    std::atomic<int> wakes{0};
    test_util::run_task([&] { return expired_timer_after_reuse(&wakes); });
    EXPECT_EQ(wakes.load(), 0);
}
