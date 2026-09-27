// test_channel.cpp — coro::channel<T> 的容量/交接/关闭/取消合同
#include <gtest/gtest.h>

#include <coro/channel.hpp>

#include "test_util.h"

#include <atomic>
#include <memory>
#include <optional>
#include <vector>

#if defined(CORO_HAS_CONCURRENCY_EXT) && CORO_HAS_CONCURRENCY_EXT

using namespace std::chrono_literals;

namespace {

    // 发送一个值的后台任务: 记录是否因关闭而失败
    coro::Task<> send_and_record(coro::channel<int>::sender* tx, int value, std::atomic<int>* ok,
                                 std::atomic<int>* closed) {
        try {
            co_await tx->send(value);
            ok->fetch_add(1);
        } catch (const coro::ClosedChannelError&) {
            closed->fetch_add(1);
        }
        co_return;
    }

    // 1) 有界容量: 未装满时发送不阻塞, 接收按 FIFO 取
    coro::Task<> bounded_fifo(std::vector<int>* out) {
        auto ch = coro::channel<int>::bounded(4);
        auto tx = ch.make_sender();
        auto rx = ch.make_receiver();
        for (int i = 0; i < 3; ++i) {
            EXPECT_TRUE(tx.try_send(i));
        }
        EXPECT_EQ(ch.size(), 3u);
        int got = -1;
        while (rx.try_recv(got)) {
            out->push_back(got);
            got = -1;
        }
        co_return;
    }

    // 2) 满时发送挂起, 直到接收方腾出位置
    coro::Task<> send_blocks_when_full(int* sent_before_drain, int* sent_after) {
        auto ch = coro::channel<int>::bounded(1);
        auto tx = ch.make_sender();
        auto rx = ch.make_receiver();
        std::atomic<int> ok{0}, closed{0};

        EXPECT_TRUE(tx.try_send(1)); // 占满
        auto blocker = coro::spawn(send_and_record(&tx, 2, &ok, &closed));
        co_await coro::yield();
        *sent_before_drain = ok.load(); // 仍挂着, 没算完成

        int got = 0;
        EXPECT_TRUE(rx.try_recv(got)); // 腾出一个位置 → 挂着的发送者被放行
        EXPECT_EQ(got, 1);
        co_await std::move(blocker);
        *sent_after = ok.load();
        co_return;
    }

    // 3) rendezvous: 没有接收者时值不留在缓冲, 双方在场才成交
    coro::Task<> rendezvous_needs_both(int* buffered, int* received, int* sender_ok) {
        auto ch = coro::channel<int>::rendezvous();
        auto tx = ch.make_sender();
        auto rx = ch.make_receiver();
        std::atomic<int> ok{0}, closed{0};

        // 没有接收者在场: 值必须停在发送者手里, 绝不能被缓存进缓冲
        auto sender_task = coro::spawn(send_and_record(&tx, 42, &ok, &closed));
        co_await coro::yield();
        *buffered = static_cast<int>(ch.size());

        auto v = co_await rx.recv(); // 接收者到场 → 直接交接
        *received = v.has_value() ? *v : -1;
        co_await std::move(sender_task);
        *sender_ok = ok.load();
        co_return;
    }

    // 4) 关闭后先排空再 EOF
    coro::Task<> drains_then_eof(std::vector<int>* out, int* eof_seen) {
        auto ch = coro::channel<int>::bounded(4);
        auto tx = ch.make_sender();
        auto rx = ch.make_receiver();
        (void)tx.try_send(7);
        (void)tx.try_send(8);
        tx.close(); // 显式关闭发送侧, 存量保留
        while (true) {
            auto v = co_await rx.recv();
            if (!v.has_value()) {
                *eof_seen = 1;
                co_return;
            }
            out->push_back(*v);
        }
    }

    // 5) 最后一个 sender 释放 = 发送侧关闭
    coro::Task<> last_sender_release_closes(int* received, int* eof) {
        auto ch = coro::channel<int>::bounded(2);
        auto rx = ch.make_receiver();
        int got = 0, is_eof = 0;
        {
            auto tx = ch.make_sender();
            (void)tx.try_send(5);
        } // tx 析构: 最后一个发送端释放 = 发送侧关闭, 存量 5 仍可排空
        while (true) {
            auto v = co_await rx.recv();
            if (!v.has_value()) {
                is_eof = 1;
                break;
            }
            ++got;
        }
        *received = got;
        *eof = is_eof;
        co_return;
    }

    // 6) 接收侧全灭时, 挂起的发送者必须失败退出而不是永久等待
    coro::Task<> no_receivers_fails_sender(int* closed_count) {
        auto ch = coro::channel<int>::bounded(1);
        auto tx = ch.make_sender();
        auto rx = ch.make_receiver();
        std::atomic<int> ok{0}, closed{0};
        (void)tx.try_send(1); // 占满容量
        auto blocked = coro::spawn(send_and_record(&tx, 2, &ok, &closed));
        co_await coro::yield(); // 该发送者已挂在队列上

        rx = coro::channel<int>::receiver{}; // 最后一个接收端释放 → 无人会再取
        co_await std::move(blocked);
        *closed_count = closed.load();
        EXPECT_EQ(ok.load(), 0);
        co_return;
    }

    // 7) move-only 元素
    coro::Task<> move_only_round_trip(int* sum) {
        auto ch = coro::channel<std::unique_ptr<int>>::bounded(2);
        auto tx = ch.make_sender();
        auto rx = ch.make_receiver();
        co_await tx.send(std::make_unique<int>(10));
        co_await tx.send(std::make_unique<int>(32));
        tx.close();
        int total = 0;
        while (auto v = co_await rx.recv()) {
            total += **v;
        }
        *sum = total;
        co_return;
    }

    // 8) 挂起的发送者被取消: 值随节点消失 (不残留、不误交给后来者), 名额归还
    coro::Task<> cancelled_sender_leaves_nothing(int* next_value) {
        auto ch = coro::channel<int>::bounded(1);
        auto tx = ch.make_sender();
        auto rx = ch.make_receiver();
        (void)tx.try_send(1); // 占满
        std::atomic<int> ok{0}, closed{0};
        auto blocked = coro::spawn(send_and_record(&tx, 2, &ok, &closed));
        co_await coro::yield(); // 该发送者已挂在队列上

        blocked.cancel();
        try {
            co_await std::move(blocked);
        } catch (const coro::CancelledError&) {
        }

        // 缓冲里只有原来的 1; 被取消的 2 必须整体消失
        int got = 0;
        EXPECT_TRUE(rx.try_recv(got));
        EXPECT_EQ(got, 1);
        EXPECT_EQ(ch.size(), 0u);

        // 名额已归还: 现在还能再发一次并被取到
        EXPECT_TRUE(tx.try_send(9));
        int again = 0;
        *next_value = rx.try_recv(again) ? again : -1;
        co_return;
    }

} // namespace

TEST(ChannelTest, BoundedKeepsFifo) {
    std::vector<int> out;
    test_util::run_task([&] { return bounded_fifo(&out); });
    ASSERT_EQ(out.size(), 3u);
    EXPECT_EQ(out[0], 0);
    EXPECT_EQ(out[2], 2);
}

TEST(ChannelTest, SendBlocksUntilReceiverDrains) {
    int before = -1, after = -1;
    test_util::run_task([&] { return send_blocks_when_full(&before, &after); });
    EXPECT_EQ(before, 0) << "缓冲满时发送者不该已经完成";
    EXPECT_EQ(after, 1) << "接收方腾出位置后挂着的发送者应被放行";
}

TEST(ChannelTest, RendezvousNeedsBothParties) {
    int buffered = -1, received = -1, sender_ok = -1;
    test_util::run_task([&] { return rendezvous_needs_both(&buffered, &received, &sender_ok); });
    EXPECT_EQ(buffered, 0) << "rendezvous 不该把值偷偷缓存起来";
    EXPECT_EQ(received, 42);
    EXPECT_EQ(sender_ok, 1);
}

TEST(ChannelTest, DrainsBufferedValuesBeforeEOF) {
    std::vector<int> out;
    int eof = 0;
    test_util::run_task([&] { return drains_then_eof(&out, &eof); });
    ASSERT_EQ(out.size(), 2u);
    EXPECT_EQ(out[0], 7);
    EXPECT_EQ(out[1], 8);
    EXPECT_EQ(eof, 1) << "排空存量之后必须报 EOF";
}

TEST(ChannelTest, LastSenderReleaseClosesSendSide) {
    int received = 0, eof = 0;
    test_util::run_task([&] { return last_sender_release_closes(&received, &eof); });
    EXPECT_EQ(received, 1) << "关闭前入队的值必须仍可排空";
    EXPECT_EQ(eof, 1) << "最后一个 sender 释放应等价于关闭发送侧";
}

TEST(ChannelTest, NoReceiversFailsPendingSender) {
    int closed_count = 0;
    test_util::run_task([&] { return no_receivers_fails_sender(&closed_count); });
    EXPECT_EQ(closed_count, 1) << "接收侧全灭时挂起的发送者必须失败退出而非永久等待";
}

TEST(ChannelTest, SupportsMoveOnlyElements) {
    int sum = 0;
    test_util::run_task([&] { return move_only_round_trip(&sum); });
    EXPECT_EQ(sum, 42);
}

TEST(ChannelTest, CancelledSenderLeavesNoValueOrSlot) {
    int next_value = 0;
    test_util::run_task([&] { return cancelled_sender_leaves_nothing(&next_value); });
    EXPECT_EQ(next_value, 9) << "被取消的发送者残留了值或占着名额";
}

#endif // CORO_HAS_CONCURRENCY_EXT
