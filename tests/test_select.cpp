// test_select.cpp — coro::select: 只有一个分支能提交副作用 (计划 M3b)
//
// 关键合同 (输家必须干净):
//   - 被 select 挂起的等待者在未获胜时不得消费消息、不得提交发送;
//   - 输家的值原封不动留给后续接收者, 名额归还;
//   - 取消挂起中的 select 不留下任何残留登记。
#if defined(CORO_HAS_CONCURRENCY_EXT) && CORO_HAS_CONCURRENCY_EXT

#include <gtest/gtest.h>

#include <coro/channel.hpp>
#include <coro/coro.hpp> // run / sleep / spawn: select 有意不拉聚合头, 测试自己引入
#include <coro/select.hpp>

#include <atomic>
#include <chrono>
#include <memory>
#include <optional>

using namespace std::chrono_literals;

namespace {

    coro::Task<int> pick_ready_second(coro::channel<int>::receiver rx1, coro::channel<int>::receiver rx2) {
        auto r = co_await coro::select(coro::recv_of(rx1), coro::recv_of(rx2));
        co_return r.index * 1000 + r.value.value_or(-1);
    }

    coro::Task<bool> times_out_when_idle(coro::channel<int>::receiver rx) {
        auto r = co_await coro::select(coro::recv_of(rx), coro::after(30ms));
        co_return r.timed_out;
    }

    coro::Task<bool> default_branch_never_blocks(coro::channel<int>::receiver rx) {
        auto r = co_await coro::select(coro::recv_of(rx), coro::default_nowait());
        co_return r.defaulted;
    }

    coro::Task<int> suspended_then_delivered(coro::channel<int>::receiver rx) {
        auto r = co_await coro::select(coro::recv_of(rx), coro::after(500ms));
        if (r.value.has_value())
            co_return *r.value;
        co_return -1;
    }

    coro::Task<> delayed_feed(typename coro::channel<int>::sender tx, int v) {
        co_await coro::sleep(20ms);
        co_await tx.send(v);
    }

    coro::Task<bool> send_branch_completes_on_taker(typename coro::channel<int>::sender tx) {
        auto r = co_await coro::select(coro::send_of(tx, 42), coro::after(500ms));
        co_return r.index == 0 && !r.timed_out;
    }

    coro::Task<> delayed_taker(typename coro::channel<int>::receiver rx, std::atomic<int>* taken_value) {
        co_await coro::sleep(10ms);
        if (auto v = co_await rx.recv())
            *taken_value = *v;
    }

    /// 核心合同: 赢家取走值之后, 输家通道里的值必须原封不动
    coro::Task<bool> loser_keeps_its_value(coro::channel<int>::receiver rxA, coro::channel<int>::receiver rxB,
                                           int* loser_value) {
        auto r = co_await coro::select(coro::recv_of(rxA), coro::recv_of(rxB));
        int seen = -1;
        const bool loser_had_value = rxB.try_recv(seen);
        *loser_value = seen;
        co_return r.value.has_value() && loser_had_value && (r.index == 0 ? *r.value == 1 && seen == 2 : *r.value == 2);
    }

    /// 关闭的通道在 select 里表现为该分支的 EOF (nullopt), 而不是吞掉别的分支
    coro::Task<bool> closed_branch_reports_eof(coro::channel<int>::receiver rxA, coro::channel<int>::receiver rxB) {
        auto r = co_await coro::select(coro::recv_of(rxA), coro::recv_of(rxB)); // rxA 已关闭且空
        co_return r.index == 0 && !r.value.has_value();
    }

    /// 撤销不残留: select 超时退出后, 通道里后到的值仍能被正常接收
    coro::Task<bool> timeout_leaves_no_registration(coro::channel<int>::receiver rx,
                                                    typename coro::channel<int>::sender tx) {
        auto r = co_await coro::select(coro::recv_of(rx), coro::after(10ms));
        if (!r.timed_out)
            co_return false;
        co_await tx.send(5); // select 已退出, 这次 send 面对的是正常通道
        auto later = co_await rx.recv();
        co_return later.has_value() && *later == 5;
    }

} // namespace

TEST(SelectTest, ReadyBranchWinsImmediately) {
    int code = -1;
    coro::run([](int* out) -> coro::Task<> {
        auto ch1 = coro::channel<int>::bounded(1);
        auto ch2 = coro::channel<int>::bounded(1);
        auto rx1 = ch1.make_receiver();
        auto rx2 = ch2.make_receiver();
        (void)ch2.make_sender().try_send(7);
        *out = co_await pick_ready_second(rx1, rx2);
    }(&code));
    EXPECT_EQ(code, 1007); // 分支 1 (从 0 起) 拿到值 7
}

TEST(SelectTest, IdleBranchesTimeOut) {
    bool timed_out = false;
    coro::run([](bool* out) -> coro::Task<> {
        auto ch = coro::channel<int>::bounded(1);
        *out = co_await times_out_when_idle(ch.make_receiver());
    }(&timed_out));
    EXPECT_TRUE(timed_out);
}

TEST(SelectTest, DefaultBranchDoesNotBlock) {
    bool defaulted = false;
    coro::run([](bool* out) -> coro::Task<> {
        auto ch = coro::channel<int>::bounded(1);
        *out = co_await default_branch_never_blocks(ch.make_receiver());
    }(&defaulted));
    EXPECT_TRUE(defaulted);
}

TEST(SelectTest, SuspendedSelectGetsDelivered) {
    int value = -1;
    coro::run([](int* out) -> coro::Task<> {
        auto ch = coro::channel<int>::bounded(1);
        auto feeder = coro::spawn(delayed_feed(ch.make_sender(), 99));
        *out = co_await suspended_then_delivered(ch.make_receiver());
        co_await std::move(feeder);
    }(&value));
    EXPECT_EQ(value, 99);
}

TEST(SelectTest, SendBranchCommitsWhenTakerArrives) {
    std::atomic<int> taken{-1};
    bool sent = false;
    coro::run([](std::atomic<int>* taken, bool* out) -> coro::Task<> {
        auto ch = coro::channel<int>::rendezvous();
        auto taker = coro::spawn(delayed_taker(ch.make_receiver(), taken));
        *out = co_await send_branch_completes_on_taker(ch.make_sender());
        co_await std::move(taker);
    }(&taken, &sent));
    EXPECT_TRUE(sent);
    EXPECT_EQ(taken.load(), 42);
}

TEST(SelectTest, LoserDoesNotConsumeItsMessage) {
    int loser_value = -1;
    bool ok = false;
    coro::run([](int* lv, bool* out) -> coro::Task<> {
        auto chA = coro::channel<int>::bounded(1);
        auto chB = coro::channel<int>::bounded(1);
        (void)chA.make_sender().try_send(1);
        (void)chB.make_sender().try_send(2);
        *out = co_await loser_keeps_its_value(chA.make_receiver(), chB.make_receiver(), lv);
    }(&loser_value, &ok));
    EXPECT_TRUE(ok) << "输家的值必须原封不动, 不能被赢家顺路消费";
    if (ok)
        EXPECT_EQ(loser_value, 2);
}

TEST(SelectTest, ClosedChannelBranchReportsEof) {
    bool eof = false;
    coro::run([](bool* out) -> coro::Task<> {
        auto chA = coro::channel<int>::bounded(1);
        auto rx = chA.make_receiver();
        { auto tx = chA.make_sender(); } // 最后一个 sender 释放 → 关闭发送侧
        auto chB = coro::channel<int>::bounded(1);
        *out = co_await closed_branch_reports_eof(rx, chB.make_receiver());
    }(&eof));
    EXPECT_TRUE(eof);
}

TEST(SelectTest, TimeoutLeavesChannelUsable) {
    bool usable = false;
    coro::run([](bool* out) -> coro::Task<> {
        auto ch = coro::channel<int>::bounded(1);
        *out = co_await timeout_leaves_no_registration(ch.make_receiver(), ch.make_sender());
    }(&usable));
    EXPECT_TRUE(usable) << "select 退出后不能在通道里留下残留登记";
}

#endif // CORO_HAS_CONCURRENCY_EXT
