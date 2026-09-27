// test_context.cpp — coro::Context / CancellationSource 的取消与 deadline 合同
#include <gtest/gtest.h>

#include <coro/context.hpp>

#include "test_util.h"

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>

#if defined(CORO_HAS_CONCURRENCY_EXT) && CORO_HAS_CONCURRENCY_EXT

using namespace std::chrono_literals;

namespace {

    // 等一次上下文结束, 记录原因: 1 = 取消, 2 = 超时, 0 = 正常返回 (不该出现)
    coro::Task<> wait_reason(coro::Context* ctx, int* reason) {
        try {
            co_await ctx->wait();
            *reason = 0;
        } catch (const coro::CancelledError&) {
            *reason = 1;
        } catch (const coro::TimeoutError&) {
            *reason = 2;
        }
        co_return;
    }

    coro::Task<> wait_one(coro::Context* ctx, std::atomic<int>* woken) {
        int reason = 0;
        co_await wait_reason(ctx, &reason);
        if (reason != 0)
            woken->fetch_add(1);
        co_return;
    }

    // 取消先于等待: 等待必须立刻得到"已取消", 绝不静默吞掉
    coro::Task<> cancel_before_wait(int* reason) {
        coro::CancellationSource src;
        auto ctx = coro::Context::from(src.token());
        src.cancel();
        co_await wait_reason(&ctx, reason);
        co_return;
    }

    // 等待者已挂起后被取消: 同样报告取消
    coro::Task<> cancel_while_waiting(int* reason) {
        coro::CancellationSource src;
        auto ctx = coro::Context::from(src.token());
        auto waiter = coro::spawn(wait_reason(&ctx, reason));
        co_await coro::yield();
        src.cancel();
        co_await std::move(waiter);
        co_return;
    }

    // 多个等待者必须全部被唤醒, 一个都不能落下
    coro::Task<> wakes_all_waiters(std::atomic<int>* woken) {
        coro::CancellationSource src;
        auto ctx = coro::Context::from(src.token());
        auto a = coro::spawn(wait_one(&ctx, woken));
        auto b = coro::spawn(wait_one(&ctx, woken));
        auto c = coro::spawn(wait_one(&ctx, woken));
        co_await coro::yield();
        src.cancel();
        co_await std::move(a);
        co_await std::move(b);
        co_await std::move(c);
        co_return;
    }

    // 只有期限、无人取消: 报告超时
    coro::Task<> deadline_alone(int* reason) {
        auto ctx = coro::Context::with_timeout(30ms);
        co_await wait_reason(&ctx, reason);
        co_return;
    }

    // 子上下文继承父取消; 父已取消时子也算取消
    coro::Task<> child_sees_parent_cancel(bool* child_cancelled) {
        coro::CancellationSource src;
        auto parent = coro::Context::from(src.token()).with_deadline(10s);
        auto child = parent.make_child();
        src.cancel();
        *child_cancelled = child.cancelled();
        co_return;
    }

    // 子上下文继承父 deadline; 子自己更晚的期限不得放宽父的限制
    coro::Task<> child_keeps_earlier_deadline(bool* tightened) {
        auto parent = coro::Context::with_timeout(30ms);
        auto child = parent.with_deadline(10s);
        *tightened = child.deadline().has_value() && child.deadline() == parent.deadline();
        co_return;
    }

    // 同步检查点: 长计算里主动跳出
    coro::Task<> checkpoint_after_cancel(bool* threw) {
        coro::CancellationSource src;
        auto ctx = coro::Context::from(src.token());
        src.cancel();
        try {
            ctx.throw_if_cancelled();
        } catch (const coro::CancelledError&) {
            *threw = true;
        }
        co_return;
    }

    coro::Task<> wait_reason_shared(std::shared_ptr<coro::Context> ctx, int* reason) {
        co_await wait_reason(ctx.get(), reason); // 必须捕获: 异常逃出会跳过 join
        co_return;
    }

    // 跨线程取消: 唤醒必须回到等待者所属循环, 不能靠超时兜底
    coro::Task<> cross_thread_cancel(int* reason, int* cost_ms) {
        auto src = std::make_shared<coro::CancellationSource>();
        auto ctx = std::make_shared<coro::Context>(coro::Context::from(src->token()));
        const auto begin = std::chrono::steady_clock::now();
        auto waiter = coro::spawn(wait_reason_shared(ctx, reason));
        co_await coro::yield();
        std::jthread killer([src] {
            std::this_thread::sleep_for(20ms);
            src->cancel();
        });
        co_await std::move(waiter);
        *cost_ms = static_cast<int>(
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - begin).count());
        co_return;
    }

    // 取消源比令牌/上下文活得更短也必须安全: 状态由 shared_ptr 保活
    coro::Task<> source_dies_before_token(bool* still_ok) {
        coro::CancellationToken held;
        {
            coro::CancellationSource src;
            held = src.token();
        } // src 析构
        *still_ok = !held.cancelled();
        co_return;
    }

} // namespace

TEST(ContextTest, CancelBeforeWaitIsNotSwallowed) {
    int reason = 0;
    test_util::run_task([&] { return cancel_before_wait(&reason); });
    EXPECT_EQ(reason, 1) << "取消先发生时, wait 必须立刻报告取消";
}

TEST(ContextTest, CancelWhileWaitingReportsCancel) {
    int reason = 0;
    test_util::run_task([&] { return cancel_while_waiting(&reason); });
    EXPECT_EQ(reason, 1);
}

TEST(ContextTest, AllWaitersAreWoken) {
    std::atomic<int> woken{0};
    test_util::run_task([&] { return wakes_all_waiters(&woken); });
    EXPECT_EQ(woken.load(), 3) << "有等待者被落下";
}

TEST(ContextTest, DeadlineWithoutCancelReportsTimeout) {
    int reason = 0;
    test_util::run_task([&] { return deadline_alone(&reason); });
    EXPECT_EQ(reason, 2);
}

TEST(ContextTest, ChildInheritsParentCancellation) {
    bool child_cancelled = false;
    test_util::run_task([&] { return child_sees_parent_cancel(&child_cancelled); });
    EXPECT_TRUE(child_cancelled);
}

TEST(ContextTest, ChildKeepsEarlierDeadline) {
    bool tightened = false;
    test_util::run_task([&] { return child_keeps_earlier_deadline(&tightened); });
    EXPECT_TRUE(tightened) << "子上下文把父 deadline 放宽了";
}

TEST(ContextTest, CheckpointThrowsAfterCancel) {
    bool threw = false;
    test_util::run_task([&] { return checkpoint_after_cancel(&threw); });
    EXPECT_TRUE(threw);
}

TEST(ContextTest, CrossThreadCancelWakesOnOwnerLoop) {
    int reason = 0, cost = 0;
    test_util::run_task([&] { return cross_thread_cancel(&reason, &cost); });
    EXPECT_EQ(reason, 1);
    EXPECT_LT(cost, 2000) << "取消未被及时唤醒, 耗时 " << cost << "ms";
}

TEST(ContextTest, TokenOutlivesItsSourceSafely) {
    bool still_ok = false;
    test_util::run_task([&] { return source_dies_before_token(&still_ok); });
    EXPECT_TRUE(still_ok) << "取消源析构后令牌状态不应变成已取消";
}

namespace {

    /// 帧局部探针: 计数当前存活的协程帧 (帧销毁即 -1)
    struct frame_probe {
        static std::atomic<int> alive;
        frame_probe() { alive.fetch_add(1); }
        ~frame_probe() { alive.fetch_sub(1); }
    };
    std::atomic<int> frame_probe::alive{0};

    /// 带 deadline 的等待超时结束 -> 协程帧随之销毁。
    /// 摘链必须发生在销毁之前, 否则之后的 cancel() 会对已释放句柄 schedule。
    coro::Task<> deadline_then_report_frame_state(coro::CancellationSource* src, int* reason, bool* frame_gone) {
        auto ctx = coro::Context::from(src->token()).with_deadline(30ms);
        {
            frame_probe probe;
            co_await wait_reason(&ctx, reason);
        }
        *frame_gone = frame_probe::alive.load() == 0;
        co_return;
    }

    /// 一次干净的"取消唤醒新等待", 用来证明摘链没把状态机搞坏
    coro::Task<> fresh_wait_still_cancellable(int* reason) {
        coro::CancellationSource s;
        auto ctx = coro::Context::from(s.token());
        auto waiter = coro::spawn(wait_reason(&ctx, reason));
        co_await coro::yield();
        s.cancel();
        co_await std::move(waiter);
        co_return;
    }

} // namespace

TEST(ContextTest, CancelAfterTimeoutDoesNotWakeDestroyedFrame) {
    int reason = 0;
    bool frame_gone = false;
    coro::CancellationSource src;
    test_util::run_task([&] { return deadline_then_report_frame_state(&src, &reason, &frame_gone); });
    EXPECT_EQ(reason, 2) << "先到原因的应是 deadline";
    EXPECT_TRUE(frame_gone) << "等待协程帧应已销毁";

    // 帧销毁之后再取消: 旧等待者必须已被摘链, 否则这里会对野句柄 schedule
    src.cancel();
    int after = 0;
    test_util::run_task([&] { return fresh_wait_still_cancellable(&after); });
    EXPECT_EQ(after, 1) << "取消语义对新等待者仍然正常 (摘链没破坏状态机)";
}
#endif // CORO_HAS_CONCURRENCY_EXT
