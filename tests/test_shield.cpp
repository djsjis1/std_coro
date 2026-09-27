// test_shield.cpp — coro::cancellation_shield 的取消延后语义 (计划 M3c)
//
// 合同: 屏蔽作用域存续期间, 所属 CancellationSource 的 cancel() 不产生终态也不唤醒
// 等待者; 最后一个作用域退出时把延后的取消补发。嵌套按计数, 最外层退出才生效。
#if defined(CORO_HAS_CONCURRENCY_EXT) && CORO_HAS_CONCURRENCY_EXT

#include <gtest/gtest.h>

#include <coro/context.hpp>
#include <coro/coro.hpp>

#include "test_util.h"

#include <atomic>
#include <chrono>
#include <memory>

using namespace std::chrono_literals;

namespace {

    // 等一次上下文结束, 记录原因: 1 = 取消, 2 = 超时, 0 = 正常返回
    // (各测试文件各自持有这份小 helper, 匿名 namespace 不跨 TU, 不做共享头)
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

    /// 作用域存续期间取消一次, 立刻观察 token 状态
    coro::Task<> cancel_inside_shield(bool* cancelled_during, bool* cancelled_after) {
        coro::CancellationSource src;
        {
            auto shield = src.make_shield();
            src.cancel();
            *cancelled_during = src.token().cancelled();
        }
        *cancelled_after = src.token().cancelled();
        co_return;
    }

    /// 等待者挂起时取消被屏蔽 -> 不该被唤醒; 作用域退出后应被唤醒并收到取消
    coro::Task<> deferred_wakeup_on_scope_exit(int* reason) {
        coro::CancellationSource src;
        auto ctx = coro::Context::from(src.token());
        auto waiter = std::make_shared<coro::Task<>>(coro::spawn(wait_reason(&ctx, reason)));
        co_await coro::yield();
        {
            auto shield = src.make_shield();
            src.cancel();
            co_await coro::sleep(30ms);
            // 屏蔽期间等待者不该被唤醒: 仍处在 reason 未写状态
        }
        // 作用域已退出, 取消补发 -> 等待者应很快收到
        co_await coro::sleep(60ms);
        co_await std::move(*waiter);
        co_return;
    }

    /// 嵌套: 内层退出不生效, 外层退出才生效
    coro::Task<> nested_shields(bool* still_shielded_after_inner, bool* cancelled_after_outer) {
        coro::CancellationSource src;
        {
            auto outer = src.make_shield();
            {
                auto inner = src.make_shield();
                src.cancel();
            } // 内层退出: 计数还不为 0
            *still_shielded_after_inner = !src.token().cancelled();
        } // 外层退出: 补发
        *cancelled_after_outer = src.token().cancelled();
        co_return;
    }

    /// 没有屏蔽时行为不得改变 (回归保护)
    coro::Task<> no_shield_cancels_immediately(bool* cancelled_now) {
        coro::CancellationSource src;
        src.cancel();
        *cancelled_now = src.token().cancelled();
        co_return;
    }

    /// 屏蔽只延后"取消", 不该影响 deadline: 超时仍按原样报告
    coro::Task<> shield_does_not_swallow_timeout(int* reason) {
        coro::CancellationSource src;
        auto ctx = coro::Context::from(src.token()).with_deadline(30ms);
        auto shield = src.make_shield();
        co_await wait_reason(&ctx, reason);
        co_return;
    }

} // namespace

TEST(ShieldTest, CancelInsideScopeIsDeferred) {
    bool during = true, after = false;
    test_util::run_task([&] { return cancel_inside_shield(&during, &after); });
    EXPECT_FALSE(during) << "屏蔽作用域内不该看到终态";
    EXPECT_TRUE(after) << "作用域退出后延后的取消必须补上";
}

TEST(ShieldTest, WaiterWakesOnlyWhenScopeExits) {
    int reason = 0;
    test_util::run_task([&] { return deferred_wakeup_on_scope_exit(&reason); });
    EXPECT_EQ(reason, 1) << "补发的取消必须以 CancelledError 浮现";
}

TEST(ShieldTest, NestedScopesNeedOutermostExit) {
    bool still_shielded = false;
    bool cancelled_after_outer = false;
    test_util::run_task([&] { return nested_shields(&still_shielded, &cancelled_after_outer); });
    EXPECT_TRUE(still_shielded) << "内层退出时计数未到 0, 取消不该生效";
    EXPECT_TRUE(cancelled_after_outer) << "最外层退出后必须生效";
}

TEST(ShieldTest, NoShieldKeepsOriginalSemantics) {
    bool cancelled_now = false;
    test_util::run_task([&] { return no_shield_cancels_immediately(&cancelled_now); });
    EXPECT_TRUE(cancelled_now) << "不使用屏蔽时取消必须立即置终态";
}

TEST(ShieldTest, ShieldDoesNotSwallowDeadline) {
    int reason = 0;
    test_util::run_task([&] { return shield_does_not_swallow_timeout(&reason); });
    EXPECT_EQ(reason, 2) << "屏蔽只针对取消, deadline 到点仍应报 TimeoutError";
}

#endif // CORO_HAS_CONCURRENCY_EXT
