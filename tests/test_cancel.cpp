// test_cancel.cpp — 取消语义: 注入 / 清理 / 循环终止 / 未启动 / no-op
#include <gtest/gtest.h>

#include <coro/coro.hpp>

#include "test_util.h"

using namespace std::chrono_literals;

namespace {

    // ── 命名协程函数 ──

    coro::Task<> long_sleeper() {
        co_await coro::sleep(10s);
        co_return;
    }

    // 协程体内有 RAII 清理对象: 取消后必须析构
    coro::Task<> with_cleanup(bool* cleaned_up) {
        struct Guard {
            bool* p;
            ~Guard() { *p = true; }
        } g{cleaned_up};
        co_await coro::sleep(10s);
        co_return;
    }

    coro::Task<> cancel_and_await(bool* caught, bool* cleaned) {
        auto t = coro::spawn(with_cleanup(cleaned));
        co_await coro::yield(); // 让 t 跑起来挂到 sleep 上
        t.cancel();
        try {
            co_await std::move(t);
        } catch (const coro::CancelledError&) {
            *caught = true;
        }
    }

    // 无限循环任务: 取消后必须终止 (旧实现永远无法完成)
    coro::Task<int> infinite_loop(int* iterations) {
        while (true) {
            co_await coro::sleep(1s);
            ++*iterations;
            if (*iterations > 1000)
                break;
        }
        co_return 0;
    }

    coro::Task<> cancel_loop(bool* caught, int* iterations) {
        auto t = coro::spawn(infinite_loop(iterations));
        co_await coro::yield();
        t.cancel();
        try {
            co_await std::move(t);
        } catch (const coro::CancelledError&) {
            *caught = true;
        }
    }

    // 取消未启动的任务
    coro::Task<> cancel_unstarted(bool* caught) {
        auto t = long_sleeper(); // 惰性, 未启动
        t.cancel();
        try {
            co_await std::move(t);
        } catch (const coro::CancelledError&) {
            *caught = true;
        }
    }

    // 取消已完成的任务: no-op, 不崩溃, 结果保留
    coro::Task<int> quick() {
        co_return 7;
    }

    coro::Task<> cancel_finished(int* result, bool* caught) {
        auto t = coro::spawn(quick());
        *result = co_await std::move(t); // 正常完成
        // t 已消耗, 此场景改为: 已完成任务被 cancel 应无效果
        auto t2 = quick();
        t2.start();
        while (!t2.is_ready())
            co_await coro::yield();
        t2.cancel(); // 已完成: no-op
        try {
            (void)co_await std::move(t2);
        } catch (const coro::CancelledError&) {
            *caught = true; // 不应走到这里
        }
    }

    // 协程体内捕获 CancelledError 做清理后继续取消 (取消保护)
    coro::Task<> shield(bool* saw_cancel) {
        try {
            co_await coro::sleep(10s);
        } catch (const coro::CancelledError&) {
            *saw_cancel = true; // 吞掉取消: 协程正常完成
        }
        co_return;
    }

    coro::Task<> cancel_shielded(bool* saw_cancel, bool* got_error) {
        auto t = coro::spawn(shield(saw_cancel));
        co_await coro::yield();
        t.cancel();
        try {
            co_await std::move(t); // 不应抛 (取消被协程体吞掉)
        } catch (const coro::CancelledError&) {
            *got_error = true;
        }
    }

} // namespace

TEST(CancelTest, CancelSuspendedTask) {
    bool caught = false, cleaned = false;
    test_util::run_task([&] { return cancel_and_await(&caught, &cleaned); });
    EXPECT_TRUE(caught);  // 等待者收到 CancelledError
    EXPECT_TRUE(cleaned); // 协程体内 RAII 清理已执行 (注入式取消)
}

TEST(CancelTest, CancelInfiniteLoop) {
    bool caught = false;
    int iterations = 0;
    test_util::run_task([&] { return cancel_loop(&caught, &iterations); });
    EXPECT_TRUE(caught);
    EXPECT_LE(iterations, 5); // 立即终止, 远小于 1000
}

TEST(CancelTest, CancelUnstartedTask) {
    bool caught = false;
    test_util::run_task([&] { return cancel_unstarted(&caught); });
    EXPECT_TRUE(caught);
}

TEST(CancelTest, CancelFinishedTaskIsNoop) {
    int result = 0;
    bool caught = false;
    test_util::run_task([&] { return cancel_finished(&result, &caught); });
    EXPECT_EQ(result, 7);
    EXPECT_FALSE(caught); // 已完成任务的 cancel 不影响结果
}

TEST(CancelTest, CancellationShield) {
    bool saw_cancel = false, got_error = false;
    test_util::run_task([&] { return cancel_shielded(&saw_cancel, &got_error); });
    EXPECT_TRUE(saw_cancel); // 协程体感知到取消
    EXPECT_FALSE(got_error); // 但吞掉了 → 等待者正常收到结果
}
