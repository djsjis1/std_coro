// test_concurrency.cpp — gather / wait_tasks(AllCompleted|FirstCompleted) / wait_for
// 注: 旧的 gather_all/gather_void/wait_any 已标记 deprecated, 测试一律走新入口,
// 以便 CORO_WERROR=ON 时本目标仍能编译。
#include <gtest/gtest.h>

#include <coro/coro.hpp>

#include "test_util.h"

#include <string>
#include <vector>

using namespace std::chrono_literals;

namespace {

    // ── 命名协程函数 ──

    coro::Task<std::string> fetch_name() {
        co_await coro::sleep(20ms);
        co_return std::string("alice");
    }

    coro::Task<int> fetch_count() {
        co_await coro::sleep(10ms);
        co_return 42;
    }

    coro::Task<> gather_two(std::string* name, int* count) {
        auto [s, n] = co_await coro::gather(fetch_name(), fetch_count());
        *name = s;
        *count = n;
    }

    coro::Task<int> make_num(int i) {
        co_await coro::sleep(5ms);
        co_return i * 10;
    }

    coro::Task<> gather_dynamic(std::vector<int>* out) {
        std::vector<coro::Task<int>> tv;
        for (int i = 0; i < 4; ++i)
            tv.push_back(make_num(i));
        *out = co_await coro::wait_tasks(std::move(tv), coro::WaitMode::AllCompleted);
    }

    coro::Task<int> fail_after_ms(int ms) {
        co_await coro::sleep(std::chrono::milliseconds(ms));
        throw std::runtime_error("sub task failed");
        co_return 0;
    }

    // gather 异常: 第一个异常传播, 其余任务继续完成
    coro::Task<> gather_with_exception(std::string* msg, int* completed) {
        auto slow = coro::spawn(fetch_count()); // 正常任务 (10ms)
        try {
            co_await coro::gather(fetch_name(), fail_after_ms(5));
        } catch (const std::runtime_error& e) {
            *msg = e.what();
        }
        *completed = co_await std::move(slow); // 其他任务不受影响
    }

    coro::Task<> tiny() {
        co_await coro::sleep(5ms);
        co_return;
    }

    coro::Task<> long_sleep_void() {
        co_await coro::sleep(10s);
        co_return;
    }

    coro::Task<> gather_void_ok(int* sum) {
        auto a = coro::spawn(tiny());
        auto b = coro::spawn(tiny());
        (void)co_await coro::gather(tiny());
        co_await std::move(a);
        co_await std::move(b);
        *sum = 3;
    }

    // void 任务异常传播 (原 gather_void 回归测试, 现已改用 gather)
    coro::Task<> fail_void_after_ms(int ms) {
        co_await coro::sleep(std::chrono::milliseconds(ms));
        throw std::runtime_error("void sub failed");
        co_return;
    }

    coro::Task<> gather_void_exception(bool* caught) {
        try {
            (void)co_await coro::gather(tiny(), fail_void_after_ms(10));
        } catch (const std::runtime_error&) {
            *caught = true;
        }
    }

    coro::Task<> gather_void_empty(bool* returned) {
        (void)co_await coro::gather();
        *returned = true;
    }

    // ── gather: 调用方被取消 → 子任务收到 CancelledError (级联取消) ──
    // 旧实现把子任务随临时 awaiter 硬销毁, await 点之后的清理代码不会执行。
    coro::Task<int> cancel_cleanup_child(int* cleaned) {
        try {
            co_await coro::sleep(10s);
        } catch (const coro::CancelledError&) {
            ++*cleaned; // 子任务 await 点之后的清理逻辑必须执行
            throw;
        }
        co_return 0;
    }

    coro::Task<int> gather_cancels_children_impl(int* cleaned, bool* awaiting_cancelled) {
        try {
            auto results = co_await coro::gather(cancel_cleanup_child(cleaned), cancel_cleanup_child(cleaned));
            co_return std::get<0>(results) + std::get<1>(results);
        } catch (const coro::CancelledError&) {
            *awaiting_cancelled = true;
            throw;
        }
    }

    coro::Task<> gather_cancel_driver(int* cleaned, bool* awaiting_cancelled) {
        auto awaiting = coro::spawn(gather_cancels_children_impl(cleaned, awaiting_cancelled));
        co_await coro::sleep(20ms);
        awaiting.cancel();
        try {
            (void)co_await std::move(awaiting);
        } catch (const coro::CancelledError&) {
            // 预期: 等待 gather 的协程本身被取消
        }
        // 留出时间让子任务的取消注入与 monitor 收尾跑完
        co_await coro::sleep(20ms);
    }

    coro::Task<int> slow_count() {
        co_await coro::sleep(200ms);
        co_return 1;
    }

    coro::Task<> wait_for_timeout(bool* caught) {
        try {
            co_await coro::wait_for(slow_count(), 30ms); // 200ms 任务, 30ms 超时
        } catch (const coro::TimeoutError&) {
            *caught = true;
        }
    }

    coro::Task<> wait_for_ok(int* result) {
        *result = co_await coro::wait_for(fetch_count(), 1s);
    }

    coro::Task<> wait_for_void_timeout(bool* caught) {
        try {
            co_await coro::wait_for(long_sleep_void(), 30ms);
        } catch (const coro::TimeoutError&) {
            *caught = true;
        }
    }

    coro::Task<> wait_for_caller_cancel(bool* cancelled) {
        auto wrapped = coro::spawn(coro::wait_for(slow_count(), 1s));
        co_await coro::sleep(10ms);
        wrapped.cancel();
        try {
            (void)co_await std::move(wrapped);
        } catch (const coro::CancelledError&) {
            *cancelled = true;
        }
    }

    coro::Task<int> fast_num(int v, int ms) {
        co_await coro::sleep(std::chrono::milliseconds(ms));
        co_return v;
    }

    /// 两路竞速 (FirstCompleted) 的薄包装: 代替已弃用的 wait_any
    coro::Task<int> first_of_two(coro::Task<int> a, coro::Task<int> b) {
        std::vector<coro::Task<int>> tasks;
        tasks.reserve(2);
        tasks.push_back(std::move(a));
        tasks.push_back(std::move(b));
        auto results = co_await coro::wait_tasks(std::move(tasks), coro::WaitMode::FirstCompleted);
        co_return std::move(results.front());
    }

    coro::Task<> first_completed_fast(int* result) {
        *result = co_await first_of_two(fast_num(100, 20), fast_num(200, 100));
    }

    coro::Task<> cancel_first_completed(bool* caught) {
        auto pending = coro::spawn(first_of_two(fast_num(1, 80), fast_num(2, 100)));
        co_await coro::sleep(10ms);
        pending.cancel();
        try {
            (void)co_await std::move(pending);
        } catch (const coro::CancelledError&) {
            *caught = true;
        }
    }

} // namespace

TEST(ConcurrencyTest, GatherStaticMixedTypes) {
    std::string name;
    int count = 0;
    test_util::run_task([&] { return gather_two(&name, &count); });
    EXPECT_EQ(name, "alice");
    EXPECT_EQ(count, 42);
}

// gather 级联取消: 调用方被取消时子任务必须收到 CancelledError 并执行清理
TEST(ConcurrencyTest, GatherCancelsChildrenOnCallerCancel) {
    int cleaned = 0; // 单 loop 线程访问, 无需原子
    bool awaiting_cancelled = false;
    test_util::run_task([&] { return gather_cancel_driver(&cleaned, &awaiting_cancelled); });
    EXPECT_TRUE(awaiting_cancelled);
    EXPECT_EQ(cleaned, 2);
}

// coro::run: 事件循环被 stop() 提前终止而主任务未完成时必须显式报错
// (旧实现静默返回, 把未完成伪装成成功)
TEST(RunStopTest, RunStoppedBeforeMainCompletesThrows) {
    bool threw = false;
    try {
        coro::run([]() -> coro::Task<> {
            auto stopper = coro::spawn([]() -> coro::Task<> {
                co_await coro::sleep(10ms);
                coro::EventLoop::get().stop();
                co_return;
            }());
            (void)stopper; // 保活: stopper 必须活到完成
            co_await coro::sleep(10s);
            co_return;
        }());
    } catch (const std::runtime_error&) {
        threw = true;
    }
    EXPECT_TRUE(threw);
}

TEST(ConcurrencyTest, GatherAllDynamic) {
    std::vector<int> out;
    test_util::run_task([&] { return gather_dynamic(&out); });
    ASSERT_EQ(out.size(), 4u);
    EXPECT_EQ(out[0], 0);
    EXPECT_EQ(out[1], 10);
    EXPECT_EQ(out[2], 20);
    EXPECT_EQ(out[3], 30);
}

TEST(ConcurrencyTest, GatherPropagatesFirstException) {
    std::string msg;
    int completed = 0;
    test_util::run_task([&] { return gather_with_exception(&msg, &completed); });
    EXPECT_EQ(msg, "sub task failed");
    EXPECT_EQ(completed, 42); // 其他任务正常完成
}

TEST(ConcurrencyTest, GatherVoidOk) {
    int sum = 0;
    test_util::run_task([&] { return gather_void_ok(&sum); });
    EXPECT_EQ(sum, 3);
}

TEST(ConcurrencyTest, GatherVoidPropagatesException) {
    bool caught = false;
    test_util::run_task([&] { return gather_void_exception(&caught); });
    EXPECT_TRUE(caught);
}

TEST(ConcurrencyTest, GatherVoidEmptyCompletesImmediately) {
    bool returned = false;
    test_util::run_task([&] { return gather_void_empty(&returned); });
    EXPECT_TRUE(returned);
}

TEST(ConcurrencyTest, WaitForTimeout) {
    bool caught = false;
    test_util::run_task([&] { return wait_for_timeout(&caught); });
    EXPECT_TRUE(caught);
}

TEST(ConcurrencyTest, WaitForSuccess) {
    int result = 0;
    test_util::run_task([&] { return wait_for_ok(&result); });
    EXPECT_EQ(result, 42);
}

TEST(ConcurrencyTest, WaitForVoidTask) {
    bool caught = false;
    test_util::run_task([&] { return wait_for_void_timeout(&caught); });
    EXPECT_TRUE(caught);
}

TEST(ConcurrencyTest, WaitForPropagatesCallerCancellation) {
    bool cancelled = false;
    test_util::run_task([&] { return wait_for_caller_cancel(&cancelled); });
    EXPECT_TRUE(cancelled);
}

TEST(ConcurrencyTest, FirstCompletedFastWins) {
    int result = 0;
    test_util::run_task([&] { return first_completed_fast(&result); });
    EXPECT_EQ(result, 100); // 快者胜
}

TEST(ConcurrencyTest, FirstCompletedCancellationKeepsMonitorsSafe) {
    bool caught = false;
    test_util::run_task([&] { return cancel_first_completed(&caught); });
    EXPECT_TRUE(caught);
}
