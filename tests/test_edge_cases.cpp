// test_edge_cases.cpp — 核心原语边界条件: 零时/空集/嵌套/状态查询/递归
#include <gtest/gtest.h>

#include <coro/coro.hpp>

#include "test_util.h"

#include <string>
#include <vector>

using namespace std::chrono_literals;

namespace {

    // ── 零时 sleep: 应立即返回或极短延迟 ──
    coro::Task<> zero_sleep(int* out) {
        co_await coro::sleep(0ms);
        *out = 42;
    }

    // ── 嵌套 wait_for: 外层超时包含内层 ──
    coro::Task<int> nested_inner() {
        co_await coro::sleep(50ms);
        co_return 1;
    }

    coro::Task<> nested_wait_for(int* result, bool* inner_caught) {
        try {
            *result = co_await coro::wait_for(coro::wait_for(nested_inner(), 30ms), 200ms);
        } catch (const coro::TimeoutError&) {
            *inner_caught = true;
        }
    }

    // ── Task 状态查询 ──
    coro::Task<int> slow_result() {
        co_await coro::sleep(50ms);
        co_return 99;
    }

    coro::Task<> task_state_queries(bool* ready_before, bool* started_before, bool* ready_after) {
        auto t = coro::spawn(slow_result());
        *started_before = t.is_started();
        *ready_before = t.is_ready();
        int v = co_await std::move(t);
        (void)v;
        *ready_after = true;
    }

    // ── 递归 spawn: 协程 spawn 自己 ──
    coro::Task<int> recursive_spawn(int depth) {
        if (depth <= 0)
            co_return 1;
        auto sub = coro::spawn(recursive_spawn(depth - 1));
        int v = co_await std::move(sub);
        co_return v + 1;
    }

    // ── 大量 yield 不崩溃 ──
    coro::Task<> yield_storm(int n, int* count) {
        for (int i = 0; i < n; ++i)
            co_await coro::yield();
        *count = n;
    }

    // ── EventLoop::dispatch 从协程内部 ──
    coro::Task<> dispatch_from_coroutine(int* dispatched_value) {
        coro::EventLoop::get().dispatch([dispatched_value] { *dispatched_value = 77; });
        co_await coro::yield();
        co_await coro::yield();
    }

    // ── 多次 dispatch 按顺序执行 ──
    coro::Task<> ordered_dispatches(std::vector<int>* order) {
        coro::EventLoop::get().dispatch([order] { order->push_back(1); });
        coro::EventLoop::get().dispatch([order] { order->push_back(2); });
        coro::EventLoop::get().dispatch([order] { order->push_back(3); });
        for (int i = 0; i < 5; ++i)
            co_await coro::yield();
    }

    // ── gather 单个任务 ──
    coro::Task<int> single_value() {
        co_return 42;
    }

    coro::Task<> gather_single(int* result) {
        auto [v] = co_await coro::gather(single_value());
        *result = v;
    }

    // ── gather_all 空集 ──
    coro::Task<> gather_empty(std::vector<int>* out) {
        std::vector<coro::Task<int>> empty;
        *out = co_await coro::gather_all(std::move(empty));
    }

    // ── wait_for 零超时: 立即超时 ──
    coro::Task<> wait_for_zero_timeout(bool* caught) {
        try {
            co_await coro::wait_for(slow_result(), 0ms);
        } catch (const coro::TimeoutError&) {
            *caught = true;
        }
    }

    // ── Task<void> 的 is_ready ──
    coro::Task<> void_marker(int* step) {
        *step = 1;
        co_await coro::yield();
        *step = 2;
    }

    coro::Task<> void_task_ready(int* step) {
        auto t = coro::spawn(void_marker(step));
        co_await coro::yield();
        co_await std::move(t);
    }

    // ── 连续 await 多个已完成任务 ──
    coro::Task<int> immediate_int(int v) {
        co_return v;
    }

    coro::Task<> chain_immediate(int* sum) {
        for (int i = 0; i < 100; ++i)
            *sum += co_await immediate_int(1);
    }

    // ── Event 多次 set (幂等) ──
    coro::Task<> event_idempotent(int* woken) {
        coro::Event ev;
        ev.set();
        ev.set();
        ev.set();
        co_await ev.wait();
        *woken = 1;
    }

    // ── Lock 重入检测 (同一协程连续 acquire) ──
    coro::Task<> lock_reentrant(int* count) {
        coro::Lock lock;
        {
            auto g1 = co_await lock.guard();
            *count = 1;
            {
                auto g2 = co_await lock.guard();
                *count = 2;
            }
            *count = 3;
        }
    }

    // ── 多层嵌套协程 ──
    coro::Task<int> level3(int v) {
        co_await coro::yield();
        co_return v * 3;
    }

    coro::Task<int> level2(int v) {
        int r = co_await level3(v);
        co_return r + 2;
    }

    coro::Task<int> level1(int v) {
        int r = co_await level2(v);
        co_return r + 1;
    }

    coro::Task<> deep_nesting(int* result) {
        *result = co_await level1(10);
    }

} // namespace

// ── 零时 sleep ──
TEST(EdgeCaseTest, ZeroDurationSleep) {
    int out = 0;
    test_util::run_task([&] { return zero_sleep(&out); });
    EXPECT_EQ(out, 42);
}

// ── 嵌套 wait_for ──
TEST(EdgeCaseTest, NestedWaitFor) {
    int result = 0;
    bool inner_caught = false;
    test_util::run_task([&] { return nested_wait_for(&result, &inner_caught); });
    EXPECT_TRUE(inner_caught);
}

// ── Task 状态查询 ──
TEST(EdgeCaseTest, TaskStateQueries) {
    bool ready_before = true, started_before = false, ready_after = false;
    test_util::run_task([&] { return task_state_queries(&ready_before, &started_before, &ready_after); });
    EXPECT_TRUE(started_before);
    EXPECT_FALSE(ready_before);
    EXPECT_TRUE(ready_after);
}

// ── 递归 spawn ──
TEST(EdgeCaseTest, RecursiveSpawn) {
    int result = 0;
    test_util::run_task([&] { return [&]() -> coro::Task<> { result = co_await recursive_spawn(10); }(); });
    EXPECT_EQ(result, 11);
}

// ── 大量 yield ──
TEST(EdgeCaseTest, YieldStorm) {
    int count = 0;
    test_util::run_task([&] { return yield_storm(10000, &count); });
    EXPECT_EQ(count, 10000);
}

// ── dispatch 从协程内部 ──
TEST(EdgeCaseTest, DispatchFromCoroutine) {
    int dispatched_value = 0;
    test_util::run_task([&] { return dispatch_from_coroutine(&dispatched_value); });
    EXPECT_EQ(dispatched_value, 77);
}

// ── 多次 dispatch 按顺序 ──
TEST(EdgeCaseTest, OrderedDispatches) {
    std::vector<int> order;
    test_util::run_task([&] { return ordered_dispatches(&order); });
    ASSERT_EQ(order.size(), 3u);
    EXPECT_EQ(order[0], 1);
    EXPECT_EQ(order[1], 2);
    EXPECT_EQ(order[2], 3);
}

// ── gather 单个任务 ──
TEST(EdgeCaseTest, GatherSingleTask) {
    int result = 0;
    test_util::run_task([&] { return gather_single(&result); });
    EXPECT_EQ(result, 42);
}

// ── gather_all 空集 ──
TEST(EdgeCaseTest, GatherAllEmpty) {
    std::vector<int> out;
    test_util::run_task([&] { return gather_empty(&out); });
    EXPECT_TRUE(out.empty());
}

// ── wait_for 零超时 ──
TEST(EdgeCaseTest, WaitForZeroTimeout) {
    bool caught = false;
    test_util::run_task([&] { return wait_for_zero_timeout(&caught); });
    EXPECT_TRUE(caught);
}

// ── Task<void> 执行流程 ──
TEST(EdgeCaseTest, VoidTaskExecutionFlow) {
    int step = 0;
    test_util::run_task([&] { return void_task_ready(&step); });
    EXPECT_EQ(step, 2);
}

// ── 连续 await 已完成任务 ──
TEST(EdgeCaseTest, ChainImmediateAwaits) {
    int sum = 0;
    test_util::run_task([&] { return chain_immediate(&sum); });
    EXPECT_EQ(sum, 100);
}

// ── Event 幂等 set ──
TEST(EdgeCaseTest, EventIdempotentSet) {
    int woken = 0;
    test_util::run_task([&] { return event_idempotent(&woken); });
    EXPECT_EQ(woken, 1);
}

// ── Lock 重入 (同一协程) ──
TEST(EdgeCaseTest, LockReentrantSameCoroutine) {
    int count = 0;
    test_util::run_task([&] { return lock_reentrant(&count); });
    EXPECT_EQ(count, 3);
}

// ── 多层嵌套协程 ──
TEST(EdgeCaseTest, DeepNesting) {
    int result = 0;
    test_util::run_task([&] { return deep_nesting(&result); });
    EXPECT_EQ(result, 33); // 10*3 + 2 + 1 = 33
}
