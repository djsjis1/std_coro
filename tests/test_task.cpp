// test_task.cpp — Task 基础: 惰性启动 / 返回值 / 移动 / 异常 / spawn
#include <gtest/gtest.h>

#include <coro/coro.hpp>

#include "test_util.h"

#include <string>

using namespace std::chrono_literals;

namespace {

    // ── 命名协程函数 (参数进帧，不依赖外部闭包生命周期) ──

    coro::Task<int> compute_42() {
        co_await coro::yield();
        co_return 42;
    }

    coro::Task<> flag_setter(bool* flag) {
        *flag = true;
        co_return;
    }

    coro::Task<> thrower() {
        throw std::runtime_error("boom");
        co_return; // 本函数没有其他协程关键字；用它声明这是协程
    }

    coro::Task<> catch_from(std::string* out) {
        try {
            co_await thrower();
        } catch (const std::runtime_error& e) {
            *out = e.what();
        }
    }

    coro::Task<> spawn_two(int* a, int* b) {
        auto t1 = coro::spawn(compute_42());
        auto t2 = coro::spawn(compute_42());
        *a = co_await std::move(t1);
        *b = co_await std::move(t2);
    }

    coro::Task<> sleep_and_set(int ms, int* out) {
        co_await coro::sleep(std::chrono::milliseconds(ms));
        *out = 1;
    }

    coro::Task<> compute_and_store(int* out) {
        *out = co_await compute_42();
    }

    coro::Task<> try_await_thrower(bool* caught) {
        try {
            co_await thrower();
        } catch (const std::runtime_error&) {
            *caught = true;
        }
    }

    coro::Task<int> thrower_int() {
        throw std::runtime_error("run boom");
        co_return 0;
    }

} // namespace

// ── 惰性启动: 创建不执行 ──
TEST(TaskTest, LazyStart) {
    bool flag = false;
    {
        auto t = flag_setter(&flag); // 创建即挂起, 协程体不执行
        EXPECT_FALSE(flag);
    } // 析构销毁未启动的帧
    EXPECT_FALSE(flag);
}

// ── co_await 返回结果 (串行等待) ──
TEST(TaskTest, CoAwaitResult) {
    int out = -1;
    test_util::run_task([&] { return compute_and_store(&out); });
    EXPECT_EQ(out, 42);
}

// ── spawn 并发 ──
TEST(TaskTest, SpawnConcurrent) {
    int a = 0, b = 0;
    test_util::run_task([&] { return spawn_two(&a, &b); });
    EXPECT_EQ(a, 42);
    EXPECT_EQ(b, 42);
}

// ── 异常跨协程传播 ──
TEST(TaskTest, ExceptionPropagation) {
    std::string msg;
    test_util::run_task([&] { return catch_from(&msg); });
    EXPECT_EQ(msg, "boom");
}

// ── 异常到达等待者 (未捕获时) ──
TEST(TaskTest, ExceptionReachesWaiter) {
    bool caught = false;
    test_util::run_task([&] { return try_await_thrower(&caught); });
    EXPECT_TRUE(caught);
}

// ── sleep 实际等待 (下限校验) ──
TEST(TaskTest, SleepActuallyWaits) {
    auto t0 = std::chrono::steady_clock::now();
    int out = 0;
    test_util::run_task([&] { return sleep_and_set(60, &out); });
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
    EXPECT_EQ(out, 1);
    EXPECT_GE(elapsed, 50); // 至少等了 50ms
}

// ── Task 移动语义 ──
TEST(TaskTest, MoveSemantics) {
    auto t1 = compute_42();
    auto t2 = std::move(t1); // 所有权转移
    t2.start();
    coro::EventLoop::get().run();

    // t1 已空, 不崩溃
    EXPECT_FALSE(t1.is_started());
}

// ── Task<void> 特化 ──
TEST(TaskTest, VoidTask) {
    bool done = false;
    test_util::run_task([&]() -> coro::Task<> { return flag_setter(&done); });
    EXPECT_TRUE(done);
}

// 捕获型协程 lambda 是受支持的；关键是闭包必须比它创建的 Task 活得更久。
TEST(TaskTest, CoroutineLambdaWithLiveClosure) {
    int result = 0;
    auto worker = [&result](int value) -> coro::Task<> {
        co_await coro::yield();
        result = value;
    };

    auto task = worker(42);
    task.start();
    coro::EventLoop::get().run();
    task.take_result();

    EXPECT_EQ(result, 42);
}

// ── coro::run 返回主协程结果 (对标 asyncio.run) ──
TEST(TaskTest, RunReturnsResult) {
    int result = coro::run(compute_42()); // 直接拿结果
    EXPECT_EQ(result, 42);
}

TEST(TaskTest, RunVoidTask) {
    bool flag = false;
    coro::run(flag_setter(&flag));
    EXPECT_TRUE(flag);
}

TEST(TaskTest, RunPropagatesException) {
    bool caught = false;
    try {
        coro::run(thrower_int()); // 异常从 take_result 重新抛出
    } catch (const std::runtime_error&) {
        caught = true;
    }
    EXPECT_TRUE(caught);
}

// ============================================================================
// Task 误用防护 (回归: 重复 co_await / move 后 co_await / 双等待者)
//
// 旧实现这些路径是静默 UB: 二次 co_await 已完成任务返回被移空的值
// (move-only 类型得到空指针), move 后 co_await 在 NDEBUG 下解引用空句柄,
// 第二个等待者覆盖 continuation 让第一个永久挂起。现在全部显式抛异常。
// ============================================================================

namespace {

    coro::Task<> double_await_inner(bool* threw, int* first_value) {
        auto make = []() -> coro::Task<int> { co_return 42; };
        auto t = make();
        *first_value = co_await t; // 第一次: 正常拿到 42
        try {
            int second = co_await t; // 二次 co_await 已完成的 Task
            (void)second;
        } catch (const std::logic_error&) {
            *threw = true;
        }
    }

    coro::Task<> moved_await_inner(bool* threw) {
        auto make = []() -> coro::Task<int> { co_return 1; };
        auto t = make();
        auto t2 = std::move(t);
        try {
            int v = co_await t; // moved-from Task: 旧实现 NDEBUG 下解引用空句柄
            (void)v;
        } catch (const std::runtime_error&) {
            *threw = true; // logic_error 与 runtime_error 互不继承, 不会误捕
        }
        co_await std::move(t2); // 正常消耗掉, 避免析构销毁未启动任务
    }

    coro::Task<int> await_by_ref(coro::Task<int>& t) {
        co_return co_await t;
    }

    coro::Task<> second_waiter_inner(bool* threw) {
        auto slow = []() -> coro::Task<int> {
            co_await coro::sleep(50ms);
            co_return 7;
        };
        auto t = coro::spawn(slow());          // 已启动
        auto w = coro::spawn(await_by_ref(t)); // 第一个等待者 (持引用, 不 move)
        co_await coro::yield();                // w 已设置 continuation 并挂起
        try {
            co_await t; // 第二个等待者: 不得覆盖 continuation
        } catch (const std::logic_error&) {
            *threw = true;
        }
        co_await std::move(w);
    }

} // namespace

TEST(TaskTest, DoubleAwaitCompletedTaskThrows) {
    bool threw = false;
    int first_value = 0;
    test_util::run_task([&] { return double_await_inner(&threw, &first_value); });
    EXPECT_EQ(first_value, 42) << "第一次 co_await 应正常返回结果";
    EXPECT_TRUE(threw) << "二次 co_await 已完成的 Task 必须报错, 不能返回被移空的值";
}

TEST(TaskTest, AwaitMovedFromTaskThrows) {
    bool threw = false;
    test_util::run_task([&] { return moved_await_inner(&threw); });
    EXPECT_TRUE(threw) << "co_await moved-from Task 必须报错, 不能解引用空句柄";
}

TEST(TaskTest, SecondAwaiterOnRunningTaskThrows) {
    bool threw = false;
    test_util::run_task([&] { return second_waiter_inner(&threw); });
    EXPECT_TRUE(threw) << "第二个等待者 co_await 未完成任务必须报错, 不能覆盖 continuation";
}
