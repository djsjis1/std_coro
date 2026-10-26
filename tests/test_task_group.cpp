// test_task_group.cpp — TaskGroup 结构化并发: 成功 / 失败取消 / 聚合 / 空组 / 兜底
#include <gtest/gtest.h>

#include <coro/coro.hpp>

#include "test_util.h"

using namespace std::chrono_literals;

namespace {

    // ── 命名协程函数 ──

    coro::Task<> worker_sleep(int ms, int* ran) {
        co_await coro::sleep(std::chrono::milliseconds(ms));
        *ran += 1;
        co_return;
    }

    coro::Task<> failing_task(int ms, const char* msg) {
        co_await coro::sleep(std::chrono::milliseconds(ms));
        throw std::runtime_error(msg);
        co_return;
    }

    // 带清理观察: 被组取消时必须执行清理 (验证取消传播到原任务)
    coro::Task<> cancel_observable(int* cleaned) {
        struct Guard {
            int* p;
            ~Guard() { *p = 1; }
        } g{cleaned};
        co_await coro::sleep(10s); // 长睡眠, 被组取消
        co_return;
    }

    // ── 场景 1: 全部成功 ──
    coro::Task<> all_succeed(int* count) {
        coro::TaskGroup group;
        group.spawn(worker_sleep(10, count));
        group.spawn(worker_sleep(20, count));
        group.spawn(worker_sleep(5, count));
        co_await group.wait();
    }

    // ── 场景 2: 一个失败 → 取消其余 → ExceptionGroup(1 个异常) ──
    coro::Task<> one_fails(int* exception_count, int* cleaned) {
        int dummy = 0; // 15ms worker 的输出位置 (不可传 nullptr)
        coro::TaskGroup group;
        group.spawn(failing_task(5, "boom"));
        group.spawn(cancel_observable(cleaned)); // 应被组取消 (清理执行)
        group.spawn(worker_sleep(15, &dummy));
        try {
            co_await group.wait();
        } catch (const coro::ExceptionGroup& eg) {
            *exception_count = (int)eg.exceptions().size();
        }
    }

    // ── 场景 3: 多个失败 → ExceptionGroup 聚合多个 ──
    // 注意: 三个任务用相同 deadline (同时失败)。若错开 deadline,
    // 第一个失败会取消其余 (正确语义), 聚合数就只剩 1。
    coro::Task<> multi_fail(int* exception_count) {
        coro::TaskGroup group;
        group.spawn(failing_task(5, "first"));
        group.spawn(failing_task(5, "second"));
        group.spawn(failing_task(5, "third"));
        try {
            co_await group.wait();
        } catch (const coro::ExceptionGroup& eg) {
            *exception_count = (int)eg.exceptions().size();
        }
    }

    // ── 场景 4: 空组 wait 立即返回 ──
    coro::Task<> empty_group(bool* returned) {
        coro::TaskGroup group;
        co_await group.wait(); // 不挂起
        *returned = true;
    }

    // ── 场景 5: 混合返回类型子任务 ──
    coro::Task<int> typed_worker(int v) {
        co_await coro::sleep(5ms);
        co_return v;
    }

    coro::Task<> mixed_types(bool* done) {
        coro::TaskGroup group;
        group.spawn(typed_worker(1)); // Task<int>
        int void_done = 0;
        group.spawn(worker_sleep(5, &void_done)); // Task<void>
        group.spawn(typed_worker(3));             // Task<int>
        co_await group.wait();
        *done = (void_done == 1);
    }

    // ── 场景 6: 忘记 wait, 析构兜底取消残留子任务 ──
    coro::Task<> forgotten_wait(int* cleaned) {
        {
            coro::TaskGroup group;
            group.spawn(cancel_observable(cleaned)); // 10s 睡眠, 不会被 wait
            co_await coro::sleep(5ms);               // 让任务真正跑起来挂到 sleep 上
        } // 析构: 自动取消 → 任务被唤醒 → Guard 析构 (清理执行)
        // 给取消传播一点时间
        co_await coro::sleep(20ms);
    }

    // ── 场景 7: 失败后 wait 抛出后组进入终态, 不再有残留 ──
    coro::Task<> post_failure_state(bool* group_done) {
        coro::TaskGroup group;
        group.spawn(failing_task(5, "x"));
        try {
            co_await group.wait();
        } catch (const coro::ExceptionGroup&) {
        }
        *group_done = group.done();
    }

    // ── 场景 8: 父任务在 wait() 上被取消 → 先等子任务清理完再传播取消 ──
    struct scope_probe {
        static std::atomic<int> alive;
        scope_probe() { alive.fetch_add(1); }
        ~scope_probe() { alive.fetch_sub(1); }
    };
    std::atomic<int> scope_probe::alive{0};

    /// 子任务收到取消后还要异步等一段才完成清理
    coro::Task<> slow_cleanup_child(std::atomic<int>* alive_during_cleanup) {
        bool cancelled = false;
        try {
            co_await coro::sleep(10s);
        } catch (const coro::CancelledError&) {
            cancelled = true; // MSVC 不允许在 catch 块内 co_await
        }
        if (cancelled) {
            co_await coro::sleep(20ms); // 真实的异步清理窗口
            *alive_during_cleanup = scope_probe::alive.load();
        }
    }

    coro::Task<> cancelled_parent_waits(std::atomic<int>* alive_during_cleanup, bool* cleaned) {
        scope_probe probe; // 父作用域资源: 子任务清理期间必须仍然存活
        coro::TaskGroup group;
        group.spawn(slow_cleanup_child(alive_during_cleanup));
        co_await group.wait(); // 父任务在此被取消: 必须等子任务退出后才恢复
        *cleaned = true;
    }

    /// 在父任务确实挂起之后才发起取消 (避免“还没进入 wait 就 cancel”的无效时序)
    coro::Task<> cancel_parent_later(coro::Task<>* parent, int delay_ms) {
        co_await coro::sleep(std::chrono::milliseconds(delay_ms));
        parent->cancel();
    }

} // namespace

TEST(TaskGroupTest, CancelledParentWaitsForChildCleanup) {
    std::atomic<int> alive_during_cleanup{-1};
    bool cleaned = false;
    auto parent = cancelled_parent_waits(&alive_during_cleanup, &cleaned);
    parent.start();
    auto killer = coro::spawn(cancel_parent_later(&parent, 10));
    coro::EventLoop::get().run();
    try {
        parent.take_result();
    } catch (const coro::CancelledError&) {
    }
    EXPECT_FALSE(cleaned) << "父任务在取消路径上不应走到 wait() 之后";
    EXPECT_EQ(alive_during_cleanup.load(), 1) << "子任务清理时父作用域资源不应已被销毁";
    EXPECT_EQ(scope_probe::alive.load(), 0) << "父帧结束后资源应已释放";
}

TEST(TaskGroupTest, SpawnAfterChildFailureIsRejected) {
    coro::TaskGroup group;
    bool rejected = false;
    int ran = 0;
    auto run = [&]() -> coro::Task<> {
        group.spawn(failing_task(1, "boom"));
        co_await coro::sleep(20ms); // 等子任务真的失败, 组转入 closing
        try {
            group.spawn(worker_sleep(1, &ran));
        } catch (const coro::StructuredConcurrencyError&) {
            rejected = true;
        }
        try {
            co_await group.wait();
        } catch (const coro::ExceptionGroup&) {
        }
    };
    auto t = run();
    test_util::run_task([&] { return std::move(t); });
    EXPECT_TRUE(rejected) << "首次失败后组必须关闭准入";
}

TEST(TaskGroupTest, AllSucceed) {
    int count = 0;
    test_util::run_task([&] { return all_succeed(&count); });
    EXPECT_EQ(count, 3); // 三个任务都跑完
}

TEST(TaskGroupTest, OneFailsCancelsOthers) {
    int exception_count = 0, cleaned = 0;
    test_util::run_task([&] { return one_fails(&exception_count, &cleaned); });
    EXPECT_EQ(exception_count, 1); // 单个异常也打包成 ExceptionGroup
    EXPECT_EQ(cleaned, 1);         // 其余任务被取消 → 清理逻辑执行
}

TEST(TaskGroupTest, MultiFailAggregates) {
    int exception_count = 0;
    test_util::run_task([&] { return multi_fail(&exception_count); });
    EXPECT_EQ(exception_count, 3); // 三个异常全部聚合
}

TEST(TaskGroupTest, EmptyGroupImmediateReturn) {
    bool returned = false;
    test_util::run_task([&] { return empty_group(&returned); });
    EXPECT_TRUE(returned);
}

TEST(TaskGroupTest, MixedReturnTypes) {
    bool done = false;
    test_util::run_task([&] { return mixed_types(&done); });
    EXPECT_TRUE(done);
}

TEST(TaskGroupTest, ForgottenWaitDestructorCancels) {
    int cleaned = 0;
    test_util::run_task([&] { return forgotten_wait(&cleaned); });
    EXPECT_EQ(cleaned, 1); // 析构兜底取消, 子任务清理执行
}

TEST(TaskGroupTest, GroupDoneAfterWait) {
    bool group_done = false;
    test_util::run_task([&] { return post_failure_state(&group_done); });
    EXPECT_TRUE(group_done); // wait 后组进入终态
}

// ============================================================================
// 组状态机 (计划 C3): open -> closing -> finished
// ============================================================================

namespace {

    coro::Task<int> group_sleep_then(int ms, int value, std::atomic<int>* hits) {
        co_await coro::sleep(std::chrono::milliseconds(ms));
        hits->fetch_add(1);
        co_return value;
    }

    coro::Task<> never_ends(std::atomic<int>* hits) {
        co_await coro::sleep(30s);
        hits->fetch_add(1);
        co_return;
    }

    // wait() 之后仍 spawn: 必须被拒绝
    coro::Task<> spawn_after_wait_throws(bool* rejected) {
        coro::TaskGroup group;
        std::atomic<int> hits{0};
        group.spawn(group_sleep_then(1, 1, &hits));
        co_await group.wait();
        try {
            group.spawn(group_sleep_then(1, 2, &hits));
        } catch (const coro::StructuredConcurrencyError&) {
            *rejected = true;
        }
        co_return;
    }

    // 重复 wait(): 第二个 waiter 会让第一个永久挂起, 因此直接拒绝
    coro::Task<> double_wait_throws(bool* rejected) {
        coro::TaskGroup group;
        std::atomic<int> hits{0};
        group.spawn(group_sleep_then(1, 1, &hits));
        co_await group.wait();
        try {
            co_await group.wait();
        } catch (const coro::StructuredConcurrencyError&) {
            *rejected = true;
        }
        co_return;
    }

    // cancel() 后仍能 wait() 收尾; 长任务确实被取消唤醒
    coro::Task<> cancel_then_wait_drains(std::atomic<int>* hits, bool* cancelled_out, int* ms_cost) {
        coro::TaskGroup group;
        group.spawn(never_ends(hits));
        group.spawn(never_ends(hits));
        const auto begin = std::chrono::steady_clock::now();
        co_await coro::yield(); // 让子任务真正进入 sleep
        group.cancel();
        try {
            co_await group.wait();
        } catch (const coro::ExceptionGroup&) {
            // 组内取消不应被聚合, 走到这里说明语义变了
            *cancelled_out = false;
            co_return;
        } catch (const coro::CancelledError&) {
        }
        *cancelled_out = true;
        *ms_cost = static_cast<int>(
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - begin).count());
        co_return;
    }

    // accepts_new_tasks() 反映真实阶段
    coro::Task<> phase_visible_through_api(bool* open_before, bool* open_after) {
        coro::TaskGroup group;
        *open_before = group.accepts_new_tasks();
        co_await group.wait(); // 空组: 立即完成
        *open_after = group.accepts_new_tasks();
        co_return;
    }

} // namespace

TEST(TaskGroupStateMachineTest, SpawnAfterWaitIsRejected) {
    bool rejected = false;
    test_util::run_task([&] { return spawn_after_wait_throws(&rejected); });
    EXPECT_TRUE(rejected) << "wait() 之后 spawn 应抛 StructuredConcurrencyError";
}

TEST(TaskGroupStateMachineTest, SecondWaitIsRejected) {
    bool rejected = false;
    test_util::run_task([&] { return double_wait_throws(&rejected); });
    EXPECT_TRUE(rejected) << "重复 wait() 会让第一个 waiter 永久挂起, 必须拒绝";
}

TEST(TaskGroupStateMachineTest, CancelStillAllowsWaitToFinish) {
    std::atomic<int> hits{0};
    bool ok = false;
    int cost = 0;
    test_util::run_task([&] { return cancel_then_wait_drains(&hits, &ok, &cost); });
    EXPECT_TRUE(ok);
    EXPECT_EQ(hits.load(), 0) << "被取消的长任务不该跑到完成";
    EXPECT_LT(cost, 2000) << "cancel 后 wait 未被关闭唤醒, 耗时 " << cost << "ms";
}

TEST(TaskGroupStateMachineTest, AcceptsNewTasksTracksPhase) {
    bool open_before = false, open_after = true;
    test_util::run_task([&] { return phase_visible_through_api(&open_before, &open_after); });
    EXPECT_TRUE(open_before);
    EXPECT_FALSE(open_after);
}
