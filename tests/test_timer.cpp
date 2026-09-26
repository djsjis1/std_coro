// test_timer.cpp — coro::Timer 的可取消/可重置合同 (计划 M3 第 2 项)
#include <gtest/gtest.h>

#include <coro/timer.hpp>

#include "test_util.h"

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>

#if defined(CORO_HAS_CONCURRENCY_EXT) && CORO_HAS_CONCURRENCY_EXT

using namespace std::chrono_literals;
using clk = std::chrono::steady_clock;

namespace {

    // 等待一次的命名协程: 避免临时协程 lambda 的闭包在启动后被销毁
    coro::Task<> wait_once(coro::Timer* timer, bool* value) {
        *value = co_await timer->wait();
        co_return;
    }

    // 等到期: 断言返回 true 且确实等了这么久
    coro::Task<bool> await_expiry(bool* value) {
        coro::Timer timer{40ms};
        const auto begin = clk::now();
        *value = co_await timer.wait();
        const auto cost = std::chrono::duration_cast<std::chrono::milliseconds>(clk::now() - begin).count();
        co_return cost >= 30; // 允许少量调度误差, 但绝不能提前返回
    }

    // cancel 唤醒: 返回 false, 且不等到原期限
    coro::Task<bool> await_cancelled(bool* value, int* cost_ms) {
        coro::Timer timer{10s}; // 远超测试允许等待的时间
        const auto begin = clk::now();
        auto waiter = coro::spawn(wait_once(&timer, value));
        co_await coro::yield();
        timer.cancel();
        co_await std::move(waiter);
        *cost_ms = static_cast<int>(std::chrono::duration_cast<std::chrono::milliseconds>(clk::now() - begin).count());
        co_return true;
    }

    // 跨线程 cancel: 唤醒必须回到等待者所属循环, 因此不能挂死
    coro::Task<bool> await_cross_thread_cancel(bool* value) {
        auto timer = std::make_shared<coro::Timer>(10s);
        auto* loop = &coro::EventLoop::get();
        auto waiter = coro::spawn(wait_once(timer.get(), value));
        co_await coro::yield();
        std::thread killer([timer, loop] {
            std::this_thread::sleep_for(20ms);
            loop->dispatch([timer] { timer->cancel(); });
        });
        co_await std::move(waiter);
        killer.join();
        co_return true;
    }

    // reset 迁移期限: 等待者不结束, 只换到期时刻
    coro::Task<bool> await_reset_migrates_deadline(bool* value) {
        coro::Timer timer{30ms};
        const auto begin = clk::now();
        auto waiter = coro::spawn(wait_once(&timer, value));
        co_await coro::yield();
        for (int i = 0; i < 5; ++i) {
            timer.reset(30ms); // 每次都往后推, 期间不该到期
            co_await coro::sleep(20ms);
        }
        co_await std::move(waiter);
        const auto cost = std::chrono::duration_cast<std::chrono::milliseconds>(clk::now() - begin).count();
        co_return cost >= 105; // 5 轮 20ms 的推迟必须体现出来 (旧阈值 150 算错了时序)
    }

    // 析构唤醒等待者: 不留永久挂起的协程
    // Timer 对象先于它的等待者销毁: 析构必须唤醒等待者。
    // 等待方持有裸指针是安全的 —— 挂起后的协程不再访问 Timer 对象本身,
    // awaiter 与 state 的 shared_ptr 生命周期独立于 Timer。
    coro::Task<bool> await_destructor_wakes_waiter(bool* value) {
        auto holder = std::make_unique<coro::Timer>(10s);
        coro::Timer* raw = holder.get();
        auto waiter = coro::spawn(wait_once(raw, value));
        co_await coro::yield();
        holder.reset(); // Timer 析构, 等待者仍挂着
        co_await std::move(waiter);
        co_return true;
    }

    // cancel 先于 wait: 等待者直接拿到已定的终态, 不会挂起
    coro::Task<bool> await_after_cancel(bool* value) {
        coro::Timer timer{10s};
        timer.cancel();
        *value = co_await timer.wait();
        co_return true;
    }

    // 两个等待者必须被拒绝, 而不是第二个静默吞掉第一个的唤醒
    coro::Task<bool> await_second_waiter_rejected(bool* threw) {
        coro::Timer timer{40ms};
        bool ignored = false;
        auto first = coro::spawn(wait_once(&timer, &ignored));
        co_await coro::yield();
        try {
            co_await timer.wait();
        } catch (const coro::StructuredConcurrencyError&) {
            *threw = true;
        }
        co_await std::move(first);
        co_return true;
    }

    coro::Task<> expiry_scenario(bool* value, bool* late) {
        *late = co_await await_expiry(value);
        co_return;
    }

    coro::Task<> reset_scenario(bool* value, bool* honoured) {
        *honoured = co_await await_reset_migrates_deadline(value);
        co_return;
    }

    // 周期用法: 上一轮结束之后, reset 必须让下一次 wait 真的再等一轮
    coro::Task<> periodic_via_reset(std::atomic<int>* ticks) {
        coro::Timer timer{20ms};
        for (int i = 0; i < 3; ++i) {
            const bool expired = co_await timer.wait();
            if (!expired)
                co_return;
            ticks->fetch_add(1);
            timer.reset(20ms); // 重新开局, 下一轮真的再等 20ms
        }
        co_return;
    }

} // namespace

TEST(TimerTest, WaitReturnsTrueOnExpiry) {
    bool value = false;
    bool late_enough = false;
    test_util::run_task([&] { return expiry_scenario(&value, &late_enough); });
    EXPECT_TRUE(value) << "到期时 wait() 应返回 true";
    EXPECT_TRUE(late_enough) << "wait() 提前返回, 定时器没有真的等待";
}

TEST(TimerTest, CancelWakesWaiterWithFalse) {
    bool value = true;
    int cost = 0;
    test_util::run_task([&] { return await_cancelled(&value, &cost); });
    EXPECT_FALSE(value) << "被 cancel 唤醒时 wait() 应返回 false";
    EXPECT_LT(cost, 1000) << "没有立即唤醒, 耗时 " << cost << "ms";
}

TEST(TimerTest, CrossThreadCancelWakesOnOwnerLoop) {
    bool value = true;
    test_util::run_task([&] { return await_cross_thread_cancel(&value); });
    EXPECT_FALSE(value) << "跨线程 cancel 未把唤醒投递回等待者所属循环";
}

TEST(TimerTest, ResetMigratesDeadlineWithoutEndingWait) {
    bool value = false;
    bool honoured = false;
    test_util::run_task([&] { return reset_scenario(&value, &honoured); });
    EXPECT_TRUE(value) << "reset 之后的到期应当正常完成等待";
    EXPECT_TRUE(honoured) << "reset 没有把 deadline 往后迁移";
}

TEST(TimerTest, DestructorCancelsPendingWaiter) {
    bool value = true;
    test_util::run_task([&] { return await_destructor_wakes_waiter(&value); });
    EXPECT_FALSE(value) << "Timer 析构必须唤醒还在等待的协程, 不能留下永久挂起";
}

TEST(TimerTest, CancelBeforeWaitResolvesImmediately) {
    bool value = true;
    test_util::run_task([&] { return await_after_cancel(&value); });
    EXPECT_FALSE(value);
}

TEST(TimerTest, SecondWaiterIsRejectedNotSilentlyLost) {
    bool threw = false;
    test_util::run_task([&] { return await_second_waiter_rejected(&threw); });
    EXPECT_TRUE(threw) << "重复 wait() 应抛 StructuredConcurrencyError";
}

// 回归: 一次性 Timer 到期后若 reset 不重新开局, 用户按"循环 reset 表达周期"
// 的写法会退化成满 CPU 的空转 (示例程序里实测到百万次打点)。
TEST(TimerTest, ResetAfterExpiryRearmsForNextWait) {
    std::atomic<int> ticks{0};
    const auto begin = clk::now();
    test_util::run_task([&] { return periodic_via_reset(&ticks); });
    const auto cost = std::chrono::duration_cast<std::chrono::milliseconds>(clk::now() - begin).count();
    EXPECT_EQ(ticks.load(), 3);
    EXPECT_GE(cost, 50) << "reset 后没有真的再等, 周期用法退化成空转";
}

#endif // CORO_HAS_CONCURRENCY_EXT
