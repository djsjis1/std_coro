// test_future.cpp — Promise/Future: 多等待者 / 跨线程 / 异常 / 重复 set
#include <gtest/gtest.h>

#include <coro/coro.hpp>

#include "test_util.h"

#include <thread>

using namespace std::chrono_literals;

namespace
{

    // ── 命名协程函数 ──

    // 两个协程等待同一个 Future, 必须都被唤醒 (回归: 旧实现单 continuation 覆盖)
    coro::Task<int> future_waiter(coro::Future<int> *f, int id, int *out1, int *out2)
    {
        int v = co_await *f;
        if (id == 1)
            *out1 = v;
        else
            *out2 = v;
        co_return v;
    }

    coro::Task<> multi_waiter_scenario(int *o1, int *o2)
    {
        coro::Promise<int> p;
        auto f = p.get_future();

        coro::Future<int> f1 = f; // Future 可拷贝 (共享状态)
        auto w1 = coro::spawn(future_waiter(&f, 1, o1, o2));
        auto w2 = coro::spawn(future_waiter(&f1, 2, o1, o2));
        co_await coro::yield(); // 两个等待者都挂上
        p.set_value(42);
        co_await std::move(w1);
        co_await std::move(w2);
    }

    // 提前 set_value: await 立即返回 (快速路径)
    coro::Task<> ready_future(int *out)
    {
        coro::Promise<int> p;
        auto f = p.get_future();
        p.set_value(7);
        *out = co_await f; // 不挂起
    }

    // set_exception: await_resume 重新抛出
    coro::Task<> exception_future(bool *caught)
    {
        coro::Promise<int> p;
        auto f = p.get_future();
        p.set_exception(std::make_exception_ptr(std::runtime_error("f boom")));
        try
        {
            (void)co_await f;
        }
        catch (const std::runtime_error &)
        {
            *caught = true;
        }
    }

    // 跨线程 set_value: 工作线程完成 Promise, 事件循环被唤醒
    coro::Task<> cross_thread_scenario(int *out)
    {
        coro::Promise<int> p;
        auto f = p.get_future();

        std::thread worker([p = std::move(p)]() mutable
                           {
            std::this_thread::sleep_for(20ms);
            p.set_value(99); });

        *out = co_await f; // 挂起直到工作线程 set_value
        worker.join();
    }

    // 重复 set 抛 logic_error
    coro::Task<> double_set_scenario(bool *threw)
    {
        coro::Promise<int> p;
        auto f = p.get_future();
        p.set_value(1);
        try
        {
            p.set_value(2);
        }
        catch (const std::logic_error &)
        {
            *threw = true;
        }
        (void)co_await f;
    }

} // namespace

TEST(FutureTest, MultipleWaitersAllWoken)
{
    int o1 = 0, o2 = 0;
    test_util::run_task([&]
                        { return multi_waiter_scenario(&o1, &o2); });
    EXPECT_EQ(o1, 42);
    EXPECT_EQ(o2, 42);
}

TEST(FutureTest, PresetValueSkipsSuspension)
{
    int out = 0;
    test_util::run_task([&]
                        { return ready_future(&out); });
    EXPECT_EQ(out, 7);
}

TEST(FutureTest, ExceptionPropagates)
{
    bool caught = false;
    test_util::run_task([&]
                        { return exception_future(&caught); });
    EXPECT_TRUE(caught);
}

TEST(FutureTest, CrossThreadSetValue)
{
    int out = 0;
    test_util::run_task([&]
                        { return cross_thread_scenario(&out); });
    EXPECT_EQ(out, 99);
}

TEST(FutureTest, DoubleSetThrows)
{
    bool threw = false;
    test_util::run_task([&]
                        { return double_set_scenario(&threw); });
    EXPECT_TRUE(threw);
}
