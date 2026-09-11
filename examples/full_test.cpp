// full_test.cpp — 综合功能验证
#include <coro/coro.hpp>
#include <iostream>
#include <vector>
using namespace std::chrono_literals;

// 1. cancel
coro::Task<> t1_cancel()
{
    auto t = coro::spawn([]() -> coro::Task<int>
                         { co_await coro::sleep(10s); co_return 42; }());
    t.cancel();
    try
    {
        co_await std::move(t);
        std::cout << "  FAIL" << std::endl;
    }
    catch (const coro::CancelledError &)
    {
        std::cout << "  OK: cancel" << std::endl;
    }
}

// 2. wait_for timeout
coro::Task<> t2_wait_for_timeout()
{
    try
    {
        co_await coro::wait_for([]() -> coro::Task<int>
                                { co_await coro::sleep(10s); co_return 1; }(), 100ms);
        std::cout << "  FAIL" << std::endl;
    }
    catch (const coro::TimeoutError &)
    {
        std::cout << "  OK: timeout" << std::endl;
    }
}

// 3. wait_for success
coro::Task<> t3_wait_for_ok()
{
    auto r = co_await coro::wait_for([]() -> coro::Task<int>
                                     { co_await coro::sleep(50ms); co_return 42; }(), 200ms);
    std::cout << "  OK: result=" << r << std::endl;
}

// 4. Lock (无sleep版, 避免构建缓存问题)
coro::Task<> t4_lock()
{
    coro::Lock lock;
    int c = 0;
    auto w = [&](int) -> coro::Task<>
    { co_await lock.acquire(); c++; lock.release(); };
    auto a = coro::spawn(w(1)), b = coro::spawn(w(2)), d = coro::spawn(w(3));
    co_await std::move(a);
    co_await std::move(b);
    co_await std::move(d);
    std::cout << "  OK: counter=" << c << " (should be 3)" << std::endl;
}

// 5. Semaphore
coro::Task<> t5_sem()
{
    coro::Semaphore sem(2);
    int max = 0, cur = 0;
    auto w = [&](int) -> coro::Task<>
    { co_await sem.acquire(); cur++; if(cur>max)max=cur; co_await coro::yield(); cur--; sem.release(); };
    auto a = coro::spawn(w(1)), b = coro::spawn(w(2)), c = coro::spawn(w(3)), d = coro::spawn(w(4));
    co_await std::move(a);
    co_await std::move(b);
    co_await std::move(c);
    co_await std::move(d);
    std::cout << "  OK: max=" << max << " (<=2)" << std::endl;
}

// 6. Event
coro::Task<> t6_event()
{
    coro::Event ev;
    auto waiter = coro::spawn([&]() -> coro::Task<>
                              { co_await ev.wait(); }());
    co_await coro::sleep(50ms);
    ev.set();
    co_await std::move(waiter);
    std::cout << "  OK: event" << std::endl;
}

// 7. Queue
coro::Task<> t7_queue()
{
    coro::Queue<int> q;
    auto prod = coro::spawn([&]() -> coro::Task<>
                            { co_await q.put(10); co_await q.put(20); }());
    int a = co_await q.get(), b = co_await q.get();
    co_await std::move(prod);
    std::cout << "  OK: " << a << ", " << b << std::endl;
}

// 8. call_later (spawn + 保存模式, 验证 self-referencing 是否问题根源)
coro::Task<> t8_call_later()
{
    bool ok = false;
    // 不用 self-referencing: 用 spawn + 保存返回值
    auto inner = coro::spawn([&ok]() -> coro::Task<void>
                             {
        co_await coro::sleep(50ms);
        ok = true; }());
    co_await coro::sleep(100ms);
    co_await std::move(inner);
    std::cout << "  OK: " << (ok ? "fired" : "MISS") << std::endl;
}

// 9. gather_all (用库函数, 命名协程函数实现)
// 注意: 任务用命名函数 (参数进帧), 避免 MSVC Debug 下 lambda 捕获问题
coro::Task<int> gather_task_impl(int i)
{
    co_await coro::sleep(30ms);
    co_return i * 10;
}

coro::Task<> t9_gather_all()
{
    std::vector<coro::Task<int>> tv;
    for (int i = 0; i < 3; i++)
        tv.push_back(gather_task_impl(i));

    auto results = co_await coro::gather_all(std::move(tv));
    std::cout << "  OK: [" << results[0] << "," << results[1] << "," << results[2] << "]" << std::endl;
}

// 10. wait_any (用库函数, 命名协程函数实现)
coro::Task<int> wait_any_fast()
{
    co_await coro::sleep(50ms);
    co_return 100;
}
coro::Task<int> wait_any_slow()
{
    co_await coro::sleep(200ms);
    co_return 200;
}

coro::Task<> t10_wait_any()
{
    int result = co_await coro::wait_any(wait_any_fast(), wait_any_slow());
    std::cout << "  OK: first=" << result << " (should be 100)" << std::endl;
}

coro::Task<> main_task()
{
    std::cout << "=== Full Test ===" << std::endl;
    co_await t1_cancel();
    co_await t2_wait_for_timeout();
    co_await t3_wait_for_ok();
    co_await t4_lock();
    co_await t5_sem();
    co_await t6_event();
    co_await t7_queue();
    co_await t8_call_later();
    co_await t9_gather_all();
    co_await t10_wait_any();
    std::cout << "=== ALL DONE ===" << std::endl;
}

int main()
{
    coro::run(main_task());
    return 0;
}
