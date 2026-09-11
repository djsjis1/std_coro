// ============================================================================
// gather.cpp - concurrent gather: await multiple tasks in parallel
// ============================================================================

#include <coro/coro.hpp>

#include <chrono>
#include <iostream>
#include <string>

using namespace std::chrono_literals;

// Simulate an API call
coro::Task<std::string> api_call(const std::string &name, int delay_ms)
{
    co_await coro::sleep(std::chrono::milliseconds(delay_ms));
    co_return "[" + name + ": OK]";
}

// Simulate an API call that may throw
coro::Task<int> risky_call(int id, int delay_ms, bool should_fail)
{
    co_await coro::sleep(std::chrono::milliseconds(delay_ms));
    if (should_fail)
    {
        throw std::runtime_error("call " + std::to_string(id) + " failed!");
    }
    co_return id * 10;
}

// Main coroutine: gather
coro::Task<> main_task()
{
    std::cout << "=== Gather Example ===" << std::endl
              << std::endl;

    // 1. Basic gather: three tasks with different return types run concurrently
    std::cout << "1. Concurrent API calls (300ms, 200ms, 100ms):" << std::endl;
    auto [r1, r2, r3] = co_await coro::gather(
        api_call("users", 300),
        api_call("posts", 200),
        api_call("photos", 100));
    std::cout << "   results: " << r1 << " " << r2 << " " << r3 << std::endl;
    std::cout << "   (total ~300ms, not 300+200+100=600ms)" << std::endl
              << std::endl;

    // 2. Mix string and int return types
    std::cout << "2. Mixed string and int types:" << std::endl;
    auto [name, value] = co_await coro::gather(
        api_call("status", 250),
        risky_call(42, 350, false));
    std::cout << "   name=" << name << ", value=" << value << std::endl
              << std::endl;

    // 3. Exception handling: one task fails, wait for others, rethrow
    std::cout << "3. Exception handling (one task fails):" << std::endl;
    try
    {
        auto [a, b, c] = co_await coro::gather(
            risky_call(1, 100, false),
            risky_call(2, 200, true), // this one throws
            risky_call(3, 150, false));
        std::cout << "   (never reached)" << std::endl;
    }
    catch (const std::exception &e)
    {
        std::cout << "   caught: " << e.what() << std::endl;
    }

    std::cout << std::endl
              << "=== Gather example done ===" << std::endl;
}

int main()
{
    coro::run(main_task());
    return 0;
}
