// ============================================================================
// basic.cpp - basic usage: sleep + serial await
// ============================================================================

#include <coro/coro.hpp>

#include <chrono>
#include <iostream>
#include <string>

using namespace std::chrono_literals;

// Simulate a slow computation
coro::Task<int> compute_async(int id, int delay_ms) {
    std::cout << "  [" << id << "] computing (needs " << delay_ms << "ms)..." << std::endl;
    co_await coro::sleep(std::chrono::milliseconds(delay_ms));
    std::cout << "  [" << id << "] done!" << std::endl;
    co_return id * 100;
}

// Simulate a network request
coro::Task<std::string> fetch_data(const std::string& url, int delay_ms) {
    std::cout << "  [fetch] requesting " << url << " ..." << std::endl;
    co_await coro::sleep(std::chrono::milliseconds(delay_ms));
    std::cout << "  [fetch] " << url << " returned data" << std::endl;
    co_return "{\"data\": \"from " + url + "\"}";
}

// Main coroutine: serial await
coro::Task<> main_task() {
    std::cout << "=== Basic Example: serial await ===" << std::endl << std::endl;

    // Serial await (like Python: await coro())
    std::cout << "1. Serial computation:" << std::endl;
    int a = co_await compute_async(1, 300);
    int b = co_await compute_async(2, 200);
    std::cout << "   results: a=" << a << ", b=" << b << std::endl << std::endl;

    std::cout << "2. Serial requests:" << std::endl;
    auto r1 = co_await fetch_data("/api/users", 400);
    auto r2 = co_await fetch_data("/api/posts", 300);
    std::cout << "   r1: " << r1 << std::endl;
    std::cout << "   r2: " << r2 << std::endl << std::endl;

    // spawn + await (like asyncio.create_task)
    std::cout << "3. spawn + await (like create_task):" << std::endl;
    auto t1 = coro::spawn(compute_async(3, 500));
    auto t2 = coro::spawn(compute_async(4, 400));

    // t1, t2 are already running in the background
    int v1 = co_await std::move(t1);
    int v2 = co_await std::move(t2);
    std::cout << "   results: v1=" << v1 << ", v2=" << v2 << std::endl;

    std::cout << std::endl << "=== Basic example done ===" << std::endl;
}

int main() {
    coro::run(main_task());
    return 0;
}
