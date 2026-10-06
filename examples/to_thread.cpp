// ============================================================================
// to_thread.cpp - offload blocking/CPU-heavy work to the thread pool
// ============================================================================
//
// co_await coro::to_thread(func) 对标 Python asyncio.to_thread():
//   - func 在线程池的工作线程上执行 (默认 hardware_concurrency 个线程);
//   - 协程在 await 处让出事件循环线程, 不阻塞其他协程;
//   - func 的返回值成为 co_await 的结果, 抛出的异常在 co_await 处重新抛出。

#include <coro/coro.hpp>

#include <chrono>
#include <iostream>
#include <thread>

using namespace std::chrono_literals;

// CPU 密集计算: 如果直接在协程里跑会卡死整个事件循环
long long cpu_fib(int n) {
    return n < 2 ? n : cpu_fib(n - 1) + cpu_fib(n - 2);
}

// 模拟阻塞式 I/O (旧同步库的接口)
int blocking_read(int delay_ms) {
    std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
    return 42;
}

coro::Task<> heartbeat() {
    // 事件循环未被阻塞的证据: 心跳协程持续运行
    for (int i = 0; i < 8; ++i) {
        co_await coro::sleep(50ms);
        std::cout << "  [heartbeat] tick " << i << std::endl;
    }
}

coro::Task<> main_task() {
    std::cout << "=== to_thread example: offload blocking work ===" << std::endl << std::endl;

    auto beat = coro::spawn(heartbeat());

    // 1. CPU 密集计算卸载到线程池
    std::cout << "1. CPU-heavy fib(35) offloaded to thread pool..." << std::endl;
    long long r = co_await coro::to_thread([] { return cpu_fib(35); });
    std::cout << "   fib(35) = " << r << std::endl;

    // 2. 阻塞式调用桥接 (旧同步 API)
    std::cout << "2. Blocking call bridged:" << std::endl;
    int v = co_await coro::to_thread([] { return blocking_read(100); });
    std::cout << "   blocking_read = " << v << std::endl;

    // 3. 工作线程抛出的异常在 co_await 处重新抛出
    std::cout << "3. Exception propagation:" << std::endl;
    try {
        co_await coro::to_thread([]() -> int { throw std::runtime_error("worker blew up"); });
    } catch (const std::exception& e) {
        std::cout << "   caught: " << e.what() << std::endl;
    }

    co_await std::move(beat);
    std::cout << std::endl << "=== to_thread example done ===" << std::endl;
}

int main() {
    coro::run(main_task());
    return 0;
}
