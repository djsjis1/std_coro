
#include <coro/coro.hpp>

#include <chrono>
#include <iostream>
#include <string>

using namespace std::chrono_literals;

// 模拟一次异步计算
static coro::Task<int> compute(int id, int delay_ms) {
    std::cout << "  [" << id << "] 计算中 (" << delay_ms << "ms)..." << std::endl;
    co_await coro::sleep(std::chrono::milliseconds(delay_ms));
    std::cout << "  [" << id << "] 完成!" << std::endl;
    co_return id * 100;
}

// 主协程：串行等待 + 并发等待
static coro::Task<> run() {
    std::cout << "=== 串行 await ===" << std::endl;
    int a = co_await compute(1, 300);
    int b = co_await compute(2, 200);
    std::cout << "结果: a=" << a << ", b=" << b << std::endl << std::endl;

    std::cout << "=== 并发 await (spawn) ===" << std::endl;
    auto t1 = coro::spawn(compute(3, 500));
    auto t2 = coro::spawn(compute(4, 400));
    int v1 = co_await std::move(t1);
    int v2 = co_await std::move(t2);
    std::cout << "结果: v1=" << v1 << ", v2=" << v2 << std::endl;

    std::cout << "=== 完成 ===" << std::endl;
}

int main() {
    coro::run(run());
    return 0;
}
