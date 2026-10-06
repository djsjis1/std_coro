// ============================================================================
// task_group.cpp - structured concurrency with TaskGroup (like asyncio.TaskGroup)
// ============================================================================
//
// TaskGroup 语义 (对标 Python 3.11 asyncio.TaskGroup):
//   - spawn 多个子任务, co_await group.wait() 等待全部完成;
//   - 任一子任务失败 → 自动取消其余子任务;
//   - wait() 结束时若有失败, 抛 ExceptionGroup (即使只有一个也打包)。

#include <coro/coro.hpp>

#include <chrono>
#include <iostream>
#include <string>

using namespace std::chrono_literals;

coro::Task<int> fetch(const std::string& name, int delay_ms) {
    std::cout << "  [" << name << "] start..." << std::endl;
    co_await coro::sleep(std::chrono::milliseconds(delay_ms));
    std::cout << "  [" << name << "] done" << std::endl;
    co_return delay_ms;
}

// 一个会失败的子任务: TaskGroup 会取消其余子任务并聚合异常
coro::Task<int> fetch_failing(const std::string& name) {
    co_await coro::sleep(std::chrono::milliseconds(50));
    throw std::runtime_error("connection refused: " + name);
    co_return 0; // 协程返回类型要求
}

coro::Task<> happy_path() {
    std::cout << "1. Happy path: all children succeed" << std::endl;
    coro::TaskGroup group;
    group.spawn(fetch("a", 100));
    group.spawn(fetch("b", 200));
    group.spawn(fetch("c", 150));
    co_await group.wait(); // 等全部完成; 有失败会抛 ExceptionGroup
    std::cout << "   all children completed" << std::endl << std::endl;
}

coro::Task<> failure_path() {
    std::cout << "2. Failure path: one child throws, siblings get cancelled" << std::endl;
    coro::TaskGroup group;
    group.spawn(fetch_failing("bad"));
    group.spawn(fetch("slow-1", 500)); // 会被组级取消提前打断
    group.spawn(fetch("slow-2", 500));
    try {
        co_await group.wait();
    } catch (const coro::ExceptionGroup& eg) {
        std::cout << "   ExceptionGroup with " << eg.exceptions().size() << " exception(s):" << std::endl;
        for (const auto& e : eg.exceptions()) {
            try {
                std::rethrow_exception(e);
            } catch (const std::exception& ex) {
                std::cout << "     - " << ex.what() << std::endl;
            }
        }
    }
    std::cout << std::endl;
}

coro::Task<> main_task() {
    std::cout << "=== TaskGroup example: structured concurrency ===" << std::endl << std::endl;
    co_await happy_path();
    co_await failure_path();
    std::cout << "=== TaskGroup example done ===" << std::endl;
}

int main() {
    coro::run(main_task());
    return 0;
}
