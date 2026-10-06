// ============================================================================
// wait_timeout.cpp - timeouts and dynamic task sets
// ============================================================================
//
// wait_for(task, timeout) 对标 asyncio.wait_for(): 超时会取消 task 并抛出
// coro::TimeoutError。wait_tasks(tasks, WaitMode) 对标 asyncio.wait():
// 可在 FirstCompleted / FirstException / AllCompleted 三种策略间选择。

#include <coro/coro.hpp>

#include <chrono>
#include <iostream>
#include <vector>

using namespace std::chrono_literals;

coro::Task<int> work(int id, int delay_ms) {
    std::cout << "  worker " << id << " starts (" << delay_ms << "ms)" << std::endl;
    co_await coro::sleep(std::chrono::milliseconds(delay_ms));
    std::cout << "  worker " << id << " completes" << std::endl;
    co_return id * 10;
}

coro::Task<> timeout_example() {
    std::cout << "1. wait_for timeout:" << std::endl;
    try {
        // 参数顺序是 (Task, timeout): 超时后 task 会收到 CancelledError
        int value = co_await coro::wait_for(work(1, 200), 50ms);
        std::cout << "   result: " << value << std::endl;
    } catch (const coro::TimeoutError&) {
        std::cout << "   timed out; task was cancelled" << std::endl;
    }
    std::cout << std::endl;
}

coro::Task<> first_completed_example() {
    std::cout << "2. wait_tasks FirstCompleted:" << std::endl;
    std::vector<coro::Task<int>> tasks;
    tasks.push_back(coro::spawn(work(1, 150)));
    tasks.push_back(coro::spawn(work(2, 50)));
    tasks.push_back(coro::spawn(work(3, 100)));

    auto results = co_await coro::wait_tasks(std::move(tasks), coro::WaitMode::FirstCompleted);
    std::cout << "   first result: " << results.front() << std::endl;
    // FirstCompleted 的落选任务继续在后台运行；本示例让事件循环在 main_task
    // 结束前自然收尾。需要结构化取消其余任务时，请选择 TaskGroup。
    co_await coro::sleep(200ms);
    std::cout << std::endl;
}

coro::Task<> all_completed_example() {
    std::cout << "3. wait_tasks AllCompleted:" << std::endl;
    std::vector<coro::Task<int>> tasks;
    tasks.push_back(work(4, 80));
    tasks.push_back(work(5, 40));
    auto results = co_await coro::wait_tasks(std::move(tasks), coro::WaitMode::AllCompleted);
    std::cout << "   results:";
    for (int value : results)
        std::cout << ' ' << value;
    std::cout << std::endl << std::endl;
}

coro::Task<> main_task() {
    std::cout << "=== wait_for / wait_tasks example ===" << std::endl << std::endl;
    co_await timeout_example();
    co_await first_completed_example();
    co_await all_completed_example();
    std::cout << "=== wait_for / wait_tasks example done ===" << std::endl;
}

int main() {
    coro::run(main_task());
    return 0;
}
