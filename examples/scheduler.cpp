// ============================================================================
// scheduler.cpp - dispatch independent task factories to multiple event loops
// ============================================================================
//
// Scheduler owns N worker threads, each with its own EventLoop. spawn_any()
// chooses the least-loaded worker, creates the coroutine frame on that worker,
// starts it, and keeps it detached until completion. Results should therefore
// be stored in shared state, a Promise/Future, or an external queue.

#include <coro/coro.hpp>

#include <atomic>
#include <chrono>
#include <iostream>
#include <memory>

using namespace std::chrono_literals;

coro::Task<> worker_job(int id, std::shared_ptr<std::atomic<int>> completed) {
    co_await coro::sleep(std::chrono::milliseconds(25 + id * 10));
    ++*completed;
    std::cout << "  job " << id << " completed on worker loop" << std::endl;
}

int main() {
    std::cout << "=== Scheduler example: multi-loop task dispatch ===" << std::endl;

    auto completed = std::make_shared<std::atomic<int>>(0);
    constexpr int jobs = 6;

    // Keep Scheduler alive until all submitted jobs finish. Its destructor calls
    // wait_all(), then stops and joins all worker EventLoops.
    coro::Scheduler scheduler(2);
    for (int id = 0; id < jobs; ++id) {
        scheduler.spawn_any([id, completed] { return worker_job(id, completed); });
    }

    scheduler.wait_all();
    std::cout << "completed " << completed->load() << " of " << jobs << " jobs" << std::endl;
    std::cout << "=== Scheduler example done ===" << std::endl;
    return completed->load() == jobs ? 0 : 1;
}
