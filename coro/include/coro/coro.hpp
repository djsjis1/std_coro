#pragma once

// ============================================================================
// coro - C++20 coroutine event loop framework (like Python asyncio)
// ============================================================================
//
// Quick start:
//   #include <coro/coro.hpp>
//
//   coro::Task<int> compute() {
//       co_await coro::sleep(std::chrono::seconds(1));
//       co_return 42;
//   }
//
//   coro::Task<> main_task() {
//       int result = co_await compute();
//       std::cout << result << std::endl;
//   }
//
//   int main() {
//       coro::run(main_task());
//   }
//
// ============================================================================

#include "event_source.hpp"
#include "event_loop.hpp"
#include "task.hpp"
#include "exceptions.hpp"
#include "sleep.hpp"
#include "gather.hpp"
#include "future.hpp"
#include "sync.hpp"
#include "queue.hpp"
#include "schedule.hpp"
#include "wait.hpp"
#include "task_group.hpp"
#include "thread.hpp"
#include "scheduler.hpp"

#include <type_traits>

namespace coro
{

    // ============================================================================
    // Convenience entry point: start a task and drive the event loop to completion
    // ============================================================================
    //
    // 对标 asyncio.run(main()): 返回主协程的返回值 (void 任务返回 void)。
    //   int result = coro::run(compute());          // 直接拿到结果
    //   coro::run(background());                    // Task<> 版本
    template <typename T>
    T run(Task<T> task)
    {
        task.start();
        EventLoop::get().run();
        if constexpr (std::is_void_v<T>)
            task.take_result();
        else
            return task.take_result();
    }

} // namespace coro
