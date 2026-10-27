#pragma once

// ============================================================================
// coro - C++20 coroutine event loop framework (like Python asyncio)
// ============================================================================
//
// 本头是**核心子集**伞头: 协程/调度/定时/同步/并发组合器。
// 它不包含带平台依赖或 CMake 开关的模块 —— 全量聚合会让 core-only 构建
// (CORO_ENABLE_NATIVE_IO=OFF) 也被拉进原生 I/O 头。其余模块按需单独 include:
//
//   net / dns / fs / fs_watch / pipe / process / unix   → 原生 I/O (CORO_ENABLE_NATIVE_IO)
//   channel / select / stream / pool / rate_limit / timer / signal / context
//                                                       → 平台无关, 按需引入
//   task_registry                                       → 任务登记与关停
//   http / http_client / tls                            → 需对应 CMake 开关
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

namespace coro {

    // ============================================================================
    // Convenience entry point: start a task and drive the event loop to completion
    // ============================================================================
    //
    // 对标 asyncio.run(main()): 返回主协程的返回值 (void 任务返回 void)。
    //   int result = coro::run(compute());          // 直接拿到结果
    //   coro::run(background());                    // Task<> 版本
    //
    // 事件循环可能被 stop() 提前终止 (信号处理/外部调用): 主任务尚未完成时
    // 必须显式报错 —— Task<void>::take_result() 只检查异常, 静默返回会
    // 把"没跑完"伪装成"成功"。
    template <typename T> T run(Task<T> task) {
        task.start();
        EventLoop::get().run();
        if (!task.is_ready())
            throw std::runtime_error("coro::run: event loop stopped before the main task completed");
        if constexpr (std::is_void_v<T>)
            task.take_result();
        else
            return task.take_result();
    }

} // namespace coro
