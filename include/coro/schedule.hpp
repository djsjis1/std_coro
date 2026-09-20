#pragma once

#include "task.hpp"
#include "sleep.hpp"

#include <chrono>
#include <memory>

// ============================================================================
// coro::schedule — call_soon / call_later / call_at
// ============================================================================
//
// Python 映射:
//   loop.call_soon(callback)      →  call_soon(callback)
//   loop.call_later(5, callback)  →  call_later(5s, callback)
//   loop.call_at(deadline, cb)    →  call_at(deadline, cb)
//
// 生命周期: 所有调度均使用"命名协程函数 + detach 自持有"模式,
//   monitor 协程启动后即 detach, 协程帧自持有运行到完成
//   (final_suspend 时自动销毁), 调用者无需管理生命周期。
//   (不用 self-referencing 临时 lambda: 捕获属于闭包对象，闭包可能先析构，
//    见 README/14 讲)
// ============================================================================

namespace coro {

    namespace detail {

        // ==================================================================
        // call_soon 的执行协程: 命名函数, 参数进帧 (避免闭包生命周期依赖)
        // ==================================================================
        template <typename F> Task<void> call_soon_impl(F func) {
            if constexpr (std::is_invocable_v<F>) {
                using result_t = std::invoke_result_t<F>;
                if constexpr (std::is_same_v<result_t, Task<void>>) {
                    // 协程回调: 等待它完成
                    co_await func();
                    co_return;
                }
            }
            // 普通回调
            func();
            co_return;
        }

        // ==================================================================
        // call_later / call_at 的执行协程: 命名函数, 参数进帧
        // ==================================================================
        template <typename F> Task<void> call_at_impl(std::chrono::steady_clock::time_point deadline, F func) {
            co_await sleep_awaiter{deadline};
            if constexpr (std::is_invocable_v<F>) {
                using result_t = std::invoke_result_t<F>;
                if constexpr (std::is_same_v<result_t, Task<void>>) {
                    // 协程回调: 等待它完成
                    co_await func();
                    co_return;
                }
            }
            // 普通回调
            func();
            co_return;
        }

    } // namespace detail

    // ============================================================================
    // call_soon — 将回调/协程立即加入就绪队列
    // ============================================================================

    template <typename F> void call_soon(F&& func) {
        if constexpr (std::is_invocable_v<F>) {
            // 命名协程函数 + detach 自持有: 协程帧自持有运行到完成
            Task<void> t = detail::call_soon_impl(std::forward<F>(func));
            t.start();
            t.detach();
        }
    }

    // ============================================================================
    // call_later — 延迟调度
    // ============================================================================

    template <typename Rep, typename Period, typename F>
    void call_later(std::chrono::duration<Rep, Period> delay, F&& func) {
        // 命名协程函数 + detach 自持有 (同 call_soon)
        Task<void> t = detail::call_at_impl(std::chrono::steady_clock::now() + delay, std::forward<F>(func));
        t.start();
        t.detach();
    }

    // ============================================================================
    // call_at — 在指定时间点调度
    // ============================================================================

    template <typename F> void call_at(std::chrono::steady_clock::time_point deadline, F&& func) {
        // 命名协程函数 + detach 自持有 (同 call_soon)
        Task<void> t = detail::call_at_impl(deadline, std::forward<F>(func));
        t.start();
        t.detach();
    }

} // namespace coro
