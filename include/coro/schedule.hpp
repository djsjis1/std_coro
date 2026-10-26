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
        template <typename> inline constexpr bool always_false_v = false;

        /// result 是不是 Task<非void>: 这种回调会被静默丢弃 (惰性 Task 从未
        /// start, 协程体永不执行), 必须在编译期拦截。
        template <typename T> struct is_task : std::false_type {};
        template <typename T> struct is_task<Task<T>> : std::true_type {};

        // ==================================================================
        // call_soon / call_later / call_at 的公共执行协程: 命名函数,
        // 参数进帧 (避免闭包生命周期依赖)。
        // immediate=true 跳过定时器直接执行 (下一轮就绪队列语义由
        // start() 的入队时机提供)。
        // ==================================================================
        template <typename F>
        Task<void> schedule_impl(bool immediate, std::chrono::steady_clock::time_point deadline, F func) {
            static_assert(std::is_invocable_v<F>,
                          "coro::call_soon/call_later/call_at: callback must be invocable without arguments");
            using result_t = std::invoke_result_t<F>;
            if constexpr (std::is_same_v<result_t, Task<void>>) {
                if (!immediate)
                    co_await sleep_awaiter{deadline};
                // 协程回调: 等待它完成
                co_await func();
            } else if constexpr (is_task<result_t>::value) {
                // 返回 Task<T>(T≠void) 的回调创建的惰性 Task 被丢弃后
                // 协程体永不执行, 结果无处消费 —— 编译期报错优于静默丢失。
                static_assert(always_false_v<F>,
                              "coro::call_soon/call_later/call_at: callback returning Task<T> (T != void) would "
                              "never run; use Task<void> and co_await the result yourself");
            } else {
                if (!immediate)
                    co_await sleep_awaiter{deadline};
                // 普通回调
                func();
            }
            co_return;
        }

    } // namespace detail

    // ============================================================================
    // call_soon — 将回调/协程立即加入就绪队列
    // ============================================================================

    template <typename F> void call_soon(F&& func) {
        // 命名协程函数 + detach 自持有: 协程帧自持有运行到完成
        Task<void> t = detail::schedule_impl(true, {}, std::forward<F>(func));
        t.start();
        t.detach();
    }

    // ============================================================================
    // call_later — 延迟调度
    // ============================================================================

    template <typename Rep, typename Period, typename F>
    void call_later(std::chrono::duration<Rep, Period> delay, F&& func) {
        // 命名协程函数 + detach 自持有 (同 call_soon)
        Task<void> t = detail::schedule_impl(false, std::chrono::steady_clock::now() + delay, std::forward<F>(func));
        t.start();
        t.detach();
    }

    // ============================================================================
    // call_at — 在指定时间点调度
    // ============================================================================

    template <typename F> void call_at(std::chrono::steady_clock::time_point deadline, F&& func) {
        // 命名协程函数 + detach 自持有 (同 call_soon)
        Task<void> t = detail::schedule_impl(false, deadline, std::forward<F>(func));
        t.start();
        t.detach();
    }

} // namespace coro
