#pragma once

#include "task.hpp"

#include <atomic>
#include <memory>
#include <tuple>
#include <type_traits>

// ============================================================================
// coro::gather — 并发等待多个 Task, 返回结果 tuple
// ============================================================================
//
// Python 映射:
//   await asyncio.gather(coro1(), coro2(), coro3())
//   →
//   auto [a, b, c] = co_await coro::gather(task1(), task2(), task3());
//
// ============================================================================
//
// 工作原理 (五步流程):
//
//   ┌─────────────────────────────────────────────────────────┐
//   │ 调用方协程                                               │
//   │   auto [r1, r2] = co_await gather(taskA(), taskB());    │
//   │                                                         │
//   │   ┌─ gather(...) 被求值:                                │
//   │   │  1. taskA() 和 taskB() 返回 Task<T> 对象            │
//   │   │  2. 它们被移入 gather_awaiter                       │
//   │   │                                                     │
//   │   ┌─ co_await gather_awaiter:                           │
//   │   │  3. await_suspend 被调用:                           │
//   │   │     a. 创建共享状态 (计数器 = 2, continuation)       │
//   │   │     b. 为每个 Task 创建一个 monitor 协程             │
//   │   │        - monitor 内部 co_await 对应的 Task           │
//   │   │        - 完成后将结果写入共享状态, 计数器 -1           │
//   │   │        - 最后一个完成的恢复调用方                     │
//   │   │     c. 启动所有 monitor                             │
//   │   │                                                     │
//   │   ┌─ 调用方被挂起, 事件循环驱动所有 monitor 并发执行     │
//   │   │                                                     │
//   │   ┌─ 所有 monitor 完成后:                               │
//   │   │  5. await_resume 被调用 → 返回 tuple<结果...>       │
//   └─────────────────────────────────────────────────────────┘
//
// ============================================================================
//
// 为什么需要 monitor 协程?
//
//   每个 Task 只能被 co_await 一次 (await_suspend 会移动内部状态)。
//   但 gather 需要"同时"等待多个 Task。
//
//   解决方案: 为每个 Task 创建一个轻量的 monitor 协程 (Task<void>)。
//   monitor 的唯一职责就是 await 对应的 Task, 然后更新计数器。
//   所有 monitor 和原始 Task 之间是一对一的关系, 互不干扰。
//
// 异常处理:
//   如果某个 Task 抛出异常:
//     - 异常被 monitor 的 try/catch 捕获
//     - 第一个异常被存储在共享状态中
//     - 其他 Task 继续运行直到完成 (不会被取消)
//     - 所有 Task 完成后, 在 await_resume 中重新抛出第一个异常
//   这与 Python asyncio.gather 的默认行为一致。
//
// ============================================================================

namespace coro
{

    namespace detail
    {

        // ==================================================================
        // gather_shared_state — 被所有 monitor 和 gather_awaiter 共享
        // ==================================================================
        //
        // 生命周期:
        //   - 由 gather_awaiter::await_suspend 创建 (shared_ptr)
        //   - 每个 monitor 持有一份 shared_ptr 拷贝
        //   - 最后一个 monitor 完成时, continuation 被恢复
        //   - 所有引用释放后自动销毁
        //
        // 线程安全:
        //   remaining 使用 std::atomic<size_t>,
        //   因为 std::shared_ptr 的引用计数操作本身要求原子性,
        //   这里保持一致以防未来多线程扩展。
        // ==================================================================
        template <typename... Ts>
        struct gather_shared_state
        {
            std::tuple<Ts...> results;            // 所有 Task 的结果
            std::atomic<size_t> remaining;        // 剩余未完成的 Task 数量
            std::coroutine_handle<> continuation; // 调用方协程句柄
            std::exception_ptr first_exception;   // 第一个异常 (如果有)

            gather_shared_state(size_t n, std::coroutine_handle<> h)
                : remaining(n), continuation(h) {}
        };

        // ==================================================================
        // gather_monitor — 等待单个 Task 完成, 更新共享状态
        // ==================================================================
        //
        // 模板参数:
        //   I     — 当前 Task 在参数包中的索引 (0, 1, 2, ...)
        //   Ts... — 所有 Task 的返回类型 (完整参数包)
        //
        // 工作流程:
        //   1. co_await task → 挂起直到 task 完成
        //   2. 将结果写入 state->results 的第 I 个位置
        //   3. remaining 原子减 1
        //   4. 如果 remaining == 0 (最后一个完成):
        //      将调用方协程 (state->continuation) 加入就绪队列
        //
        // 返回值: Task<void> — monitor 本身不产生有意义的值
        // ==================================================================
        template <size_t I, typename... Ts>
        Task<void> gather_monitor(
            Task<std::tuple_element_t<I, std::tuple<Ts...>>> task,
            std::shared_ptr<gather_shared_state<Ts...>> state)
        {

            // 提取第 I 个位置的类型
            using T = std::tuple_element_t<I, std::tuple<Ts...>>;

            try
            {
                if constexpr (!std::is_void_v<T>)
                {
                    // 非 void 类型: co_await 获取结果, 存入共享状态
                    std::get<I>(state->results) = co_await std::move(task);
                }
                else
                {
                    // void 类型: 只需要等待完成, 不需要存储结果
                    co_await std::move(task);
                }
            }
            catch (...)
            {
                // 只保存第一个异常 (后续异常被忽略)
                // 这与 Python asyncio.gather 的行为一致
                if (!state->first_exception)
                {
                    state->first_exception = std::current_exception();
                }
            }

            // 原子减 1; 如果是最后一个, 恢复调用方
            // 使用前缀 -- 确保先减后比较
            if (--state->remaining == 0)
            {
                EventLoop::get().schedule(state->continuation);
            }
        }

        // ==================================================================
        // gather_awaiter — co_await gather(...) 表达式的结果对象
        // ==================================================================
        //
        // 这个对象存活在调用方协程的栈帧中 (通过 co_await 的暂停机制)。
        // 当调用方被挂起时, 此对象的生命周期与挂起状态绑定。
        // ==================================================================
        template <typename... Ts>
        struct gather_awaiter
        {
            // ---- 数据成员 ----

            // 原始 Task 对象 (所有权转移到这里)
            std::tuple<Task<Ts>...> tasks_;

            // 共享状态 (堆分配, 所有 monitor 和此 awaiter 共享)
            std::shared_ptr<gather_shared_state<Ts...>> state_;

            // 辅助 alias: 将 Task<void> 与 Ts... 参数包绑定,
            // 这样 sizeof...(Ts) 个 Task<void> 可以通过参数包展开生成
            template <typename>
            using void_task = Task<void>;

            // monitor 协程 (必须保持存活, 否则协程帧会被销毁)
            // void_task<Ts>... 展开为 Task<void>, Task<void>, ... (共 N 个)
            std::tuple<void_task<Ts>...> monitors_;

            explicit gather_awaiter(std::tuple<Task<Ts>...> tasks)
                : tasks_(std::move(tasks)) {}

            // ---- Awaitable 接口 ----

            bool await_ready() const noexcept { return false; }

            void await_suspend(std::coroutine_handle<> h)
            {
                // 创建共享状态: 计数器初始化为 Task 数量
                state_ = std::make_shared<gather_shared_state<Ts...>>(
                    sizeof...(Ts), h);

                // 为每个 Task 创建一个 monitor 协程
                // make_monitors 使用 index_sequence 展开参数包:
                //   monitor_0 = gather_monitor<0>(task_0, state)
                //   monitor_1 = gather_monitor<1>(task_1, state)
                //   ...
                monitors_ = make_monitors(std::index_sequence_for<Ts...>{});

                // 并发启动所有 monitor
                // std::apply 将 tuple 展开为 lambda 的参数包
                // fold expression (m.start(), ...) 逐个调用 start()
                std::apply([](auto &...m)
                           { (m.start(), ...); }, monitors_);
            }

            std::tuple<Ts...> await_resume()
            {
                // 如果有异常, 重新抛出
                if (state_->first_exception)
                {
                    std::rethrow_exception(state_->first_exception);
                }
                // 返回所有结果 (每个 monitor 已经将结果写入了 state_->results)
                return std::move(state_->results);
            }

        private:
            /// 使用 index_sequence 展开参数包, 为每个 Task 调用 gather_monitor
            template <size_t... Is>
            auto make_monitors(std::index_sequence<Is...>)
            {
                return std::make_tuple(
                    // 显式传递 Ts... 模板参数, 因为 gather_monitor 的
                    // 第一个参数类型 (Task<tuple_element_t<...>>) 中
                    // Ts... 处于不可推导上下文 (non-deduced context)
                    gather_monitor<Is, Ts...>(
                        std::move(std::get<Is>(tasks_)), state_)...);
            }
        };

    } // namespace detail

    // ============================================================================
    // 公开 API: gather(Task<Ts>... tasks)
    // ============================================================================
    //
    // 接受任意数量和类型的 Task, 返回一个可被 co_await 的 gather_awaiter。
    //
    // 用法示例:
    //   auto [s, n] = co_await gather(
    //       fetch_string(),    // → Task<string>
    //       compute_number()   // → Task<int>
    //   );
    //   // s: string, n: int
    //
    // 注意:
    //   - 参数按值传递 (Task<Ts>), 会移动传入的 Task
    //   - 返回类型 auto → 自动推导为 gather_awaiter<Ts...>
    //   - 支持 Task<void> 混合使用 (结果 tuple 中该位置为 void 标记)
    // ============================================================================
    template <typename... Ts>
    auto gather(Task<Ts>... tasks)
    {
        return detail::gather_awaiter<Ts...>(
            std::make_tuple(std::move(tasks)...));
    }

} // namespace coro
