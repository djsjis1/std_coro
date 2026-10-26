#pragma once

#include "task.hpp"

#include <exception>
#include <memory>
#include <mutex>
#include <tuple>
#include <type_traits>
#include <utility>
#include <variant>

// ============================================================================
// coro::gather — 并发等待多个 Task, 返回结果 tuple
// ============================================================================
//
// 语义矩阵 (与其他组合等待入口的区别, 不要凭名字推断):
//
//   gather(...)              → 等全部完成, 收全部结果; 任一失败先记录,
//                              全部退出后再抛第一个异常 (wait-all)
//   gather_all / gather_void → 同上, 方便可变数量/void 场景
//   wait_any(...)            → 第一个完成即返回; 其余任务不被取消 (调用方仍持有)
//   wait_tasks(...)          → 返回已完成集合; FirstCompleted 模式下剩余任务会被
//                              detach 继续跑, 调用方拿不到它们的控制权 (文档即合同)
//   wait_for / wait_until    → 为单个 Task 加期限; 超时先 cancel 再等它退出
//
// 调用方被取消: 剩余子任务收到 CancelledError 优雅终止 (await 点之后的
//   清理代码照常执行), 对标 asyncio (取消 await gather 的任务会级联取消)。
//
// 与 Python 的差别: asyncio.gather 默认首异常立即传播 (且不自动取消其余任务);
// 本实现选择 wait-all + 聚合首异常, 调用方需要 fail-fast 时应使用 TaskGroup。
//
// Python 映射:
//   await asyncio.gather(coro1(), coro2(), coro3())
//   →
//   auto [a, b, c] = co_await coro::gather(task1(), task2(), task3());
//
// ============================================================================
//
// 工作原理:
//
//   ┌─────────────────────────────────────────────────────────┐
//   │ 调用方协程                                               │
//   │   auto [r1, r2] = co_await gather(taskA(), taskB());    │
//   │                                                         │
//   │   1. gather(...) 把 Task 移入堆上共享状态                │
//   │   2. await_suspend: 为每个 Task 创建 monitor 协程,       │
//   │      start()+detach() (帧自持有运行到完成);              │
//   │      再登记调用方句柄 (全部完成时恢复它)                 │
//   │   3. monitor 内 co_await 对应的 Task (左值, 不移动),     │
//   │      完成后写结果、计数减一; 最后一个恢复调用方           │
//   │   4. 调用方被取消/销毁 → awaiter 的 on_waiter_destroyed: │
//      标记 cont_dead + 对剩余 Task 逐个 cancel()             │
//   └─────────────────────────────────────────────────────────┘
//
// ============================================================================
//
// 为什么需要 monitor 协程?
//
//   每个 Task 只能被 co_await 一次 (await_suspend 会消费 continuation)。
//   但 gather 需要"同时"等待多个 Task。
//
//   解决方案: 为每个 Task 创建一个轻量的 monitor 协程 (Task<void>)。
//   monitor 的唯一职责就是 await 对应的 Task, 然后更新共享状态。
//
// 为什么任务所有权在共享状态而不是 awaiter (堆上, 不在调用方帧上)?
//
//   awaiter 是 co_await 全表达式的临时对象: 调用方被取消注入时它随栈展开
//   销毁。若 Task 按成员放在 awaiter 里 (旧实现), 挂起中的子任务帧会被
//   ~Task 直接 destroy —— 子任务 await 点之后的清理代码不会执行, 且与本
//   文件声明的"其他 Task 继续运行直到完成"自相矛盾。现在 Task 放在共享
//   状态 (shared_ptr), monitor 已 detach 自持有: 调用方死亡后子任务收到
//   CancelledError 正常 unwind (栈上对象析构 = 清理逻辑), 与 Python 一致。
//
// 异常处理:
//   如果某个 Task 抛出异常:
//     - 异常被 monitor 的 try/catch 捕获
//     - 第一个异常被存储在共享状态中
//     - 其他 Task 继续运行直到完成 (不会被取消)
//     - 所有 Task 完成后, 在 await_resume 中重新抛出第一个异常
//   (wait-all + 聚合首异常; 与 asyncio.gather 的 fail-fast 有意不同)
//
// ============================================================================

namespace coro {

    namespace detail {

        // ==================================================================
        // gather_shared_state — 被所有 monitor 和 gather_awaiter 共享
        // ==================================================================
        //
        // 生命周期:
        //   - gather() 工厂创建 (shared_ptr, 任务同时移入)
        //   - 每个 monitor 与 awaiter 各持一份引用
        //   - 最后一个 monitor 完成时恢复调用方 (若仍存活)
        //   - 所有引用释放后自动销毁 (调用方死后由 monitor 收尾释放)
        //
        // 线程安全: remaining/caller/first_exception 由 mutex 保护 ——
        // Scheduler 多 loop 场景下 monitor 可能与调用方在不同线程完成。
        // ==================================================================
        // 将 void 结果替换为 std::monostate (std::tuple<void> 非法)
        template <typename T> using storage_t = std::conditional_t<std::is_void_v<T>, std::monostate, T>;

        template <typename... Ts> struct gather_shared_state {
            explicit gather_shared_state(std::tuple<Task<Ts>...> t) : tasks(std::move(t)) {}

            std::tuple<Task<Ts>...> tasks;        // 子任务所有权 (monitor 按左值等待, 保留句柄供取消)
            std::tuple<storage_t<Ts>...> results; // 全部结果 (void → monostate)
            mutable std::mutex mutex;
            size_t remaining = sizeof...(Ts);   // 未完成的 monitor 数
            std::coroutine_handle<> caller{};   // 调用方句柄 (空 = 未挂起或已销毁)
            EventLoop* caller_loop = nullptr;   // 唤醒路由回调用方的 loop
            std::exception_ptr first_exception; // 第一个异常 (全部完成后重抛)
            bool cont_dead = false;             // 等待者已销毁: 迟到的完成不得调度它

            /// 登记等待者; 全部任务恰好已完成 (或等待者已死) 时直接调度返回
            bool install(std::coroutine_handle<> h, EventLoop* loop) {
                std::lock_guard lock(mutex);
                if (remaining == 0 || cont_dead) {
                    loop->schedule(h);
                    return true;
                }
                caller = h;
                caller_loop = loop;
                return false;
            }

            /// 等待者死亡/销毁 (on_waiter_destroyed): 防止迟到完成调度悬空句柄
            void abandon() noexcept {
                std::lock_guard lock(mutex);
                cont_dead = true;
                caller = {};
                caller_loop = nullptr;
            }

            /// monitor 收尾: 记录异常 (仅首个), 最后一个完成时恢复调用方
            void complete(std::exception_ptr error) {
                std::lock_guard lock(mutex);
                if (error && !first_exception)
                    first_exception = std::move(error);
                if (--remaining == 0 && caller && !cont_dead) {
                    auto waiter = caller;
                    EventLoop* loop = caller_loop;
                    caller = {};
                    caller_loop = nullptr;
                    loop->schedule(waiter);
                }
            }

            /// 优雅取消全部剩余子任务 (对已完成的任务是 no-op)。
            /// 子任务在下一个 await 点收到 CancelledError, 栈上对象正常析构。
            void cancel_remaining() noexcept {
                std::apply([](auto&... t) { (t.cancel(), ...); }, tasks);
            }
        };

        // ==================================================================
        // gather_monitor — 等待单个 Task 完成, 更新共享状态
        // ==================================================================
        //
        // 模板参数:
        //   I     — 当前 Task 在参数包中的索引 (0, 1, 2, ...)
        //   Ts... — 所有 Task 的返回类型 (完整参数包)
        //
        // Task 对象按左值 co_await (wrapper 按引用存储, 不移动):
        // 共享状态里的 Task 保留句柄, 调用方死亡路径仍能 cancel() 它们。
        // ==================================================================
        template <size_t I, typename... Ts>
        Task<void> gather_monitor(std::shared_ptr<gather_shared_state<Ts...>> state) {
            using T = std::tuple_element_t<I, std::tuple<Ts...>>;
            std::exception_ptr error;
            try {
                if constexpr (!std::is_void_v<T>) {
                    std::get<I>(state->results) = co_await std::get<I>(state->tasks);
                } else {
                    co_await std::get<I>(state->tasks);
                    std::get<I>(state->results) = std::monostate{};
                }
            } catch (...) {
                // 异常记录进共享状态 (complete 内只保留第一个)
                error = std::current_exception();
            }
            state->complete(std::move(error));
            co_return;
        }

        // ==================================================================
        // gather_awaiter — co_await gather(...) 表达式的结果对象
        // ==================================================================
        //
        // 轻量外壳: 只持共享状态。任务与 monitor 的生命周期都不依赖
        // 本对象 (它只是 co_await 全表达式的临时对象, 调用方取消时销毁)。
        // ==================================================================
        template <typename... Ts> struct gather_awaiter {
            std::shared_ptr<gather_shared_state<Ts...>> state_;

            explicit gather_awaiter(std::shared_ptr<gather_shared_state<Ts...>> s) : state_(std::move(s)) {}

            // ---- Awaitable 接口 ----

            bool await_ready() const noexcept { return false; }

            void await_suspend(std::coroutine_handle<> h) {
                // 1. 启动全部 monitor 并 detach: 帧自持有运行到完成,
                //    不随调用方帧 (即本临时 awaiter 的宿主) 销毁。
                auto launch = [&]<size_t... Is>(std::index_sequence<Is...>) {
                    (([&] {
                         Task<void> mon = gather_monitor<Is, Ts...>(state_);
                         mon.start();
                         mon.detach();
                     }()),
                     ...);
                };
                launch(std::index_sequence_for<Ts...>{});
                // 2. 最后登记等待者 (全部同步完成的兜底路径由 install 处理)
                state_->install(h, &EventLoop::get());
            }

            std::tuple<storage_t<Ts>...> await_resume() {
                if (state_->first_exception)
                    std::rethrow_exception(state_->first_exception);
                return std::move(state_->results);
            }

            /// 调用方被取消注入 / 帧被销毁 (两条路径都经 wrapper 析构到达):
            /// 标记 cont_dead 防止迟到完成调度已销毁的句柄; 优雅取消全部
            /// 剩余子任务 —— 而不是随临时对象把它们硬销毁 (清理代码会跳过)。
            void on_waiter_destroyed(std::coroutine_handle<>) noexcept {
                state_->abandon();
                state_->cancel_remaining();
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
    //   - 支持 Task<void> 混合使用 (结果 tuple 中该位置为 monostate)
    //   - co_await 期间调用方被取消 → 剩余子任务被优雅取消 (见文件头)
    // ============================================================================
    template <typename... Ts> auto gather(Task<Ts>... tasks) {
        return detail::gather_awaiter<Ts...>(
            std::make_shared<detail::gather_shared_state<Ts...>>(std::make_tuple(std::move(tasks)...)));
    }

    /// 空参数 gather: 立即返回空 tuple
    inline Task<std::tuple<>> gather() {
        co_return std::tuple<>{};
    }

} // namespace coro
