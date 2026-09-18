#pragma once

#include "task.hpp"
#include "sleep.hpp"
#include "sync.hpp"

#include <chrono>
#include <memory>
#include <tuple>
#include <vector>

// ============================================================================
// coro::wait — wait_for / wait_any / gather_all / gather_void
// ============================================================================

namespace coro {

    // 辅助: 无条件挂起 + 回调 (模板化, 零 std::function 开销)
    template <typename F> struct suspend_awaiter {
        F on_suspend;
        bool await_ready() const noexcept { return false; }
        void await_suspend(std::coroutine_handle<> h) { on_suspend(h); }
        void await_resume() const noexcept {}
    };

    template <typename F> suspend_awaiter(F) -> suspend_awaiter<F>;

    namespace detail {
        // wait_for 的定时器协程: 用命名函数而非 self-referencing lambda。
        // 参数作为协程参数被复制进帧, 生命周期由帧保证,
        // 规避 MSVC Debug 下 lambda 捕获的帧存储问题。
        //
        // task 指向调用方 wait_for 协程帧内的局部 Task (地址稳定):
        //   - 超时路径: wait_for 仍挂起在 co_await task 上 → 指针有效
        //   - 成功路径: wait_for 先 cancel+detach 定时器, 定时器恢复时
        //     在取消检查处抛 CancelledError, 不会执行到 task->cancel()
        //   - 异常展开路径: wait_for 帧销毁时局部 Task 析构 → 销毁挂起中
        //     的定时器帧 (sleep_awaiter 析构置位令牌, 定时器堆惰性跳过)
        template <typename T> Task<void> wait_for_timer_impl(Task<T>* task, std::chrono::milliseconds timeout) {
            co_await coro::sleep(timeout);
            // task 已完成时 cancel 是 no-op
            task->cancel();
        }
    } // namespace detail

    // ============================================================================
    // wait_for — 带超时的等待
    // ============================================================================
    template <typename T, typename Rep, typename Period>
    Task<T> wait_for(Task<T> task, std::chrono::duration<Rep, Period> timeout) {
        // task / timer 都是本协程帧的局部对象 (地址稳定, 生命周期覆盖
        // 挂起期) —— 不再需要堆上 shared_ptr 保活。
        auto timeout_ms = std::chrono::duration_cast<std::chrono::milliseconds>(timeout);
        Task<void> timer = detail::wait_for_timer_impl<T>(&task, timeout_ms);
        timer.start();

        try {
            // 注意: co_await 左值 task (wrapper 按引用存储, 不移动 Task)。
            // 若写成 std::move(task), Task 会被移走, 定时器超时路径的
            // task->cancel() 将变成 no-op → 超时失效。
            T result = co_await task;
            // 成功路径: 主动取消 timer, 否则它会睡满整个 timeout,
            // 事件循环要空等到 timeout 才退出。
            // cancel() 会把 timer 帧放入就绪队列; 随后 detach() 放弃托管,
            // 使本帧销毁时不会 destroy 一个"已入队未执行"的帧
            // (定时器帧恢复后在取消检查处抛 CancelledError 自行收尾)。
            timer.cancel();
            timer.detach();
            co_return result;
        } catch (const CancelledError&) {
            throw TimeoutError{};
        }
    }

    // wait_for 的 Task<void> 重载 (非模板, 优先于主模板匹配)
    // 主模板无法实例化 T=void (T result = co_await ... 对 void 非法)。
    template <typename Rep, typename Period>
    Task<void> wait_for(Task<void> task, std::chrono::duration<Rep, Period> timeout) {
        auto timeout_ms = std::chrono::duration_cast<std::chrono::milliseconds>(timeout);
        Task<void> timer = detail::wait_for_timer_impl<void>(&task, timeout_ms);
        timer.start();

        try {
            co_await task;  // 同主模板: 左值, 不移动
            timer.cancel(); // 成功路径: 主动取消 timer (见主模板注释)
            timer.detach();
        } catch (const CancelledError&) {
            throw TimeoutError{};
        }
    }

    namespace detail {
        template <typename T> struct wait_any_state {
            std::optional<T> result;
            std::exception_ptr exc;
            std::coroutine_handle<> cont;
            bool done = false;
        };

        // wait_any 的 monitor 协程: 命名函数 (参数进帧)。
        // 第一个完成的 monitor 设置 result 并唤醒调用方。
        // task 指向调用方帧内局部 Task: 首行 co_await std::move(*task)
        // 把 Task 移入本帧, 之后指针不再使用 (调用方帧挂起期间始终存活)。
        template <typename T> Task<void> wait_any_monitor(Task<T>* task, std::shared_ptr<wait_any_state<T>> state) {
            try {
                T val = co_await std::move(*task);
                // 只有第一个完成的 monitor 设置结果 (先设置结果再唤醒)
                if (!state->done) {
                    state->result = std::move(val);
                    state->done = true;
                    if (state->cont)
                        EventLoop::get().schedule(state->cont);
                }
            } catch (...) {
                if (!state->done) {
                    state->exc = std::current_exception();
                    state->done = true;
                    if (state->cont)
                        EventLoop::get().schedule(state->cont);
                }
            }
        }
    } // namespace detail

    // ============================================================================
    // wait_any — FIRST_COMPLETED (两路竞速)
    // ============================================================================
    template <typename T> Task<T> wait_any(Task<T> t1, Task<T> t2) {
        auto state = std::make_shared<detail::wait_any_state<T>>();

        // t1/t2 是本帧局部对象 (地址稳定); monitor 把它们移入自己帧后自持有运行
        Task<void> m1 = detail::wait_any_monitor<T>(&t1, state);
        Task<void> m2 = detail::wait_any_monitor<T>(&t2, state);
        m1.start();
        m1.detach(); // fire-and-forget: monitor 帧自持有到完成
        m2.start();
        m2.detach();

        // 挂起调用方, 在 await_suspend 中设置 cont。
        // monitor 已加入就绪队列但尚未运行, 所以 cont 先于 monitor 完成被设置。
        co_await suspend_awaiter{[state](std::coroutine_handle<> h) { state->cont = h; }};

        if (state->exc)
            std::rethrow_exception(state->exc);
        co_return std::move(*state->result);
    }

    // ============================================================================
    // wait_any — N 路竞速 (vector 版本)
    // ============================================================================
    //
    // 用法:
    //   auto result = co_await coro::wait_any(std::move(tasks));
    //
    // 第一个完成 (成功或失败) 的 Task 决定返回值;
    // 其余 Task 继续在后台运行 (结果丢弃, 与两路版语义一致)。
    // ============================================================================

    namespace detail {
        // N 路 wait_any 的 monitor: 指针方式 (与两路版 wait_any_monitor 一致)
        // task 指向调用方帧内局部 Task: 首行 co_await std::move(*task)
        // 把 Task 移入本帧, 之后指针不再使用 (调用方帧挂起期间始终存活)。
        // 注意: 不可按值传递 Task<T> 协程参数 —— GCC 10 帧布局对含
        //       std::optional<T> 的 move-only 类型有 double-free bug。
        template <typename T> Task<void> wait_any_n_monitor(Task<T>* task, std::shared_ptr<wait_any_state<T>> state) {
            try {
                T val = co_await std::move(*task);
                if (!state->done) {
                    state->result = std::move(val);
                    state->done = true;
                    if (state->cont)
                        EventLoop::get().schedule(state->cont);
                }
            } catch (...) {
                if (!state->done) {
                    state->exc = std::current_exception();
                    state->done = true;
                    if (state->cont)
                        EventLoop::get().schedule(state->cont);
                }
            }
        }
    } // namespace detail

    template <typename T> Task<T> wait_any(std::vector<Task<T>> tasks) {
        if (tasks.empty())
            throw std::invalid_argument("wait_any: empty task list");
        if (tasks.size() == 1)
            co_return co_await std::move(tasks[0]);

        auto state = std::make_shared<detail::wait_any_state<T>>();
        for (size_t i = 0; i < tasks.size(); i++) {
            Task<void> m = detail::wait_any_n_monitor<T>(&tasks[i], state);
            m.start();
            m.detach();
        }

        co_await suspend_awaiter{[state](std::coroutine_handle<> h) { state->cont = h; }};

        if (state->exc)
            std::rethrow_exception(state->exc);
        co_return std::move(*state->result);
    }

    namespace detail {
        template <typename T> struct gather_all_state {
            std::vector<T> results;
            std::atomic<size_t> remaining;
            std::coroutine_handle<> cont;
            std::exception_ptr exc;
        };

        // gather_all 的 monitor 协程: 指针方式 (同 wait_any_monitor)
        // task 指向调用方帧内局部 vector 元素, 地址稳定, 调用方挂起期间始终存活。
        // 注意: 不可按值传递 Task<T> 协程参数 (GCC 10 帧布局 double-free)。
        template <typename T>
        Task<void> gather_all_monitor(size_t index, Task<T>* task, std::shared_ptr<gather_all_state<T>> state) {
            try {
                state->results[index] = co_await std::move(*task);
            } catch (...) {
                if (!state->exc)
                    state->exc = std::current_exception();
            }
            if (--state->remaining == 0)
                EventLoop::get().schedule(state->cont);
        }
    } // namespace detail

    // ============================================================================
    // gather_all — 动态数量 gather
    // ============================================================================
    template <typename T> Task<std::vector<T>> gather_all(std::vector<Task<T>> tasks) {
        if (tasks.empty())
            co_return std::vector<T>{};

        auto s = std::make_shared<detail::gather_all_state<T>>();
        s->results.resize(tasks.size());
        s->remaining = tasks.size();

        // 每个 monitor 用命名协程函数 (参数进帧, 规避 MSVC Debug lambda 问题);
        // start + detach: monitor 帧自持有运行到完成, 无需堆上 Task 对象
        for (size_t i = 0; i < tasks.size(); i++) {
            Task<void> mon = detail::gather_all_monitor(i, &tasks[i], s);
            mon.start();
            mon.detach();
        }

        // 挂起调用方, 在 await_suspend 回调中设置 cont
        co_await suspend_awaiter{[s](std::coroutine_handle<> h) { s->cont = h; }};

        if (s->exc)
            std::rethrow_exception(s->exc);
        co_return std::move(s->results);
    }

    // ============================================================================
    // gather_void — 并发等待多个 Task<void>, 无返回值
    // ============================================================================
    //
    // 用法:
    //   co_await gather_void(task1(), task2(), task3());
    //
    // 与 gather 的区别: gather 返回 tuple<T...> 但 void 不能放入 tuple,
    //                 gather_void 专门处理 void task, 不返回结果。
    // ============================================================================

    namespace detail {
        // gather_void 的共享状态: 计数器 + 调用方句柄 + 第一个异常
        struct gather_void_state {
            std::atomic<size_t> count;
            std::coroutine_handle<> caller;
            std::exception_ptr first_exception; // 第一个异常 (全部完成后重新抛出)
        };

        // gather_void 的 monitor 协程: 指针方式 (同 wait_any_monitor)
        // 注意: inline — 非模板函数定义在头文件中, 多 TU 链接时必须 inline
        inline Task<void> gather_void_monitor(Task<void>* task, std::shared_ptr<gather_void_state> state) {
            // 无论成败都计数 (防止异常路径死锁);
            // 异常不吞: 记录第一个, 全部完成后由调用方重新抛出
            try {
                co_await std::move(*task);
            } catch (...) {
                if (!state->first_exception)
                    state->first_exception = std::current_exception();
            }
            if (--state->count == 0)
                EventLoop::get().schedule(state->caller);
        }

        template <typename... Ts> Task<void> gather_void_impl(std::tuple<Task<Ts>...> tasks) {
            auto state = std::make_shared<gather_void_state>();
            state->count = sizeof...(Ts);

            auto launch = [&]<size_t... Is>(std::index_sequence<Is...>) {
                (([&] {
                     Task<void> mon = gather_void_monitor(&std::get<Is>(tasks), state);
                     mon.start();
                     mon.detach(); // monitor 帧自持有运行到完成
                 }()),
                 ...);
            };
            launch(std::index_sequence_for<Ts...>{});

            // 挂起, 在 await_suspend 中设置 caller
            co_await suspend_awaiter{[&state](std::coroutine_handle<> h) { state->caller = h; }};

            // 所有任务结束后: 传播第一个异常 (对标 asyncio.gather 语义)
            if (state->first_exception)
                std::rethrow_exception(state->first_exception);
        }
    } // namespace detail

    template <typename... Ts> Task<void> gather_void(Task<Ts>... tasks) {
        return detail::gather_void_impl(std::make_tuple(std::move(tasks)...));
    }

    // ============================================================================
    // wait_tasks — N 路 wait (对标 Python asyncio.wait 的三种 return_when)
    // ============================================================================
    //
    //   enum class WaitMode { FirstCompleted, FirstException, AllCompleted };
    //   auto results = co_await coro::wait_tasks(std::move(tasks), mode);
    //
    // 三种模式:
    //   FirstCompleted — 任意一个任务完成 (成功或失败) 即返回;
    //                    返回 vector 只含那一个结果; 失败则抛它的异常。
    //                    其余任务继续在后台运行 (结果丢弃)。
    //   FirstException — 任一任务失败立即抛出该异常;
    //                    全部成功则等待全部完成, 返回全部结果。
    //   AllCompleted   — 等待全部完成 (等价 gather_all);
    //                    全部成功返回全部结果; 有失败抛第一个异常。
    //
    // 实现与 gather_all 同构 (monitor 协程模式), 见 detail::wait_tasks_monitor。
    // ============================================================================

    enum class WaitMode {
        FirstCompleted,
        FirstException,
        AllCompleted,
    };

    namespace detail {
        template <typename T> struct wait_tasks_state {
            std::vector<T> results;             // 按任务索引存储 (gather_all 模式)
            std::exception_ptr first_exception; // 第一个失败 (任意模式)
            std::atomic<size_t> remaining;      // 未完成任务数
            std::coroutine_handle<> caller;     // wait_tasks 的调用协程
            WaitMode mode;
            std::atomic<bool> early_done{false}; // 提前返回标记 (First* 模式只唤醒一次)
            size_t first_done_index = 0;         // FirstCompleted 下首个完成任务的索引
        };

        // monitor 协程: 指针方式 (同 wait_any_monitor)
        // task 指向调用方帧内局部 vector 元素, 地址稳定。
        // 注意: 不可按值传递 Task<T> 协程参数 (GCC 10 帧布局 double-free)。
        template <typename T>
        Task<void> wait_tasks_monitor(size_t index, Task<T>* task, std::shared_ptr<wait_tasks_state<T>> state) {
            bool task_failed = false;
            try {
                state->results[index] = co_await std::move(*task);
            } catch (...) {
                task_failed = true;
                if (!state->first_exception)
                    state->first_exception = std::current_exception();
            }

            const bool is_last = (--state->remaining == 0);
            bool should_notify = false;

            if (state->mode == WaitMode::FirstCompleted || state->mode == WaitMode::FirstException) {
                // First* 模式: 只唤醒一次 (第一个完成 / 第一个异常 / 恰好全部完成)。
                // early_done 保证 caller 完成后不再被唤醒 (防二次 schedule → UB)。
                bool wants_notify = state->mode == WaitMode::FirstCompleted ? true         // 任何完成都想唤醒
                                                                            : task_failed; // 只有失败想唤醒
                if (is_last || wants_notify) {
                    should_notify = !state->early_done.exchange(true);
                    if (should_notify && state->mode == WaitMode::FirstCompleted && !task_failed)
                        state->first_done_index = index;
                }
            } else {
                // AllCompleted: 全部完成才唤醒
                should_notify = is_last;
            }

            if (should_notify)
                EventLoop::get().schedule(state->caller);
            co_return;
        }

        template <typename T>
        Task<std::vector<T>> wait_tasks_impl(std::vector<Task<T>> tasks, WaitMode mode,
                                             std::shared_ptr<wait_tasks_state<T>> state) {
            if (tasks.empty())
                co_return std::vector<T>{};

            state->results.resize(tasks.size());
            state->remaining = tasks.size();
            state->mode = mode;

            for (size_t i = 0; i < tasks.size(); ++i) {
                Task<void> mon = wait_tasks_monitor(i, &tasks[i], state);
                mon.start();
                mon.detach(); // monitor 帧自持有运行到完成
            }

            // 挂起, 在 await_suspend 中设置 caller
            co_await suspend_awaiter{[state](std::coroutine_handle<> h) { state->caller = h; }};

            if (mode == WaitMode::FirstCompleted) {
                // 被唤醒: 要么第一个完成(成功/失败), 要么恰好全部完成。
                if (state->first_exception)
                    std::rethrow_exception(state->first_exception);
                // 只 move 首个结果 (其余任务继续后台运行, results 其余槽仍可写)
                T first = std::move(state->results[state->first_done_index]);
                co_return std::vector<T>{std::move(first)};
            }

            if (mode == WaitMode::FirstException && state->first_exception)
                std::rethrow_exception(state->first_exception);

            // AllCompleted / FirstException(全部成功)
            if (state->first_exception)
                std::rethrow_exception(state->first_exception);
            co_return std::move(state->results);
        }
    } // namespace detail

    /// N 路 wait (同类型任务), 对标 asyncio.wait(tasks, return_when=...)
    template <typename T>
    Task<std::vector<T>> wait_tasks(std::vector<Task<T>> tasks, WaitMode mode = WaitMode::AllCompleted) {
        auto state = std::make_shared<detail::wait_tasks_state<T>>();
        return detail::wait_tasks_impl<T>(std::move(tasks), mode, state);
    }

} // namespace coro
