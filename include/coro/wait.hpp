#pragma once

#include "task.hpp"
#include "sleep.hpp"
#include "sync.hpp"

#include <atomic>
#include <chrono>
#include <exception>
#include <mutex>
#include <memory>
#include <optional>
#include <stdexcept>
#include <tuple>
#include <utility>
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
        // wait_for 的定时器协程: 用命名函数而非临时捕获 lambda。
        // 参数作为协程参数被复制进帧, 生命周期由帧保证。
        //
        // task 指向调用方 wait_for 协程帧内的局部 Task (地址稳定):
        //   - 超时路径: wait_for 仍挂起在 co_await task 上 → 指针有效
        //   - 成功路径: wait_for 先 cancel+detach 定时器, 定时器恢复时
        //     在取消检查处抛 CancelledError, 不会执行到 task->cancel()
        //   - 异常展开路径: wait_for 帧销毁时局部 Task 析构 → 销毁挂起中
        //     的定时器帧 (sleep_awaiter 析构置位令牌, 定时器堆惰性跳过)
        template <typename T>
        Task<void> wait_for_timer_impl(Task<T>* task, std::chrono::milliseconds timeout,
                                       std::shared_ptr<std::atomic<bool>> timed_out) {
            co_await coro::sleep(timeout);
            timed_out->store(true, std::memory_order_release);
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
        auto timed_out = std::make_shared<std::atomic<bool>>(false);
        Task<void> timer = detail::wait_for_timer_impl<T>(&task, timeout_ms, timed_out);
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
            // 只有定时器先获胜才转换成 TimeoutError; 调用方主动 cancel
            // 必须保留 CancelledError, 否则上层无法区分超时和关停取消。
            if (timed_out->load(std::memory_order_acquire))
                throw TimeoutError{};
            throw;
        }
    }

    // wait_for 的 Task<void> 重载 (非模板, 优先于主模板匹配)
    // 主模板无法实例化 T=void (T result = co_await ... 对 void 非法)。
    template <typename Rep, typename Period>
    Task<void> wait_for(Task<void> task, std::chrono::duration<Rep, Period> timeout) {
        auto timeout_ms = std::chrono::duration_cast<std::chrono::milliseconds>(timeout);
        auto timed_out = std::make_shared<std::atomic<bool>>(false);
        Task<void> timer = detail::wait_for_timer_impl<void>(&task, timeout_ms, timed_out);
        timer.start();

        try {
            co_await task;  // 同主模板: 左值, 不移动
            timer.cancel(); // 成功路径: 主动取消 timer (见主模板注释)
            timer.detach();
        } catch (const CancelledError&) {
            if (timed_out->load(std::memory_order_acquire))
                throw TimeoutError{};
            throw;
        }
    }

    namespace detail {
        template <typename T> struct wait_any_state {
            mutable std::mutex mutex;
            std::optional<T> result;
            std::exception_ptr exc;
            std::coroutine_handle<> cont;
            EventLoop* cont_loop = nullptr;
            bool done = false;
            bool cont_dead = false;

            void install(std::coroutine_handle<> h, EventLoop* loop) {
                std::lock_guard lock(mutex);
                if (done || cont_dead)
                    loop->schedule(h);
                else {
                    cont = h;
                    cont_loop = loop;
                }
            }

            void abandon() noexcept {
                std::lock_guard lock(mutex);
                cont_dead = true;
                cont = nullptr;
                cont_loop = nullptr;
            }

            bool complete(T value) {
                std::coroutine_handle<> waiter;
                EventLoop* loop = nullptr;
                std::lock_guard lock(mutex);
                if (done)
                    return false;
                result.emplace(std::move(value));
                done = true;
                if (cont && !cont_dead) {
                    waiter = cont;
                    loop = cont_loop;
                    cont = nullptr;
                    cont_loop = nullptr;
                    loop->schedule(waiter);
                }
                return true;
            }

            bool fail(std::exception_ptr error) {
                std::coroutine_handle<> waiter;
                EventLoop* loop = nullptr;
                std::lock_guard lock(mutex);
                if (done)
                    return false;
                exc = std::move(error);
                done = true;
                if (cont && !cont_dead) {
                    waiter = cont;
                    loop = cont_loop;
                    cont = nullptr;
                    cont_loop = nullptr;
                    loop->schedule(waiter);
                }
                return true;
            }
        };

        // wait_any 的 monitor 协程: 命名函数 (参数进帧)。
        // 第一个完成的 monitor 设置 result 并唤醒调用方。
        // monitor 自持有 Task, 不能保存调用方协程帧中的裸指针。
        // wait_any 返回后其余 monitor 仍会继续运行, 因此 Task 必须在堆上
        // 由 shared_ptr 保活, 否则调用方被取消/销毁后会出现 UAF。
        template <typename T>
        Task<void> wait_any_monitor(std::shared_ptr<Task<T>> task, std::shared_ptr<wait_any_state<T>> state) {
            try {
                T val = co_await std::move(*task);
                state->complete(std::move(val));
            } catch (...) {
                state->fail(std::current_exception());
            }
        }
    } // namespace detail

    // ============================================================================
    // wait_any — FIRST_COMPLETED (两路竞速)
    // ============================================================================
    template <typename T> Task<T> wait_any(Task<T> t1, Task<T> t2) {
        auto state = std::make_shared<detail::wait_any_state<T>>();
        // 取消守卫: 调用方帧被取消销毁时, 置 cont_dead 防止 monitor schedule 悬空句柄
        struct _cancel_guard {
            std::shared_ptr<detail::wait_any_state<T>>& s;
            ~_cancel_guard() { s->abandon(); }
        } _g{state};

        // 将任务移到堆上交给 monitor 共享持有, 调用方帧提前销毁也安全。
        auto task1 = std::make_shared<Task<T>>(std::move(t1));
        auto task2 = std::make_shared<Task<T>>(std::move(t2));
        Task<void> m1 = detail::wait_any_monitor<T>(std::move(task1), state);
        Task<void> m2 = detail::wait_any_monitor<T>(std::move(task2), state);
        m1.start();
        m1.detach(); // fire-and-forget: monitor 帧自持有到完成
        m2.start();
        m2.detach();

        // 挂起调用方, 在 await_suspend 中设置 cont。
        // monitor 已加入就绪队列但尚未运行, 所以 cont 先于 monitor 完成被设置。
        co_await suspend_awaiter{[state](std::coroutine_handle<> h) { state->install(h, &EventLoop::get()); }};

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
        // N 路 monitor 也必须共享持有整个任务容器, 不能引用调用方帧。
        template <typename T>
        Task<void> wait_any_n_monitor(std::shared_ptr<std::vector<Task<T>>> tasks, size_t index,
                                      std::shared_ptr<wait_any_state<T>> state) {
            try {
                T val = co_await std::move((*tasks)[index]);
                state->complete(std::move(val));
            } catch (...) {
                state->fail(std::current_exception());
            }
        }
    } // namespace detail

    template <typename T> Task<T> wait_any(std::vector<Task<T>> tasks) {
        if (tasks.empty())
            throw std::invalid_argument("wait_any: empty task list");
        if (tasks.size() == 1)
            co_return co_await std::move(tasks[0]);

        auto state = std::make_shared<detail::wait_any_state<T>>();
        // 取消守卫 (见两路版注释)
        struct _cancel_guard {
            std::shared_ptr<detail::wait_any_state<T>>& s;
            ~_cancel_guard() { s->abandon(); }
        } _g{state};
        auto owned_tasks = std::make_shared<std::vector<Task<T>>>(std::move(tasks));
        for (size_t i = 0; i < owned_tasks->size(); i++) {
            Task<void> m = detail::wait_any_n_monitor<T>(owned_tasks, i, state);
            m.start();
            m.detach();
        }

        co_await suspend_awaiter{[state](std::coroutine_handle<> h) { state->install(h, &EventLoop::get()); }};

        if (state->exc)
            std::rethrow_exception(state->exc);
        co_return std::move(*state->result);
    }

    namespace detail {
        template <typename T> struct gather_all_state {
            mutable std::mutex mutex;
            // optional 槽位避免要求 T 可默认构造；任务完成后再逐项 move 到结果。
            std::vector<std::optional<T>> results;
            size_t remaining = 0;
            std::coroutine_handle<> cont;
            EventLoop* cont_loop = nullptr;
            std::exception_ptr exc;
            bool cont_dead = false;

            void install(std::coroutine_handle<> h, EventLoop* loop) {
                std::lock_guard lock(mutex);
                if (remaining == 0 || cont_dead)
                    loop->schedule(h);
                else {
                    cont = h;
                    cont_loop = loop;
                }
            }

            void abandon() noexcept {
                std::lock_guard lock(mutex);
                cont_dead = true;
                cont = nullptr;
                cont_loop = nullptr;
            }

            void complete(size_t index, std::optional<T> value, std::exception_ptr error) {
                std::coroutine_handle<> waiter;
                EventLoop* loop = nullptr;
                std::lock_guard lock(mutex);
                if (error) {
                    if (!exc)
                        exc = std::move(error);
                } else {
                    results[index].emplace(std::move(*value));
                }
                if (--remaining == 0 && cont && !cont_dead) {
                    waiter = cont;
                    loop = cont_loop;
                    cont = nullptr;
                    cont_loop = nullptr;
                    loop->schedule(waiter);
                }
            }

            std::vector<T> take_results() {
                std::lock_guard lock(mutex);
                std::vector<T> out;
                out.reserve(results.size());
                for (auto& value : results)
                    out.push_back(std::move(*value));
                return out;
            }
        };

        // gather_all 的 monitor 共享持有任务容器, 防止调用方取消后容器失效。
        template <typename T>
        Task<void> gather_all_monitor(size_t index, std::shared_ptr<std::vector<Task<T>>> tasks,
                                      std::shared_ptr<gather_all_state<T>> state) {
            std::optional<T> value;
            std::exception_ptr error;
            try {
                value.emplace(co_await std::move((*tasks)[index]));
            } catch (...) {
                error = std::current_exception();
            }
            state->complete(index, std::move(value), std::move(error));
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
        auto owned_tasks = std::make_shared<std::vector<Task<T>>>(std::move(tasks));
        // 取消守卫: 调用方帧被取消销毁时, 置 cont_dead 防止 monitor schedule 悬空句柄
        struct _cancel_guard {
            std::shared_ptr<detail::gather_all_state<T>>& s;
            ~_cancel_guard() { s->abandon(); }
        } _g{s};

        // 每个 monitor 用命名协程函数 (参数进帧, 避免临时闭包悬空);
        // start + detach: monitor 帧自持有运行到完成, 无需堆上 Task 对象
        for (size_t i = 0; i < owned_tasks->size(); i++) {
            Task<void> mon = detail::gather_all_monitor(i, owned_tasks, s);
            mon.start();
            mon.detach();
        }

        // 挂起调用方, 在 await_suspend 回调中设置 cont
        co_await suspend_awaiter{[s](std::coroutine_handle<> h) { s->install(h, &EventLoop::get()); }};

        if (s->exc)
            std::rethrow_exception(s->exc);
        co_return s->take_results();
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
            mutable std::mutex mutex;
            size_t count = 0;
            std::coroutine_handle<> caller;
            EventLoop* caller_loop = nullptr;
            std::exception_ptr first_exception; // 第一个异常 (全部完成后重新抛出)
            bool cont_dead = false;

            void install(std::coroutine_handle<> h, EventLoop* loop) {
                std::lock_guard lock(mutex);
                if (count == 0 || cont_dead)
                    loop->schedule(h);
                else {
                    caller = h;
                    caller_loop = loop;
                }
            }

            void abandon() noexcept {
                std::lock_guard lock(mutex);
                cont_dead = true;
                caller = nullptr;
                caller_loop = nullptr;
            }

            void complete(std::exception_ptr error) {
                std::coroutine_handle<> waiter;
                EventLoop* loop = nullptr;
                std::lock_guard lock(mutex);
                if (error && !first_exception)
                    first_exception = std::move(error);
                if (--count == 0 && caller && !cont_dead) {
                    waiter = caller;
                    loop = caller_loop;
                    caller = nullptr;
                    caller_loop = nullptr;
                    loop->schedule(waiter);
                }
            }
        };

        // gather_void 的 monitor 共享持有 tuple, 防止调用方协程帧提前销毁。
        template <typename Tuple, size_t I>
        Task<void> gather_void_monitor(std::shared_ptr<Tuple> tasks, std::shared_ptr<gather_void_state> state) {
            // 无论成败都计数 (防止异常路径死锁);
            // 异常不吞: 记录第一个, 全部完成后由调用方重新抛出
            std::exception_ptr error;
            try {
                co_await std::move(std::get<I>(*tasks));
            } catch (...) {
                error = std::current_exception();
            }
            state->complete(std::move(error));
        }

        template <typename... Ts> Task<void> gather_void_impl(std::tuple<Task<Ts>...> tasks) {
            if constexpr (sizeof...(Ts) == 0) {
                co_return;
            }
            auto state = std::make_shared<gather_void_state>();
            state->count = sizeof...(Ts);
            auto owned_tasks = std::make_shared<std::tuple<Task<Ts>...>>(std::move(tasks));
            // 取消守卫: 调用方帧被取消销毁时, 置 cont_dead 防止 monitor schedule 悬空句柄
            struct _cancel_guard {
                std::shared_ptr<gather_void_state>& s;
                ~_cancel_guard() { s->abandon(); }
            } _g{state};

            auto launch = [&]<size_t... Is>(std::index_sequence<Is...>) {
                (([&] {
                     using Tuple = std::tuple<Task<Ts>...>;
                     Task<void> mon = gather_void_monitor<Tuple, Is>(owned_tasks, state);
                     mon.start();
                     mon.detach(); // monitor 帧自持有运行到完成
                 }()),
                 ...);
            };
            launch(std::index_sequence_for<Ts...>{});

            // 挂起, 在 await_suspend 中设置 caller
            co_await suspend_awaiter{[&state](std::coroutine_handle<> h) { state->install(h, &EventLoop::get()); }};

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
            mutable std::mutex mutex;
            std::vector<std::optional<T>> results;        // optional 避免要求 T 可默认构造
            std::exception_ptr first_exception;           // 第一个失败 (任意模式)
            std::exception_ptr first_completed_exception; // FirstCompleted 首项自己的失败
            size_t remaining = 0;                         // 未完成任务数
            std::coroutine_handle<> caller;               // wait_tasks 的调用协程
            EventLoop* caller_loop = nullptr;
            WaitMode mode = WaitMode::AllCompleted;
            bool early_done = false;     // First* 模式只唤醒一次
            size_t first_done_index = 0; // FirstCompleted 下首个完成任务的索引
            bool cont_dead = false;

            void install(std::coroutine_handle<> h, EventLoop* loop) {
                std::lock_guard lock(mutex);
                const bool first_ready = mode != WaitMode::AllCompleted && early_done;
                if (remaining == 0 || first_ready || cont_dead)
                    loop->schedule(h);
                else {
                    caller = h;
                    caller_loop = loop;
                }
            }

            void abandon() noexcept {
                std::lock_guard lock(mutex);
                cont_dead = true;
                caller = nullptr;
                caller_loop = nullptr;
            }

            void complete(size_t index, bool task_failed, std::optional<T> value, std::exception_ptr error) {
                std::coroutine_handle<> waiter;
                EventLoop* loop = nullptr;
                std::lock_guard lock(mutex);
                if (value)
                    results[index].emplace(std::move(*value));
                if (error && !first_exception)
                    first_exception = error;

                const bool is_last = (--remaining == 0);
                bool should_notify = false;
                if (mode == WaitMode::FirstCompleted || mode == WaitMode::FirstException) {
                    const bool wants_notify = mode == WaitMode::FirstCompleted ? true : task_failed;
                    if (is_last || wants_notify) {
                        should_notify = !early_done;
                        early_done = true;
                        if (should_notify && mode == WaitMode::FirstCompleted) {
                            first_done_index = index;
                            first_completed_exception = error;
                        }
                    }
                } else {
                    should_notify = is_last;
                }
                if (should_notify && caller && !cont_dead) {
                    waiter = caller;
                    loop = caller_loop;
                    caller = nullptr;
                    caller_loop = nullptr;
                    loop->schedule(waiter);
                }
            }

            std::vector<T> take_first_completed() {
                std::lock_guard lock(mutex);
                if (first_completed_exception)
                    std::rethrow_exception(first_completed_exception);
                std::vector<T> out;
                out.reserve(1);
                out.push_back(std::move(*results[first_done_index]));
                return out;
            }

            std::vector<T> take_all_results() {
                std::lock_guard lock(mutex);
                std::vector<T> out;
                out.reserve(results.size());
                for (auto& value : results)
                    out.push_back(std::move(*value));
                return out;
            }
        };

        // monitor 共享持有任务容器, wait_tasks 的调用方即使被取消并销毁
        // 也不会让后台 monitor 解引用悬空的 vector 元素。
        template <typename T>
        Task<void> wait_tasks_monitor(size_t index, std::shared_ptr<std::vector<Task<T>>> tasks,
                                      std::shared_ptr<wait_tasks_state<T>> state) {
            bool task_failed = false;
            std::optional<T> value;
            std::exception_ptr error;
            try {
                value.emplace(co_await std::move((*tasks)[index]));
            } catch (...) {
                task_failed = true;
                error = std::current_exception();
            }
            state->complete(index, task_failed, std::move(value), std::move(error));
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
            auto owned_tasks = std::make_shared<std::vector<Task<T>>>(std::move(tasks));
            // 取消守卫: 调用方帧被取消销毁时, 置 cont_dead 防止 monitor schedule 悬空句柄
            struct _cancel_guard {
                std::shared_ptr<wait_tasks_state<T>>& s;
                ~_cancel_guard() { s->abandon(); }
            } _g{state};

            for (size_t i = 0; i < owned_tasks->size(); ++i) {
                Task<void> mon = wait_tasks_monitor(i, owned_tasks, state);
                mon.start();
                mon.detach(); // monitor 帧自持有运行到完成
            }

            // 挂起, 在 await_suspend 中设置 caller
            co_await suspend_awaiter{[state](std::coroutine_handle<> h) { state->install(h, &EventLoop::get()); }};

            if (mode == WaitMode::FirstCompleted) {
                // 被唤醒: 要么第一个完成(成功/失败), 要么恰好全部完成。
                // 只取首个完成项自己的结果。后续任务在调用方恢复前失败时，
                // 不能把那个较晚的异常误判成 FirstCompleted 的结果。
                co_return state->take_first_completed();
            }

            if (mode == WaitMode::FirstException && state->first_exception)
                std::rethrow_exception(state->first_exception);

            // AllCompleted / FirstException(全部成功)
            if (state->first_exception)
                std::rethrow_exception(state->first_exception);
            co_return state->take_all_results();
        }
    } // namespace detail

    /// N 路 wait (同类型任务), 对标 asyncio.wait(tasks, return_when=...)
    template <typename T>
    Task<std::vector<T>> wait_tasks(std::vector<Task<T>> tasks, WaitMode mode = WaitMode::AllCompleted) {
        auto state = std::make_shared<detail::wait_tasks_state<T>>();
        return detail::wait_tasks_impl<T>(std::move(tasks), mode, state);
    }

} // namespace coro
