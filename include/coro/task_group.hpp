#pragma once

#include "task.hpp"
#include "exceptions.hpp"

#include <atomic>
#include <memory>
#include <mutex>
#include <vector>

// ============================================================================
// coro::TaskGroup — 结构化并发 (对标 Python 3.11 asyncio.TaskGroup)
// ============================================================================
//
// 用法 (对标):
//
//   async with asyncio.TaskGroup() as tg:      coro::TaskGroup group;
//       tg.create_task(fetch(a))               group.spawn(fetch_a());
//       tg.create_task(fetch(b))               group.spawn(fetch_b());
//   # 退出时自动等待全部                     co_await group.wait();  // 显式收尾
//
// 语义 (与 Python TaskGroup 一致):
//   1. wait() 等待所有子任务结束 (成功或失败)
//   2. 任一子任务失败 → 立即取消其余所有子任务
//   3. 全部结束后:
//        - 无失败 → 正常返回
//        - 有失败 → 抛 ExceptionGroup (即使只有 1 个异常也打包, 统一处理路径)
//   4. 组内取消引起的 CancelledError 不聚合进 ExceptionGroup
//
// 设计说明 (可扩展性):
//   - 每个子任务配一个 monitor 协程 (命名函数, 参数进帧): 负责记录异常、
//     响应组取消 (转发给原任务)、计数递减唤醒 wait 者。
//   - 组只持有 monitor 句柄 (类型统一为 Task<void>), 因此可以接受任意
//     Task<T> 的子任务而无需类型擦除技巧。
//   - 析构兜底: 若用户忘记 wait(), 自动取消残留子任务 (防孤儿任务)。
//
// 注意:
//   - spawn 不返回子任务句柄 (Python 用法中通常也无需 await 组内任务);
//     子任务需要对外产生效果时, 请通过参数/共享状态传递。
//   - 一个 TaskGroup 只能 wait() 一次: 组有显式状态机 open -> closing -> finished,
//     wait()/cancel() 之后再 spawn 会抛 StructuredConcurrencyError, 重复 wait() 同理。
// ============================================================================

namespace coro {

    class TaskGroup;

    namespace detail {

        // ==================================================================
        // task_group_state — TaskGroup 与所有 monitor 协程的共享状态
        // ==================================================================
        struct task_group_state {
            mutable std::mutex mutex;
            size_t remaining = 0;           // 未完成子任务数
            std::coroutine_handle<> waiter; // wait() 的调用协程
            EventLoop* waiter_loop = nullptr;
            std::vector<std::exception_ptr> exceptions;        // 子任务失败集合 (事件循环线程)
            std::vector<std::shared_ptr<Task<void>>> monitors; // 用于组取消
            bool cancel_requested = false;                     // 已有失败, 正在取消其余

            // 组生命周期状态机: open -> closing (已请求收尾) -> finished (子任务全结束)。
            // 与 mutex 同域读写: wait() 一旦进入 closing 就拒绝新任务与第二个 waiter,
            // 否则后到的 waiter 会覆盖前一个, 让先等待的协程永久挂起。
            enum group_phase { open = 0, closing = 1, finished = 2 };
            int phase = open;
            // wait() 只能用一个 waiter, 但 cancel() 不占用它: 先 cancel 再 wait 收尾是
            // 合法顺序, 两者必须分开记录, 否则 cancel 会让组再也无法收尾。
            bool wait_used = false;

            bool waiter_ready() const noexcept {
                std::lock_guard lock(mutex);
                return remaining == 0;
            }

            void install_waiter(std::coroutine_handle<> h, EventLoop* loop) {
                std::lock_guard lock(mutex);
                if (remaining == 0)
                    loop->schedule(h);
                else {
                    waiter = h;
                    waiter_loop = loop;
                }
            }

            void abandon_waiter() noexcept {
                std::lock_guard lock(mutex);
                waiter = nullptr;
                waiter_loop = nullptr;
            }

            std::vector<std::shared_ptr<Task<void>>> record_failure(std::exception_ptr error,
                                                                    const std::shared_ptr<Task<void>>& self) {
                std::vector<std::shared_ptr<Task<void>>> to_cancel;
                std::lock_guard lock(mutex);
                exceptions.push_back(std::move(error));
                if (!cancel_requested) {
                    cancel_requested = true;
                    for (auto& m : monitors) {
                        if (m && m != self && !m->is_ready())
                            to_cancel.push_back(m);
                    }
                }
                return to_cancel;
            }

            void record_exception(std::exception_ptr error) {
                std::lock_guard lock(mutex);
                exceptions.push_back(std::move(error));
            }

            void complete() {
                std::coroutine_handle<> h;
                EventLoop* loop = nullptr;
                std::lock_guard lock(mutex);
                if (--remaining == 0 && waiter) {
                    h = waiter;
                    loop = waiter_loop;
                    waiter = nullptr;
                    waiter_loop = nullptr;
                    loop->schedule(h);
                }
            }

            size_t pending() const noexcept {
                std::lock_guard lock(mutex);
                return remaining;
            }
        };

        template <typename T>
        Task<void> task_group_monitor(std::shared_ptr<Task<void>> self, std::shared_ptr<Task<T>> task_ptr,
                                      std::shared_ptr<task_group_state> state);

        // ==================================================================
        // task_group_wait_awaiter — co_await group.wait() 的 awaiter
        // ==================================================================
        struct task_group_wait_awaiter {
            std::shared_ptr<task_group_state> st;

            bool await_ready() const noexcept {
                return st->waiter_ready(); // 全部完成 (含空组): 不挂起
            }

            void await_suspend(std::coroutine_handle<> h) { st->install_waiter(h, &EventLoop::get()); }

            void await_resume() {
                st->abandon_waiter();
                std::vector<std::exception_ptr> exceptions;
                {
                    std::lock_guard lock(st->mutex);
                    exceptions = std::move(st->exceptions);
                    st->phase = detail::task_group_state::finished;
                }
                // 有失败 → 抛聚合异常 (单个异常也打包, 与 Python 一致)
                if (!exceptions.empty()) {
                    throw ExceptionGroup(std::move(exceptions));
                }
            }

            /// 等待者被取消/销毁 (框架的取消注入会绕过 await_resume):
            /// 1) 摘除 waiter —— 否则最后一个 monitor 完成时会 schedule 一个
            ///    已销毁 (或已 catch 取消继续跑) 的句柄, 前者是 UB, 后者是错误唤醒;
            /// 2) 取消传染给全部子任务 —— 对齐 Python TaskGroup:
            ///    `async with tg` 所在任务被取消时, 子任务一并被取消。
            void on_waiter_destroyed(std::coroutine_handle<> h) noexcept {
                std::vector<std::shared_ptr<Task<void>>> to_cancel;
                {
                    std::lock_guard lock(st->mutex);
                    if (st->waiter == h) {
                        st->waiter = nullptr;
                        st->waiter_loop = nullptr;
                    }
                    for (auto& m : st->monitors) {
                        if (m && !m->is_ready())
                            to_cancel.push_back(m);
                    }
                }
                // 取消动作放在锁外 (与 record_failure 的约定一致, 避免锁反转)
                for (auto& m : to_cancel)
                    m->cancel();
            }
        };

    } // namespace detail

    class TaskGroup {
      public:
        TaskGroup() = default;

        /// 析构兜底: 只**请求**取消未完成的子任务 (用户忘记 wait() 时防孤儿)。
        /// 这里不做任何等待, 也不谎称收尾已完成 —— 异步收尾只能通过 co_await wait()。
        ~TaskGroup() {
            std::vector<std::shared_ptr<Task<void>>> monitors;
            {
                std::lock_guard lock(state_->mutex);
                monitors = state_->monitors;
            }
            for (auto& m : monitors) {
                if (m && !m->is_ready())
                    m->cancel();
            }
        }

        TaskGroup(const TaskGroup&) = delete;
        TaskGroup& operator=(const TaskGroup&) = delete;

        /// 添加并立即启动一个子任务 (对标 tg.create_task)
        /// 任意 Task<T> / Task<> 均可, 返回类型不要求一致
        template <typename T> void spawn(Task<T> task) {
            {
                std::lock_guard lock(state_->mutex);
                if (state_->phase != detail::task_group_state::open)
                    throw StructuredConcurrencyError("TaskGroup::spawn called after wait()/cancel()");
            }
            // 原任务移入 shared_ptr (稳定地址, monitor 与它共享所有权)
            auto task_ptr = std::make_shared<Task<T>>(std::move(task));
            // monitor 协程: 包装原任务, 记录异常 / 响应组取消
            auto mon = std::make_shared<Task<void>>();
            *mon = detail::task_group_monitor(mon, task_ptr, state_);
            {
                std::lock_guard lock(state_->mutex);
                state_->monitors.push_back(mon);
                ++state_->remaining;
            }
            mon->start();
        }

        /// 等待所有子任务完成 (对标 async with 块退出)。
        /// 返回可 co_await 的对象; 有失败时在 await_resume 抛 ExceptionGroup。
        auto wait() {
            {
                std::lock_guard lock(state_->mutex);
                if (state_->wait_used)
                    throw StructuredConcurrencyError("TaskGroup::wait may be used only once");
                state_->wait_used = true;
                state_->phase = detail::task_group_state::closing;
            }
            return detail::task_group_wait_awaiter{state_};
        }

        /// 显式请求取消组内全部未完成子任务 (非阻塞)。必须在组所属事件循环线程调用,
        /// 因为 Task::cancel 的唤醒会投递到当前循环。取消本身不是收尾: 仍需 co_await
        /// wait() 才能确认全部结束 —— 与析构一样只"请求", 不假装完成异步等待。
        void cancel() {
            std::vector<std::shared_ptr<Task<void>>> monitors;
            {
                std::lock_guard lock(state_->mutex);
                if (state_->phase == detail::task_group_state::open)
                    state_->phase = detail::task_group_state::closing;
                monitors = state_->monitors;
            }
            for (auto& m : monitors) {
                if (m && !m->is_ready())
                    m->cancel();
            }
        }

        /// 是否仍可 spawn 新任务 (open 状态)
        bool accepts_new_tasks() const {
            std::lock_guard lock(state_->mutex);
            return state_->phase == detail::task_group_state::open;
        }

        /// 尚未完成的子任务数
        size_t pending_count() const noexcept { return state_->pending(); }

        /// 组是否已进入终态 (所有子任务结束)
        bool done() const noexcept { return state_->pending() == 0; }

      private:
        std::shared_ptr<detail::task_group_state> state_ = std::make_shared<detail::task_group_state>();
    };

    namespace detail {

        // ==================================================================
        // task_group_monitor — 单个子任务的监控协程 (命名函数, 参数进帧)
        // ==================================================================
        //
        // 三种结束路径:
        //   1. 原任务成功完成        → 计数 -1
        //   2. 原任务抛异常 (非取消) → 记录异常, 触发组取消
        //   3. 自己被组取消          → 把取消转发给原任务, 等它结束
        // ==================================================================
        template <typename T>
        Task<void> task_group_monitor(std::shared_ptr<Task<void>> self, std::shared_ptr<Task<T>> task_ptr,
                                      std::shared_ptr<task_group_state> state) {
            bool forward_cancel = false;
            try {
                (void)co_await *task_ptr; // 左值: 不移动原任务
            } catch (const CancelledError&) {
                // 组取消到达本 monitor。注意: MSVC 不允许在 catch 块内
                // co_await, 所以只记标志, 转发逻辑移到块外。
                forward_cancel = true;
            } catch (...) {
                // 原任务真实失败: 记录 + 触发组取消
                auto to_cancel = state->record_failure(std::current_exception(), self);
                // 注意: 必须跳过自己 (record_failure 已过滤)。取消动作放在
                // mutex 外执行, 避免 cancel 路径回调状态时形成锁反转。
                for (auto& m : to_cancel)
                    m->cancel();
            }

            if (forward_cancel) {
                // 把取消转发给原任务, 等它结束 (此时不在 catch 块内)
                task_ptr->cancel();
                try {
                    co_await *task_ptr;
                } catch (const CancelledError&) {
                    // 转发取消成功: 任务以取消结束, 不聚合
                } catch (...) {
                    // 任务在取消生效前已自行失败: 它的异常仍然聚合
                    // (对标 Python: 已失败任务的异常不会被取消抹掉)
                    state->record_exception(std::current_exception());
                }
            }

            // 全部结束: 唤醒 wait() 调用者
            state->complete();
            co_return;
        }

    } // namespace detail

} // namespace coro
