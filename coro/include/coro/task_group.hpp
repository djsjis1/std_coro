#pragma once

#include "task.hpp"
#include "exceptions.hpp"

#include <atomic>
#include <iostream>
#include <memory>
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
//   - 一个 TaskGroup 只能 wait() 一次 (之后组进入终态)。
// ============================================================================

namespace coro {

    class TaskGroup;

    namespace detail {

        // ==================================================================
        // task_group_state — TaskGroup 与所有 monitor 协程的共享状态
        // ==================================================================
        struct task_group_state {
            std::atomic<size_t> remaining{0};                  // 未完成子任务数
            std::coroutine_handle<> waiter;                    // wait() 的调用协程
            std::vector<std::exception_ptr> exceptions;        // 子任务失败集合 (事件循环线程)
            std::vector<std::shared_ptr<Task<void>>> monitors; // 用于组取消
            bool cancel_requested = false;                     // 已有失败, 正在取消其余
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
                return st->remaining == 0; // 全部完成 (含空组): 不挂起
            }

            void await_suspend(std::coroutine_handle<> h) { st->waiter = h; }

            void await_resume() {
                // 有失败 → 抛聚合异常 (单个异常也打包, 与 Python 一致)
                if (!st->exceptions.empty()) {
                    throw ExceptionGroup(std::move(st->exceptions));
                }
            }
        };

    } // namespace detail

    class TaskGroup {
      public:
        TaskGroup() = default;

        /// 析构兜底: 取消所有未完成的子任务 (用户忘记 wait() 时防孤儿)
        ~TaskGroup() {
            if (state_->remaining > 0) {
                for (auto& m : state_->monitors) {
                    if (m && !m->is_ready())
                        m->cancel();
                }
            }
        }

        TaskGroup(const TaskGroup&) = delete;
        TaskGroup& operator=(const TaskGroup&) = delete;

        /// 添加并立即启动一个子任务 (对标 tg.create_task)
        /// 任意 Task<T> / Task<> 均可, 返回类型不要求一致
        template <typename T> void spawn(Task<T> task) {
            // 原任务移入 shared_ptr (稳定地址, monitor 与它共享所有权)
            auto task_ptr = std::make_shared<Task<T>>(std::move(task));
            // monitor 协程: 包装原任务, 记录异常 / 响应组取消
            auto mon = std::make_shared<Task<void>>();
            *mon = detail::task_group_monitor(mon, task_ptr, state_);
            state_->monitors.push_back(mon);
            ++state_->remaining;
            mon->start();
        }

        /// 等待所有子任务完成 (对标 async with 块退出)。
        /// 返回可 co_await 的对象; 有失败时在 await_resume 抛 ExceptionGroup。
        auto wait() { return detail::task_group_wait_awaiter{state_}; }

        /// 尚未完成的子任务数
        size_t pending_count() const noexcept { return state_->remaining.load(); }

        /// 组是否已进入终态 (所有子任务结束)
        bool done() const noexcept { return state_->remaining == 0; }

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
                state->exceptions.push_back(std::current_exception());
                if (!state->cancel_requested) {
                    state->cancel_requested = true;
                    for (auto& m : state->monitors) {
                        // 注意: 必须跳过自己 (m != self)!
                        // 若取消自己 → 自己完成后帧销毁, 随后被 resume 已销毁帧 → UB
                        if (m && m != self && !m->is_ready())
                            m->cancel(); // 取消其余子任务
                    }
                }
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
                    state->exceptions.push_back(std::current_exception());
                }
            }

            if (--state->remaining == 0) {
                // 全部结束: 唤醒 wait() 调用者
                if (state->waiter)
                    EventLoop::get().schedule(state->waiter);
            }
            co_return;
        }

    } // namespace detail

} // namespace coro
