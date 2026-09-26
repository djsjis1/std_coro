#pragma once

#include "event_loop.hpp"
#include "sleep.hpp"
#include "task.hpp"

#include <atomic>
#include <chrono>
#include <coroutine>
#include <exception>
#include <memory>
#include <mutex>
#include <vector>

// ============================================================================
// coro::detail::task_registry — 服务组件的任务所有权登记表
// ============================================================================
//
// 为什么需要它: TcpServer/UdpServer/Web 都是"一个连接(数据报/请求)一个协程"的
// 模型, 过去用 spawn(...).detach() 派生任务, 帧没有所有者、结束与否无人知晓,
// 于是 stop() 只能顺手停掉全局 EventLoop 来"假装"收尾。本登记表把这三件事补齐:
//   1. 所有权: 任务帧由 registry 持有 shared_ptr, 用户拿到移动专属 lease;
//   2. 计数与排空: 无论正常结束、被取消还是抛异常, 都由 RAII 摘除并计数;
//   3. 收尾: 提供 settle(grace) —— 宽限期到点才请求取消, 不配合取消的任务如实
//      报告未完成, 绝不强行销毁仍在运行的协程帧。
//
// 边界 (刻意为之, 防止长成大杂烩):
//   - 不理解连接/数据报/HTTP 请求等业务概念, 只认 Task<> 与容量;
//   - 不做任何基类, 不与具体 Server 共享继承层次;
//   - 容量策略只返回"拒绝", 丢弃计数与后续动作由所有者决定。
//
// 线程亲缘: spawn/lease 可在任意线程调用; 任务帧的唤醒始终投递回它自己所属的
// EventLoop (由 spawn 时的当前循环记录), 因此跨 worker 关闭是安全的。
// ============================================================================

namespace coro {
    namespace detail {

        /// settle() 的结果: 供调用方决定要不要继续等待/上报
        struct shutdown_report {
            bool drained = false;  // 是否全部任务真正结束
            size_t unfinished = 0; // 到点仍未结束的任务数 (不配合取消的业务代码)
        };

        /// 单个任务的身份与所有权: 帧由登记表持有, owner 记录取消唤醒要投递到哪个循环。
        struct registry_entry {
            std::shared_ptr<Task<void>> task;
            EventLoop* owner = nullptr;
            std::atomic<bool> finished{false};
        };

        /// 登记表与所有任务帧共享的状态。所有者复制的是 shared_ptr, 因此即使 Server
        /// 对象先于滞留任务析构, 摘除与计数依然安全 (不依赖 Server 的 this)。
        struct registry_state {
            mutable std::mutex mutex;
            std::vector<std::shared_ptr<registry_entry>> entries;
            std::atomic<size_t> active{0};
            std::atomic<size_t> rejected{0};
            std::atomic<size_t> exceptions{0};
            size_t capacity = 0;

            void finish(const std::shared_ptr<registry_entry>& e) {
                {
                    std::lock_guard<std::mutex> lock(mutex);
                    for (auto it = entries.begin(); it != entries.end(); ++it) {
                        if (*it == e) {
                            entries.erase(it);
                            break;
                        }
                    }
                }
                e->finished.store(true);
                active.fetch_sub(1);
            }
        };

        class task_registry {
          public:
            /// 任务的移动专属句柄。析构只放弃所有权, 不阻塞、也不隐含取消 ——
            /// 取消必须显式 cancel(), 收尾由 settle() 负责。
            class lease {
              public:
                lease() = default;
                lease(const lease&) = delete;
                lease& operator=(const lease&) = delete;
                lease(lease&&) noexcept = default;
                lease& operator=(lease&&) noexcept = default;
                ~lease() = default;

                /// 句柄是否指向一个已登记的任务
                bool valid() const noexcept { return static_cast<bool>(entry_); }

                /// 任务是否已真正结束 (含被取消/抛异常)
                bool done() const noexcept { return !entry_ || entry_->finished.load(); }

                /// 请求取消: 唤醒投递回任务所属 EventLoop, 绝不在别处直接 cancel()
                void cancel() const {
                    if (!entry_ || entry_->finished.load() || !entry_->task)
                        return;
                    auto task = entry_->task;
                    auto* loop = entry_->owner;
                    if (loop != nullptr)
                        loop->dispatch([task] { task->cancel(); });
                    else
                        task->cancel();
                }

              private:
                friend class task_registry;
                explicit lease(std::shared_ptr<registry_entry> e) : entry_(std::move(e)) {}
                std::shared_ptr<registry_entry> entry_;
            };

            /// capacity = 0 表示不限制并发任务数
            explicit task_registry(size_t capacity = 0) : state_(std::make_shared<registry_state>()) {
                state_->capacity = capacity;
            }

            task_registry(const task_registry&) = delete;
            task_registry& operator=(const task_registry&) = delete;

            /// 派生并登记一个任务。达到容量上限时返回无效 lease 并累加 rejected 计数。
            /// 接受 factory 而非已构造的 Task: 被拒绝时 factory 根本不会被调用,
            /// 调用方因此能安全地继续使用/回收已准备好的资源 (如已 accept 的连接)。
            template <typename Factory> lease spawn(Factory&& factory) {
                auto e = std::make_shared<registry_entry>();
                {
                    std::lock_guard<std::mutex> lock(state_->mutex);
                    if (state_->capacity != 0 && state_->entries.size() >= state_->capacity) {
                        ++state_->rejected;
                        return lease{};
                    }
                    e->owner = &EventLoop::get();
                    state_->entries.push_back(e);
                    state_->active.fetch_add(1);
                }
                auto holder = std::make_shared<Task<void>>();
                e->task = holder;
                *holder = runner(state_, e, factory());
                holder->start();
                return lease(e);
            }

            /// 尚未结束的任务数
            size_t size() const noexcept { return state_->active.load(); }

            bool empty() const noexcept { return size() == 0; }

            /// 对所有存活任务请求取消 (非阻塞, 唤醒各自回到所属循环)
            void request_cancel_all() {
                std::vector<std::shared_ptr<registry_entry>> snapshot;
                {
                    std::lock_guard<std::mutex> lock(state_->mutex);
                    snapshot = state_->entries;
                }
                for (const auto& e : snapshot) {
                    if (!e || e->finished.load() || !e->task)
                        continue;
                    auto task = e->task;
                    auto* loop = e->owner;
                    if (loop != nullptr)
                        loop->dispatch([task] { task->cancel(); });
                    else
                        task->cancel();
                }
            }

            void set_capacity(size_t capacity) {
                std::lock_guard<std::mutex> lock(state_->mutex);
                state_->capacity = capacity;
            }

            size_t capacity() const {
                std::lock_guard<std::mutex> lock(state_->mutex);
                return state_->capacity;
            }

            /// 因容量上限而被拒绝的次数
            size_t rejected() const noexcept { return state_->rejected.load(); }

            /// 未被任务自身处理、由登记表兜底捕获的异常数
            size_t exception_count() const noexcept { return state_->exceptions.load(); }

            /// 等待全部任务结束: 宽限期内不干预; 到点才请求取消, 再给 cancel_grace
            /// 收尾; 仍未结束就如实报告 unfinished —— grace 是"何时开始取消"的时刻,
            /// 不是"可以强行销毁协程帧"的时刻。轮询 1ms 只用于关停路径, 不在热路径。
            template <typename Rep1, typename Period1, typename Rep2, typename Period2>
            Task<shutdown_report> settle(std::chrono::duration<Rep1, Period1> grace,
                                         std::chrono::duration<Rep2, Period2> cancel_grace) {
                shutdown_report report;
                const auto drain_deadline = std::chrono::steady_clock::now() + grace;
                while (!empty() && std::chrono::steady_clock::now() < drain_deadline)
                    co_await coro::sleep(std::chrono::milliseconds(1));
                if (empty()) {
                    report.drained = true;
                    co_return report;
                }
                request_cancel_all();
                const auto cancel_deadline = std::chrono::steady_clock::now() + cancel_grace;
                while (!empty() && std::chrono::steady_clock::now() < cancel_deadline)
                    co_await coro::sleep(std::chrono::milliseconds(1));
                report.drained = empty();
                report.unfinished = size();
                co_return report;
            }

          private:
            /// 包装协程: 正常结束 / 被取消 / 抛异常, 都必须摘除自己并计数 ——
            /// 这是"用户 handler 抛异常也不漏计数与资源释放"的保证点。
            static Task<void> runner(std::shared_ptr<registry_state> st, std::shared_ptr<registry_entry> e,
                                     Task<void> body) {
                struct scope {
                    std::shared_ptr<registry_state> st;
                    std::shared_ptr<registry_entry> e;
                    ~scope() { st->finish(e); }
                } s{st, e};

                try {
                    co_await std::move(body);
                } catch (const CancelledError&) {
                    // 由 stop_request()/cancel() 触发的正常退出, 不算故障
                } catch (...) {
                    st->exceptions.fetch_add(1);
                }
                co_return;
            }

            std::shared_ptr<registry_state> state_;
        };

    } // namespace detail
} // namespace coro
