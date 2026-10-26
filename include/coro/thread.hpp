#pragma once

#include "exceptions.hpp"
#include "future.hpp"
#include "task.hpp"

#include <condition_variable>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

// ============================================================================
// coro::to_thread — 把阻塞函数丢到线程池, co_await 拿结果
// ============================================================================
//
// 对标 Python 的 asyncio.to_thread / loop.run_in_executor:
//
//   # Python:                                # C++:
//   result = await asyncio.to_thread(        int result = co_await coro::to_thread(
//       blocking_io_read, path)                  [] { return blocking_io_read(path); });
//
// 原理 (与 Future 章节的桥接模式一致):
//   1. to_thread 创建 Promise, 把"执行 func + 完成 Promise"包装成任务提交线程池
//   2. 协程 co_await future 挂起 (不占事件循环线程)
//   3. 工作线程执行完 → Promise.set_value (跨线程安全) → 事件循环被唤醒
//   4. 协程恢复, 拿到结果 (异常同样跨线程传播)
//
// 约束:
//   - func 在工作线程执行, 不要触碰事件循环/协程对象
//   - func 的返回类型决定 Task<T>; void 返回值用 Task<>
//   - 默认线程池在进程退出时等待所有任务完成 (析构 join)
// ============================================================================

namespace coro {

    namespace detail {

        // ==================================================================
        // ThreadPool — 固定大小的简单工作线程池
        // ==================================================================
        //
        // 仅用于 to_thread 的阻塞任务卸载。任务按提交顺序 FIFO 执行。
        // 队列容量可限 (0 = 无界); 满了由 submit/try_submit 明确拒绝,
        // 不会像旧实现那样无上限地吃内存。
        class ThreadPool {
          public:
            explicit ThreadPool(size_t n = std::thread::hardware_concurrency(), size_t max_pending = 0)
                : max_pending_(max_pending) {
                if (n == 0)
                    n = 1;
                try {
                    for (size_t i = 0; i < n; ++i)
                        workers_.emplace_back([this] { worker_loop(); });
                } catch (...) {
                    // A failed constructor does not run ~ThreadPool.
                    stop_and_join();
                    throw;
                }
            }

            ~ThreadPool() { stop_and_join(); }

            ThreadPool(const ThreadPool&) = delete;
            ThreadPool& operator=(const ThreadPool&) = delete;

            /// 提交一个任务 (线程安全)。已停止或队列满 → 抛 OverloadError。
            void submit(std::function<void()> task) {
                if (!try_submit(std::move(task)))
                    throw OverloadError("ThreadPool::submit: stopped or queue is full");
            }

            /// 尝试提交: 被拒绝时返回 false, 不抛异常 (供背压路径使用)
            bool try_submit(std::function<void()> task) {
                {
                    std::lock_guard lk(mtx_);
                    if (stopping_)
                        return false;
                    if (max_pending_ != 0 && tasks_.size() >= max_pending_)
                        return false;
                    tasks_.push(std::move(task));
                }
                cv_.notify_one();
                return true;
            }

            /// 停止接收新任务; 已排队的任务仍会执行完 (析构时同一路径)
            void shutdown() { stop_and_join(); }

            size_t worker_count() const noexcept { return workers_.size(); }

            /// 当前排队中的任务数 (观测用)
            size_t pending() const noexcept {
                std::lock_guard lk(mtx_);
                return tasks_.size();
            }

            bool stopped() const noexcept {
                std::lock_guard lk(mtx_);
                return stopping_;
            }

          private:
            void stop_and_join() {
                {
                    std::lock_guard lk(mtx_);
                    stopping_ = true;
                }
                cv_.notify_all();
                for (auto& worker : workers_)
                    if (worker.joinable())
                        worker.join();
            }

            void worker_loop() {
                while (true) {
                    std::function<void()> task;
                    {
                        std::unique_lock lk(mtx_);
                        cv_.wait(lk, [this] { return stopping_ || !tasks_.empty(); });
                        if (stopping_ && tasks_.empty())
                            return;
                        task = std::move(tasks_.front());
                        tasks_.pop();
                    }
                    task();
                }
            }

            std::vector<std::thread> workers_;
            std::queue<std::function<void()>> tasks_;
            mutable std::mutex mtx_;
            std::condition_variable cv_;
            bool stopping_ = false;
            size_t max_pending_ = 0; // 0 = 无界
        };

        /// 全局默认线程池 (进程退出时析构, 等待任务完成)
        inline ThreadPool& default_thread_pool() {
            static ThreadPool pool;
            return pool;
        }

        // ==================================================================
        // to_thread_impl — 命名协程函数 (参数进帧, 不依赖调用方闭包生命周期):
        // 挂起等待 Future, 工作线程完成后恢复并转发结果
        // ==================================================================
        template <typename R> Task<R> to_thread_impl(std::shared_ptr<Promise<R>> promise) {
            co_return co_await promise->get_future();
        }

        /// 把“执行 callable → 完成 promise”包装成线程池任务
        template <typename F, typename R>
        void submit_to_pool(const std::shared_ptr<Promise<R>>& promise, const std::shared_ptr<F>& callable) {
            auto task = [promise, callable] {
                try {
                    if constexpr (std::is_void_v<R>) {
                        std::invoke(*callable);
                        promise->set_value();
                    } else {
                        promise->set_value(std::invoke(*callable));
                    }
                } catch (...) {
                    promise->set_exception(std::current_exception());
                }
            };
            // 拒绝 (已停止/队列满) 必须让等待者看到明确失败, 而不是挂死。
            if (!default_thread_pool().try_submit(std::move(task)))
                promise->set_exception(std::make_exception_ptr(
                    OverloadError("to_thread: executor stopped or its queue is full")));
        }

    } // namespace detail

    // ============================================================================
    // to_thread — 在默认线程池执行阻塞函数, 返回可 co_await 的 Task
    // ============================================================================
    //
    // 用法:
    //   int v = co_await coro::to_thread([] { return blocking_read(); });
    //   co_await coro::to_thread([] { heavy_cpu_work(); });   // void 版本
    //
    // 语义 (显式选择, 不隐藏差异):
    //   - to_thread: **调用时立即提交** —— 即使返回的 Task 没被 co_await,
    //     工作也可能已经开始。需要“开始执行时才提交”请用 to_thread_lazy。
    //   - 异常: func 抛出的异常在工作线程被捕获, 在 co_await 处重新抛出。
    //   - 执行器已停止/队列满: co_await 处得到 OverloadError (不会静默丢任务)。
    // ============================================================================
    template <typename F> Task<std::invoke_result_t<F&>> to_thread(F func) {
        using R = std::invoke_result_t<F&>;

        auto promise = std::make_shared<Promise<R>>();
        auto callable = std::make_shared<F>(std::move(func));
        auto result = detail::to_thread_impl<R>(promise);
        detail::submit_to_pool<F, R>(promise, callable);
        return result;
    }

    /// 惰性卸载 (对标 asyncio.to_thread): 协程真正启动时才提交到线程池。
    /// 只创建 Task 而不 await/start → 工作函数一次都不会执行。
    template <typename F> Task<std::invoke_result_t<F&>> to_thread_lazy(F func) {
        using R = std::invoke_result_t<F&>;
        auto promise = std::make_shared<Promise<R>>();
        auto callable = std::make_shared<F>(std::move(func));
        detail::submit_to_pool<F, R>(promise, callable);
        co_return co_await promise->get_future();
    }

} // namespace coro
