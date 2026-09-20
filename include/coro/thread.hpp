#pragma once

#include "future.hpp"
#include "task.hpp"

#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <type_traits>
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
        class ThreadPool {
          public:
            explicit ThreadPool(size_t n = std::thread::hardware_concurrency()) {
                if (n == 0)
                    n = 1;
                for (size_t i = 0; i < n; ++i)
                    workers_.emplace_back([this] { worker_loop(); });
            }

            ~ThreadPool() {
                {
                    std::lock_guard lk(mtx_);
                    stopping_ = true;
                }
                cv_.notify_all();
                for (auto& w : workers_)
                    w.join(); // 等待所有已提交任务完成
            }

            ThreadPool(const ThreadPool&) = delete;
            ThreadPool& operator=(const ThreadPool&) = delete;

            /// 提交一个任务 (线程安全)
            void submit(std::function<void()> task) {
                {
                    std::lock_guard lk(mtx_);
                    tasks_.push(std::move(task));
                }
                cv_.notify_one();
            }

            size_t worker_count() const noexcept { return workers_.size(); }

          private:
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
            std::mutex mtx_;
            std::condition_variable cv_;
            bool stopping_ = false;
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

    } // namespace detail

    // ============================================================================
    // to_thread — 在默认线程池执行阻塞函数, 返回可 co_await 的 Task
    // ============================================================================
    //
    // 用法:
    //   int v = co_await coro::to_thread([] { return blocking_read(); });
    //   co_await coro::to_thread([] { heavy_cpu_work(); });   // void 版本
    //
    // 异常: func 抛出的异常在工作线程被捕获, 在 co_await 处重新抛出。
    // ============================================================================
    template <typename F> Task<std::invoke_result_t<F>> to_thread(F func) {
        using R = std::invoke_result_t<F>;

        auto promise = std::make_shared<Promise<R>>();

        // 工作线程任务 (普通 lambda, 非协程): 执行 func → 完成 Promise
        detail::default_thread_pool().submit([promise, f = std::move(func)]() mutable {
            try {
                if constexpr (std::is_void_v<R>) {
                    f();
                    promise->set_value();
                } else {
                    promise->set_value(f());
                }
            } catch (...) {
                promise->set_exception(std::current_exception());
            }
        });

        return detail::to_thread_impl<R>(promise);
    }

} // namespace coro
