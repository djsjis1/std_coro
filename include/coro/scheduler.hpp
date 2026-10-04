#pragma once

#include "task.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <thread>
#include <vector>

// ============================================================================
// coro::Scheduler — 自动多核分发 (方案 B: 共享池 + 协程亲和)
// ============================================================================
//
// 对标目标: 像 goroutine 一样"创建任务, 系统自动丢到某个核上跑"。
//
// 模型: N 个 worker 线程, 每个线程一个独立 EventLoop (常驻模式);
//       spawn_any() 把惰性 Task 分发给"活跃协程最少"的 worker 并启动。
//       协程一旦绑定 worker 就不迁移 → 每 worker 内仍是单线程语义
//       (共享状态无需加锁, 库的同步原语零改动)。
//
// 用法:
//   coro::Scheduler sched;                    // 默认 = CPU 核数个 worker
//   for (auto& req : requests)
//       sched.spawn_any([req] { return handle(req); }); // 在 worker 创建任务
//   sched.wait_all();                         // 阻塞直到全部完成
//   // sched 析构时自动停止并 join 所有 worker
//
// 与手动 loop-per-thread 的关系: 完全兼容, 可混合使用 (同一线程内
// 仍可 EventLoop::get().run() 跑自己的任务)。
//
// 注意:
//   - spawn_any 不返回结果 (协程效果通过参数/共享状态传递)
//   - 跨线程共享数据需要用户自行加锁 (与 Go 相同)
//   - 网络 socket 与创建它的 loop 绑定 (跨 loop 使用会被拉回原 loop)
// ============================================================================

namespace coro {

    class Scheduler {
      public:
        /// 创建 N 个 worker 线程 (默认 = 硬件并发数)
        explicit Scheduler(size_t workers = std::thread::hardware_concurrency()) {
            if (workers == 0)
                workers = 1;
            workers_.resize(workers);
            // atomic 数组: make_unique 值初始化 (全部为 0)
            assigned_ = std::make_unique<std::atomic<size_t>[]>(workers);
            try {
                for (auto& w : workers_) {
                    w.thread = std::thread([&w] {
                        EventLoop* loop = nullptr;
                        try {
                            loop = &EventLoop::get();
                            // 由 run 内的第一条回调发布就绪, 保证立即 stop 不会被
                            // 尚未开始的 run_until_stopped 覆盖。
                            loop->dispatch([&w, loop] {
                                w.loop.store(loop, std::memory_order_release);
                                w.started.store(true, std::memory_order_release);
                            });
                        } catch (...) {
                            w.startup_error = std::current_exception();
                            w.started.store(true, std::memory_order_release);
                            return;
                        }
                        loop->run_until_stopped();
                    });
                }
                for (auto& w : workers_) {
                    while (!w.started.load(std::memory_order_acquire))
                        std::this_thread::yield();
                    if (w.startup_error)
                        std::rethrow_exception(w.startup_error);
                }
            } catch (...) {
                // 部分线程创建失败时构造函数不会进入 ~Scheduler, 必须在这里 join。
                for (auto& w : workers_) {
                    if (!w.thread.joinable())
                        continue;
                    while (!w.started.load(std::memory_order_acquire))
                        std::this_thread::yield();
                    if (auto* loop = w.loop.load(std::memory_order_acquire))
                        loop->stop();
                    w.thread.join();
                }
                throw;
            }
        }

        /// 停止并 join 所有 worker (先等待已分发任务完成)
        ~Scheduler() {
            // 1. 等待所有已投递任务完成 (带自适应退避, 同 wait_all)
            wait_all();
            // 2. 逐个 stop+join: 确保每个 worker 的 EventLoop 完全析构后
            //    再处理下一个, 防止主线程 stop() 访问正在销毁的 EventLoop
            for (auto& w : workers_) {
                auto* lp = w.loop.load(std::memory_order_acquire);
                if (lp)
                    lp->stop();
                if (w.thread.joinable())
                    w.thread.join();
            }
        }

        Scheduler(const Scheduler&) = delete;
        Scheduler& operator=(const Scheduler&) = delete;

        /// 分发一个任务工厂到"活跃协程最少"的 worker:
        /// factory 在 worker 线程被调用 (协程帧在 worker 线程创建/销毁,
        /// 避免跨线程堆操作), 返回的 Task 立即启动并自持有运行到完成。
        ///
        /// 工厂返回 Task<T>; 闭包由包装协程持有到任务结束, 也支持带捕获的协程 lambda。
        ///   sched.spawn_any([] { return handle_request(req); });   // 推荐
        ///   sched.spawn_any([arg1, arg2] { return make_task(arg1, arg2); });
        ///
        /// 注意: 不返回任务句柄 (结果通过参数/共享状态传递)。
        template <typename F> void spawn_any(F factory) {
            // dispatch 的 std::function 需要可复制包装; 工厂本身允许仅可移动。
            auto holder = std::make_shared<F>(std::move(factory));
            ++pending_dispatches_;
            // 单次扫描直接拿索引 (旧实现 pick_least_loaded 返回指针后
            // 还要 index_of 再扫一遍, O(2N) → O(N))
            size_t best = pick_least_loaded_index();
            ++assigned_[best];
            try {
                workers_[best].loop.load(std::memory_order_acquire)->dispatch([this, holder]() mutable {
                    try {
                        auto task = run_factory(std::move(*holder));
                        task.start();
                        task.detach();
                    } catch (...) {
                        // 包装任务创建失败也要可观测; 工厂运行时异常走 Task 的默认报告。
                        try {
                            detail::detached_exception_handler()(std::current_exception());
                        } catch (...) {
                            std::fprintf(stderr, "[coro] scheduler error handler threw\n");
                        }
                    }
                    --pending_dispatches_;
                });
            } catch (...) {
                --assigned_[best];
                --pending_dispatches_;
                throw;
            }
        }

        /// 阻塞等待所有已分发任务完成。不能从本 Scheduler 的 worker 调用。
        /// Scheduler 的析构也必须由 worker 之外的所有者执行。
        /// 注: 协程在 worker 线程完成时没有跨线程通知机制 (active_coroutines_
        /// 是 atomic 可安全读, 但每次完成都 notify 会给热路径加系统调用),
        /// 因此仍是轮询 —— 但用自适应退避: 首次 50µs, 每轮翻倍, 上限 1ms。
        /// 对快任务场景比固定 1ms 轮询快约一个数量级, 慢任务不增加开销。
        void wait_all() {
            for (const auto& worker : workers_) {
                if (worker.thread.get_id() == std::this_thread::get_id())
                    throw std::logic_error("Scheduler::wait_all cannot block a worker of the same scheduler");
            }
            using namespace std::chrono_literals;
            auto interval = 50us;
            while (true) {
                if (pending_dispatches_.load() > 0) {
                    std::this_thread::sleep_for(interval);
                    interval = std::min<std::chrono::microseconds>(interval * 2, 1ms);
                    continue;
                }
                bool all_idle = true;
                for (auto& w : workers_) {
                    auto* lp = w.loop.load(std::memory_order_acquire);
                    if (lp && lp->active_task_count() > 0) {
                        all_idle = false;
                        break;
                    }
                }
                if (all_idle)
                    return;
                std::this_thread::sleep_for(interval);
                interval = std::min<std::chrono::microseconds>(interval * 2, 1ms);
            }
        }

        size_t worker_count() const noexcept { return workers_.size(); }

        /// 第 i 个 worker 的 loop (高级用法: 手动调度/查询状态)
        EventLoop* loop_at(size_t i) const {
            return i < workers_.size() ? workers_[i].loop.load(std::memory_order_acquire) : nullptr;
        }

      private:
        template <typename F> static Task<> run_factory(F factory) { co_await factory(); }

        /// 选择「活跃协程最少」的 worker, 返回其索引。
        /// 两级选择:
        ///   主: 活跃协程数 (当前真实负载)
        ///   辅: 累计分发数 (活跃数相同时打破聚集 —— 短任务瞬间完成,
        ///        active 恒为 0 时退化为 round-robin, 保证均衡)
        size_t pick_least_loaded_index() const {
            size_t best_i = 0;
            size_t best_active = SIZE_MAX, best_assigned = SIZE_MAX;
            for (size_t i = 0; i < workers_.size(); ++i) {
                EventLoop* loop = workers_[i].loop.load(std::memory_order_relaxed); // 辅助决策, relaxed 足够
                if (!loop)
                    continue;
                size_t a = loop->active_task_count();
                size_t g = assigned_[i].load(std::memory_order_relaxed);
                if (a < best_active || (a == best_active && g < best_assigned)) {
                    best_i = i;
                    best_active = a;
                    best_assigned = g;
                }
            }
            return best_i;
        }

        struct Worker {
            std::thread thread;
            std::atomic<EventLoop*> loop{nullptr}; // atomic: worker 线程写, 主线程读 (消除数据竞争)
            std::atomic<bool> started{false};
            std::exception_ptr startup_error;

            Worker() = default;
            Worker(Worker&& other) noexcept
                : thread(std::move(other.thread)), loop(other.loop.load(std::memory_order_relaxed)),
                  started(other.started.load(std::memory_order_relaxed)),
                  startup_error(std::move(other.startup_error)) {}
            Worker& operator=(Worker&& other) noexcept {
                thread = std::move(other.thread);
                loop.store(other.loop.load(std::memory_order_relaxed), std::memory_order_relaxed);
                started.store(other.started.load(std::memory_order_relaxed), std::memory_order_relaxed);
                startup_error = std::move(other.startup_error);
                return *this;
            }
            Worker(const Worker&) = delete;
            Worker& operator=(const Worker&) = delete;
        };
        std::vector<Worker> workers_;
        std::unique_ptr<std::atomic<size_t>[]> assigned_; // 每 worker 累计分发数 (均衡辅助)
        std::atomic<size_t> pending_dispatches_{0};       // 已投递但 worker 尚未执行的分发数
    };

} // namespace coro
