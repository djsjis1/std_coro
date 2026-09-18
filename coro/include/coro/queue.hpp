#pragma once

#include "sync.hpp"

#include <deque>
#include <optional>

// ============================================================================
// coro::Queue<T> — 协程间通信队列 (类似 asyncio.Queue)
// ============================================================================
//
// 生产者-消费者模式的基础设施。
//
// Python 映射:
//   q = asyncio.Queue(maxsize=10)
//   await q.put(item)     →  co_await q.put(item)
//   item = await q.get()  →  auto item = co_await q.get()
//
// 特点:
//   - put: 队列满时挂起生产者, 直到有空间
//   - get: 队列空时挂起消费者, 直到有数据
//   - 可选 maxsize (默认无限)
// ============================================================================

namespace coro {

    template <typename T> class Queue {
      public:
        /// 构造函数: maxsize 为 0 表示无限队列
        explicit Queue(size_t maxsize = 0) : maxsize_(maxsize) {}

        // 队列不可拷贝 (内部持有句柄)
        Queue(const Queue&) = delete;
        Queue& operator=(const Queue&) = delete;

        // ==================================================================
        // get_awaiter — co_await q.get() 的返回值
        // ==================================================================
        class get_awaiter {
          public:
            get_awaiter(Queue* q) : q_(q) {} // 保存指向所属队列的指针

            /// 有数据? 有则直接返回 (快速路径, 不挂起)
            bool await_ready() const noexcept { return !q_->items_.empty(); }

            /// 没数据 → 把协程句柄登记到等待队列, 然后挂起
            void await_suspend(std::coroutine_handle<> h) { q_->get_waiters_.push_back(h); }

            /// 被唤醒后: 取出队首数据, 并顺带叫醒一个生产者
            T await_resume() {
                auto item = std::move(q_->items_.front());
                q_->items_.pop_front();
                // 如果有 put 等待者，唤醒一个
                q_->notify_putters();
                return item;
            }

            /// 等待者协程帧被销毁时, 从 get 等待队列摘除僵尸句柄
            void on_waiter_destroyed(std::coroutine_handle<> h) noexcept { q_->remove_get_waiter(h); }

          private:
            Queue* q_; // 所属队列 (本 awaiter 生命周期内队列必然存活)
        };

        // ==================================================================
        // put_awaiter — co_await q.put(item) 的返回值
        // ==================================================================
        class put_awaiter {
          public:
            // 保存队列指针 + 待放入的数据
            put_awaiter(Queue* q, T item) : q_(q), item_(std::move(item)) {}

            /// 有空间 (无限队列或未满)? 有则直接放 (快速路径, 不挂起)
            bool await_ready() const noexcept { return q_->maxsize_ == 0 || q_->items_.size() < q_->maxsize_; }

            /// 满了 → 把协程句柄登记到等待队列, 然后挂起
            void await_suspend(std::coroutine_handle<> h) { q_->put_waiters_.push_back(h); }

            /// 被唤醒后: 放入数据, 并顺带叫醒一个消费者
            void await_resume() {
                q_->items_.push_back(std::move(item_));
                // 如果有 get 等待者，唤醒一个
                q_->notify_getters();
                ++q_->unfinished_tasks_; // 未完成任务计数 (join 依赖)
            }

            /// 等待者协程帧被销毁时, 从 put 等待队列摘除僵尸句柄
            void on_waiter_destroyed(std::coroutine_handle<> h) noexcept { q_->remove_put_waiter(h); }

          private:
            Queue* q_; // 所属队列
            T item_;   // 待放入的数据 (挂起期间暂存在这里)
        };

        // ---- 公开 API ----

        auto get() { return get_awaiter(this); }                        // 取数据 (空则挂起)
        auto put(T item) { return put_awaiter(this, std::move(item)); } // 放数据 (满则挂起)

        /// 非阻塞取: 空 → nullopt (对标 Python q.get_nowait)
        std::optional<T> get_nowait() {
            if (items_.empty())
                return std::nullopt;
            auto item = std::move(items_.front());
            items_.pop_front();
            notify_putters(); // 腾出空间 → 唤醒一个生产者
            return item;
        }

        /// 非阻塞放: 满 → false (对标 Python q.put_nowait)
        bool put_nowait(T item) {
            if (maxsize_ > 0 && items_.size() >= maxsize_)
                return false;
            items_.push_back(std::move(item));
            notify_getters();    // 有货了 → 唤醒一个消费者
            ++unfinished_tasks_; // 未完成任务计数 (join 依赖)
            return true;
        }

        // ==================================================================
        // task_done / join — 生产者-消费者收尾 (对标 asyncio.Queue)
        // ==================================================================
        //
        //   co_await q.put(item);        // 计数 +1
        //   auto item = co_await q.get();
        //   process(item);
        //   q.task_done();               // 计数 -1
        //   co_await q.join();           // 挂起直到计数归零 (全部处理完)
        // ==================================================================

        /// 消费者处理完一个任务 (unfinished_tasks -1; 归零时唤醒所有 join 等待者)
        void task_done() {
            if (unfinished_tasks_ > 0) {
                --unfinished_tasks_;
                if (unfinished_tasks_ == 0) {
                    while (!join_waiters_.empty()) {
                        auto h = join_waiters_.front();
                        join_waiters_.pop_front();
                        EventLoop::get().schedule(h);
                    }
                }
            }
            // 计数为 0 时调用: 忽略 (Python 抛 ValueError, C++ 简化)
        }

        /// co_await q.join() 的 awaiter: 未完成任务归零前挂起
        struct join_awaiter {
            Queue* q;

            bool await_ready() const noexcept { return q->unfinished_tasks_ == 0; }
            void await_suspend(std::coroutine_handle<> h) { q->join_waiters_.push_back(h); }
            void await_resume() const noexcept {}

            /// 等待者协程帧被销毁时, 从 join 等待队列摘除僵尸句柄
            /// (否则 task_done() 时 schedule 已销毁的帧 → UB)
            void on_waiter_destroyed(std::coroutine_handle<> h) noexcept {
                for (auto it = q->join_waiters_.begin(); it != q->join_waiters_.end();) {
                    if (*it == h)
                        it = q->join_waiters_.erase(it);
                    else
                        ++it;
                }
            }
        };

        auto join() { return join_awaiter{this}; }

        /// 未处理完的任务数
        size_t unfinished_count() const noexcept { return unfinished_tasks_; }

        size_t size() const noexcept { return items_.size(); }                           // 当前元素个数
        bool empty() const noexcept { return items_.empty(); }                           // 是否为空
        bool full() const noexcept { return maxsize_ > 0 && items_.size() >= maxsize_; } // 是否已满

      private:
        /// 叫醒最早等待的生产者 (队列有空位了)
        void notify_putters() {
            if (!put_waiters_.empty()) {
                auto h = put_waiters_.front();
                put_waiters_.pop_front();
                EventLoop::get().schedule(h); // 交给事件循环恢复
            }
        }

        /// 叫醒最早等待的消费者 (队列有数据了)
        void notify_getters() {
            if (!get_waiters_.empty()) {
                auto h = get_waiters_.front();
                get_waiters_.pop_front();
                EventLoop::get().schedule(h); // 交给事件循环恢复
            }
        }

        /// 从 get 等待队列中移除指定句柄 (防悬空句柄被调度)
        void remove_get_waiter(std::coroutine_handle<> h) {
            for (auto it = get_waiters_.begin(); it != get_waiters_.end();) {
                if (*it == h)
                    it = get_waiters_.erase(it); // 找到则删除
                else
                    ++it;
            }
        }

        /// 从 put 等待队列中移除指定句柄 (防悬空句柄被调度)
        void remove_put_waiter(std::coroutine_handle<> h) {
            for (auto it = put_waiters_.begin(); it != put_waiters_.end();) {
                if (*it == h)
                    it = put_waiters_.erase(it); // 找到则删除
                else
                    ++it;
            }
        }

        size_t maxsize_;                                   // 队列容量上限 (0 = 无限)
        std::deque<T> items_;                              // 数据缓冲区
        std::deque<std::coroutine_handle<>> put_waiters_;  // 挂起的生产者队列 (FIFO)
        std::deque<std::coroutine_handle<>> get_waiters_;  // 挂起的消费者队列 (FIFO)
        std::deque<std::coroutine_handle<>> join_waiters_; // join() 的等待者
        size_t unfinished_tasks_ = 0;                      // 已 put 未 task_done 的任务数
    };

} // namespace coro
