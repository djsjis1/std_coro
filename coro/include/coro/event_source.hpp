#pragma once

#include <chrono>
#include <condition_variable>
#include <coroutine>
#include <mutex>
#include <memory>

// ============================================================================
// coro::EventSource — 等待原语抽象层 (模仿 Python selectors 模块)
// ============================================================================
//
// EventLoop 通过 EventSource 阻塞等待三类事件:
//   1. 定时器到点      → wait_for(timeout) 超时返回
//   2. 跨线程唤醒      → wake() 立即返回
//   3. I/O 完成通知    → 完成包到达后恢复协程 (平台实现)
//
// 平台实现 (每平台原生, 零配置自动选择):
//   Windows →  IocpEventSource  (IOCP, iocp_event_source.hpp)
//   Linux   →  UringEventSource (io_uring, uring_event_source.hpp)
//   其他    →  CVEventSource    (condition_variable, 纯标准库)
//
// 扩展新平台: 继承 EventSource 实现 wait_for/wake/has_pending,
// 通过 EventLoop::set_event_source() 安装。
// ============================================================================

namespace coro {

    class EventSource {
      public:
        virtual ~EventSource() = default;

        // ---- 完成回调 (由 EventLoop 安装事件源时注入) ----
        //
        // 事件源完成 I/O 后通过它把协程交还给「拥有本事件源的 EventLoop」。
        // 旧实现是 detail::scheduler() 全局函数指针, 每个线程构造自己的
        // EventLoop 时都会覆写它 —— 多线程同时构造 loop 是数据竞争。
        // 现改为事件源成员: 事件源本身就是每 loop 一个, 天然无竞争。
        using completion_fn = void (*)(void*, std::coroutine_handle<>);

        void set_completion_handler(void* ctx, completion_fn fn) noexcept {
            complete_ctx_ = ctx;
            complete_fn_ = fn;
        }

      protected:
        /// 完成一个 I/O 操作: 把协程交还给事件循环 (未安装回调时忽略)
        void on_complete(std::coroutine_handle<> h) {
            if (complete_fn_)
                complete_fn_(complete_ctx_, h);
        }

      private:
        void* complete_ctx_ = nullptr;
        completion_fn complete_fn_ = nullptr;

      public:
        // ---- 核心等待 ----

        /// 阻塞等待, 最多 timeout 毫秒。
        /// 返回: 0 = 超时 (可以处理定时器), >0 = 有就绪事件 (I/O 或唤醒)
        /// 注意: 调用方应把 wait_for 视为"睡一会", 醒来后重新检查所有状态。
        virtual int wait_for(std::chrono::milliseconds timeout) = 0;

        /// 唤醒等待者 (线程安全, 可从任何线程调用)
        /// 等价于 Python 的 self-pipe/eventfd 写一字节。
        virtual void wake() = 0;

        /// 是否还有挂起的异步操作 (I/O 未完成)
        /// 事件循环用它在"就绪队列空 && 定时器堆空"时判断是否应退出:
        ///   - CVEventSource 始终返回 false (没有 I/O)
        ///   - IocpEventSource 返回 true (还有未完成的 accept/read/write)
        virtual bool has_pending() const { return false; }
    };

    // ============================================================================
    // CVEventSource — 跨平台默认实现 (C++ 标准库, 零依赖)
    // ============================================================================
    //
    // 原理 (学习 Python 的 self-pipe 唤醒):
    //   - wait_for:  cv.wait_for(lock, timeout, woken_)  → 定时器到点 或 被 wake
    //   - wake:      置 woken_ + notify_one()
    //
    // 跨平台性: std::condition_variable 在 Windows 上是 SleepConditionVariableCS,
    //           Linux 上是 futex, macOS 上是 pthread_cond, 全部原生支持。
    // ============================================================================

    class CVEventSource : public EventSource {
      public:
        int wait_for(std::chrono::milliseconds timeout) override {
            std::unique_lock lock(mutex_);
            // 谓词过滤虚假唤醒: 只有 woken_ 为 true 才提前返回
            cv_.wait_for(lock, timeout, [&] { return woken_; });
            woken_ = false; // 消费唤醒标记
            return 0;
        }

        void wake() override {
            {
                std::lock_guard lock(mutex_);
                woken_ = true;
            }
            cv_.notify_one(); // 没有等待者时是 no-op
        }

      private:
        std::mutex mutex_;
        std::condition_variable cv_;
        bool woken_ = false;
    };

} // namespace coro
