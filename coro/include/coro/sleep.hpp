#pragma once

#include "event_loop.hpp"

#include <atomic>
#include <chrono>
#include <memory>

// ============================================================================
// coro::sleep / coro::yield — 时间控制原语
// ============================================================================

namespace coro
{

    // ============================================================================
    // sleep — 挂起当前协程指定时间
    // ============================================================================
    //
    // 用法:
    //   using namespace std::chrono_literals;
    //   co_await sleep(500ms);                       // 挂起 500 毫秒
    //   co_await sleep(std::chrono::seconds(2));     // 挂起 2 秒
    //
    // Python 映射:
    //   await asyncio.sleep(1)  →  co_await coro::sleep(1s)
    //
    // 实现原理:
    //   1. 计算截止时间 = now + duration
    //   2. await_suspend 中调用 EventLoop::schedule_timer(h, deadline)
    //      将当前协程注册到事件循环的定时器堆
    //   3. 协程挂起, 控制权返回事件循环
    //   4. 事件循环在 deadline 到达时, 自动将协程从定时器堆移入就绪队列
    //   5. 协程恢复执行, await_resume() 空操作
    //
    // 注意:
    //   - 时间是单调的 (steady_clock), 不受系统时间调整影响
    //   - sleep 期间协程不占用 CPU, 事件循环可以去执行其他就绪协程
    //   - 精度取决于操作系统的调度精度 (通常 ~1-15ms)
    // ============================================================================

    struct sleep_awaiter
    {
        std::chrono::steady_clock::time_point deadline;
        // 共享取消令牌: await_resume 时置 true,
        // 通知事件循环丢弃堆中的僵尸条目 (防止 resume 已销毁的协程帧)
        std::shared_ptr<std::atomic<bool>> token;

        /// 从截止时间构造 (sleep 工厂函数使用)
        /// 注: 声明了析构函数后不再是聚合体, 需要显式构造器
        explicit sleep_awaiter(std::chrono::steady_clock::time_point d)
            : deadline(d)
        {
        }

        /// 析构时置位令牌: 若协程帧在 sleep 期间被销毁
        /// (取消/wait_for 提前返回/父协程销毁等), 定时器堆条目到期时
        /// 将直接跳过, 而不是 resume 已销毁的帧 (UB)。
        /// 正常到期路径 await_resume 也会置位, 幂等。
        ~sleep_awaiter()
        {
            if (token)
                *token = true;
        }

        // 显式默认移动: 析构函数的存在会抑制隐式移动构造
        sleep_awaiter(sleep_awaiter &&) = default;
        sleep_awaiter &operator=(sleep_awaiter &&) = default;

        /// 总是返回 false: 每次 sleep 都要真正挂起
        bool await_ready() const noexcept { return false; }

        /// 将当前协程注册到定时器堆, 在 deadline 到达前不会被恢复
        void await_suspend(std::coroutine_handle<> h)
        {
            token = std::make_shared<std::atomic<bool>>(false);
            EventLoop::get().schedule_timer(h, deadline, token);
        }

        /// 标记本定时器已消费 (正常到期 或 被 cancel 强制唤醒都会经过这里)
        void await_resume() const noexcept
        {
            if (token)
                *token = true;
        }
    };

    /// 工厂函数: 创建 sleep_awaiter
    template <typename Rep, typename Period>
    sleep_awaiter sleep(std::chrono::duration<Rep, Period> duration)
    {
        return sleep_awaiter{std::chrono::steady_clock::now() + duration};
    }

    // ============================================================================
    // yield — 主动让出 CPU, 将当前协程放回就绪队列末尾
    // ============================================================================
    //
    // 用法:
    //   co_await yield();
    //
    // Python 映射:
    //   await asyncio.sleep(0)  →  co_await coro::yield()
    //
    // 作用:
    //   - 让其他就绪协程有机会执行 (协作式多任务的公平调度)
    //   - 在计算密集型协程中插入 yield 点, 避免长时间独占事件循环
    //
    // 与 sleep(0) 的区别:
    //   sleep(0) 也会立即恢复, 但 yield 直接放入就绪队列更高效
    //   (不经过定时器堆, 减少了优先级队列操作)
    // ============================================================================

    struct yield_awaiter
    {
        bool await_ready() const noexcept { return false; }

        /// 将协程直接放回就绪队列 (尾部)
        void await_suspend(std::coroutine_handle<> h)
        {
            EventLoop::get().schedule(h);
        }

        void await_resume() const noexcept {}
    };

    inline yield_awaiter yield()
    {
        return {};
    }

} // namespace coro
