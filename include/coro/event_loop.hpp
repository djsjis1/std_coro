#pragma once

#include "event_source.hpp"

#ifdef _WIN32
#include "iocp_event_source.hpp"
#elif defined(__linux__)
#include "uring_event_source.hpp"
#endif

#include <chrono>
#include <coroutine>
#include <cstdint>
#include <functional>
#include <iostream>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <thread>
#include <atomic>
#include <memory>
#include <unordered_set>
#include <unordered_map>
#include <vector>

// ============================================================================
// coro::EventLoop — 每线程一个事件循环（类似 Python asyncio 的事件循环）
// ============================================================================
//
// 核心职责:
//   1. 维护一个「就绪队列」(FIFO) — 存放可以立即恢复执行的协程
//   2. 维护一个「定时器堆」(min-heap) — 存放等待超时的协程
//   3. run() 方法驱动整个循环，直到没有待处理的工作为止
//
// Python 映射:
//   asyncio.get_event_loop()       →  EventLoop::get() 单例
//   asyncio.run(main())            →  coro::run(main_task())
//   loop.call_soon_threadsafe(cb)  →  schedule(h)  (跨线程安全, 自动唤醒)
//   loop.stop()                    →  stop()
//
// ============================================================================
//
// 等待原语 (模仿 Python selectors):
//
//   EventLoop 不直接睡眠, 而是委托给 EventSource 抽象层:
//     - 默认 CVEventSource (condition_variable): 跨平台, 支持跨线程唤醒
//     - 将来可替换为 EpollEventSource / KqueueEventSource / WinEventSource,
//       获得 I/O 事件支持, EventLoop 本身不用改
//
//   跨线程唤醒流程 (等价 Python 的 self-pipe 唤醒):
//     其他线程:  schedule(h) → 加锁入队 → event_source_->wake()
//     事件循环:  event_source_->wait_for(剩余时间) → 立即醒来处理
//
// ============================================================================
//
// 调度流程:
//   每次迭代:
//     a. 将到期的定时器从堆中移入就绪队列
//     b. 如果就绪队列为空, 通过 EventSource 等待 (定时器到点 / 被唤醒 / I/O)
//     c. 从就绪队列取出一个协程并恢复执行
//   当就绪队列和定时器堆都为空时, 循环退出
//
// 线程安全:
//   - schedule() / wake() / stop(): 线程安全, 可从任何线程调用
//   - schedule_timer(): 仅供事件循环线程调用 (协程 await_suspend 内部)
//   - timer_heap_: 仅事件循环线程访问, 无需加锁
//   - ready_queue_: 由 queue_mutex_ 保护 (跨线程调度)
// ============================================================================

namespace coro {

    class EventLoop;

    namespace detail {
        // 当前正在执行的协程句柄 (事件循环 resume 前后设置/清除)。
        // thread_local: 每个线程独立; 对标 asyncio.current_task()。
        inline thread_local std::coroutine_handle<> t_current_task;

        // 当前线程绑定的 EventLoop (每线程一个, 对标 asyncio 的
        // "一个线程一个事件循环"模型)。
        inline thread_local EventLoop* t_current_loop = nullptr;
    } // namespace detail

    class EventLoop {
      public:
        // ---- 访问: 当前线程的事件循环 (不存在则惰性创建) ----
        //
        // 模型对标 Python asyncio: 每个线程有自己独立的事件循环,
        // 各跑各的协程 → 天然多核并行 (一个线程一个核)。
        //
        // 库内部所有原语 (sleep/sync/queue/future/gather/task_group)
        // 都通过 get() 访问"当前线程的 loop", 因此:
        //   - 同一线程内的协程共享一个 loop (单线程语义不变)
        //   - 不同线程各自 run() 时互不干扰 (并行)
        //   - 跨线程唤醒 (Future.set_value) 由 owner_loop 精确路由
        static EventLoop& get() {
            if (!detail::t_current_loop) {
                // 函数内 thread_local static: 首次访问时构造 (本线程的实例)
                static thread_local EventLoop instance;
                detail::t_current_loop = &instance;
            }
            return *detail::t_current_loop;
        }

        /// 显式绑定当前线程到指定 loop (run() 内部自动调用; 进阶用法)
        static void bind(EventLoop* loop) noexcept { detail::t_current_loop = loop; }

        // ---- 核心 API ----

        /// 驱动事件循环, 直到没有就绪协程或定时器
        /// 禁止嵌套调用（如果已经在运行中则直接返回）
        void run();

        /// 常驻模式: 驱动事件循环直到 stop() 被调用 (无工作时也保持等待)。
        /// 供 Scheduler 的 worker 线程使用: worker 常驻, 随时接受跨线程投递的任务。
        /// 禁止嵌套调用。
        void run_until_stopped();

        /// 查询事件循环是否正在运行
        bool is_running() const { return running_; }

        /// 请求停止（当前迭代完成后退出, 线程安全）
        /// 事件循环正在等待时调用, 会立即唤醒它退出
        void stop() {
            running_ = false;
            wake();
        }

        /// 唤醒事件循环线程 (线程安全, 可从任何线程调用)
        /// 等价于 Python 的 self-pipe/eventfd 写一字节
        void wake() { event_source_->wake(); }

        /// 替换等待原语 (做网络库时用平台实现替换默认 CVEventSource)
        void set_event_source(std::shared_ptr<EventSource> src) {
            if (!src)
                throw std::invalid_argument("EventLoop::set_event_source: null source");
            if (running_.load(std::memory_order_acquire))
                throw std::logic_error("EventLoop::set_event_source: loop is running");
            if (event_source_ && event_source_->has_pending())
                throw std::logic_error("EventLoop::set_event_source: pending I/O");
            event_source_ = std::move(src);
            install_event_source();
        }

        /// 获取当前等待原语 (网络层需要它关联 socket 到 IOCP)
        EventSource* event_source_ptr() const { return event_source_.get(); }

#ifdef _WIN32
        /// 当前 loop 的 IOCP 事件源 (构造时缓存, 类型化)。
        /// IO 层 (net/fs/pipe/process) 每次操作都用它 —— 替代旧的
        /// dynamic_cast(event_source_ptr()) 每操作一次 RTTI 行走。
        /// 用户安装了非 IOCP 事件源时为 nullptr (调用方需判空)。
        net::IocpEventSource* iocp() const noexcept { return iocp_source_; }
#endif
#ifdef CORO_URING_ENABLED
        /// 当前 loop 的 io_uring 事件源 (构造时缓存, 类型化), 语义同 iocp()
        net::UringEventSource* uring() const noexcept { return uring_source_; }
#endif

        /// 跨线程投递一个普通函数到本事件循环执行 (Scheduler 分发用)。
        /// 在事件循环线程以非协程方式执行 —— 供"在正确线程创建协程帧"的场景:
        /// 无栈协程的帧在调用工厂函数的线程分配, 完成时也在该线程销毁,
        /// 跨线程创建/销毁帧会触发 Debug CRT 堆断言。
        void dispatch(std::function<void()> fn) {
            {
                std::lock_guard lock(queue_mutex_);
                fn_queue_.push(std::move(fn));
                has_pending_fn_.store(true, std::memory_order_release);
            }
            // 入睡协议: 仅在跨线程且循环已声明入睡时投递唤醒包
            // (exchange 抢占 awake_; 循环醒着时零系统调用)
            cross_thread_wake_if_asleep();
        }

        // ---- 调度 API（供内部 awaiter 调用） ----

        /// 将协程加入就绪队列 (线程安全, 跨线程可调用)
        /// 这是协程调度的核心入口, sleep/gather/Future 最终都通过它来恢复协程。
        /// 等价于 Python 的 loop.call_soon_threadsafe()。
        void schedule(std::coroutine_handle<> h);

        /// 将协程加入定时器堆, deadline 到达时自动移入就绪队列
        /// sleep 的底层实现就是调用这个方法。
        /// 注意: 仅供事件循环线程调用 (协程 await_suspend 内部),
        ///       不做线程安全保护。
        /// token: 可选的共享取消标志, 协程被 cancel 后由 await_resume 置 true,
        ///        到期的僵尸条目将被 process_timers 跳过。
        void schedule_timer(std::coroutine_handle<> h, std::chrono::steady_clock::time_point deadline,
                            std::shared_ptr<std::atomic<bool>> token = nullptr);

        // ---- 活跃协程计数与任务注册表 (由 Task 的启动/完成路径维护) ----
        //
        // 作用一: 事件循环的退出条件之一。"队列空 + 定时器空 + 无挂起 I/O"
        // 不代表没有工作——协程可能正挂起在等待跨线程事件 (如 Future 的
        // set_value)。活跃计数 > 0 时事件循环必须继续等待 (无限等待被唤醒),
        // 否则等待者协程悬空, 之后被唤醒时事件循环早已退出 → 崩溃。
        // 作用二: all_tasks() 注册表 (对标 asyncio.all_tasks) 供调试/监控,
        // 记录所有已启动且尚未完成的协程帧地址。
        // 性能: 注册表在热路径上每协程生命周期多花 2 次锁 + 2 次 hash
        // 操作, 默认编译关闭; 需要调试/监控时定义 CORO_TASK_REGISTRY。
        void on_coroutine_started(std::coroutine_handle<> h) {
            std::lock_guard lock(queue_mutex_);
            live_frames_.emplace(h.address(), ++next_generation_);
            ++active_coroutines_;
#ifdef CORO_TASK_REGISTRY
            all_tasks_.insert(h.address());
#endif
        }
        void on_coroutine_finished(std::coroutine_handle<> h) {
            std::lock_guard lock(queue_mutex_);
            live_frames_.erase(h.address());
            scheduled_set_.erase(h.address());
            --active_coroutines_;
#ifdef CORO_TASK_REGISTRY
            all_tasks_.erase(h.address());
#endif
        }

        /// Task 析构时调用: 尝试将句柄标记为「已废弃」。
        /// 如果句柄在就绪队列中 (scheduled_set_ 有记录), 注册废弃, 延迟到事件循环安全销毁。
        /// 返回 true 表示已注册废弃 (帧保持存活), false 表示不在队列中 (调用方可安全销毁)。
        bool mark_abandoned(std::coroutine_handle<> h) {
            std::lock_guard lock(queue_mutex_);
            auto it = scheduled_set_.find(h.address());
            if (it == scheduled_set_.end())
                return false; // 不在就绪队列: 调用方可安全销毁帧
            abandoned_handles_.insert(h.address());
            return true; // 已注册废弃: 调用方不要销毁帧
        }

        /// Task 在底层异步 I/O 挂起时析构：帧必须保留到完成包/CQE 被消费。
        /// I/O 取消完成后句柄会进入就绪队列，run() 看到 abandoned 标记后
        /// 只销毁帧而不 resume，避免 OVERLAPPED/uring_op 的 use-after-free。
        /// 与 Task::cancel 一样，当前要求从所属事件循环线程调用。
        void mark_io_abandoned(std::coroutine_handle<> h) {
            std::lock_guard lock(queue_mutex_);
            abandoned_handles_.insert(h.address());
        }

        /// 检查句柄是否已废弃 (仅事件循环线程调用)
        bool is_abandoned(std::coroutine_handle<> h) const { return abandoned_handles_.count(h.address()) > 0; }

        /// 移除废弃标记并销毁帧 (事件循环清理路径)
        void cleanup_abandoned(std::coroutine_handle<> h) {
            abandoned_handles_.erase(h.address());
            h.destroy();
        }

        bool has_active_coroutines() const { return active_coroutines_ > 0; }

        /// 活跃任务数 (已启动且未完成)
        size_t active_task_count() const { return active_coroutines_.load(); }

        /// 活跃任务帧地址快照 (对标 asyncio.all_tasks, 供调试)。
        /// 仅在编译时定义了 CORO_TASK_REGISTRY 时有内容, 否则为空。
        std::vector<void*> all_tasks() const {
#ifdef CORO_TASK_REGISTRY
            std::lock_guard lock(queue_mutex_);
            return {all_tasks_.begin(), all_tasks_.end()};
#else
            return {};
#endif
        }

        /// 当前正在执行 (刚被 resume) 的协程句柄; 非协程上下文为空
        static std::coroutine_handle<> current_task() { return detail::t_current_task; }

      private:
        // 构造时: 按平台选择默认事件源 (像 Python 一样零配置)
        EventLoop() : event_source_(make_default_event_source()) { install_event_source(); }

        static std::shared_ptr<EventSource> make_default_event_source() {
#ifdef _WIN32
            // Windows: 默认 IOCP (等价 Python ProactorEventLoop)
            auto iocp = std::make_shared<net::IocpEventSource>();
            if (iocp->valid())
                return iocp;
#elif defined(__linux__) && defined(CORO_URING_ENABLED)
            // Linux: io_uring 可用时走 Proactor；初始化失败回退到 CV,
            // 让核心协程/定时器仍可运行，IO awaiter 会返回 ENOTSUP。
            auto uring = std::make_shared<net::UringEventSource>();
            if (uring->valid())
                return uring;
#endif
            // 其他平台或原生事件源初始化失败: 使用 condition_variable。
            return std::make_shared<CVEventSource>();
        }

        // 安装/重装事件源: 注册完成回调 + 缓存类型化指针
        void install_event_source() {
            // 完成回调: I/O 完成包 → 协程交还给「拥有本事件源的 loop」。
            // 替代旧的全局 detail::scheduler() 函数指针 —— 那个会被每个
            // 线程构造 loop 时覆写, 多线程同时构造是数据竞争。
            event_source_->set_completion_handler(
                this, [](void* ctx, std::coroutine_handle<> h) { static_cast<EventLoop*>(ctx)->schedule(h); });
            // 类型化缓存 (每 loop 一次 dynamic_cast, 取代 IO 层每操作一次)
#ifdef _WIN32
            iocp_source_ = dynamic_cast<net::IocpEventSource*>(event_source_.get());
#endif
#ifdef CORO_URING_ENABLED
            uring_source_ = dynamic_cast<net::UringEventSource*>(event_source_.get());
#endif
        }

        // ---- 定时器堆条目 ----
        // 使用 std::priority_queue 实现最小堆:
        //   operator< 返回 deadline > other.deadline 意味着
        //   "更早的截止时间" 排在堆顶（优先级最高）
        //
        // 僵尸条目问题:
        //   协程被 cancel 强制唤醒后, 它的 timer 条目仍留在堆里,
        //   到期时若直接 resume 已销毁的协程帧 → UB。
        //   解决: token 是堆上的共享标志, 两个时机置 true:
        //     1. sleep_awaiter::await_resume (正常到期路径)
        //     2. sleep_awaiter 析构 (协程帧销毁路径, 含取消注入的异常展开;
        //        此时 await_resume 根本不会执行, 必须靠析构置位)
        //   process_timers 跳过已标记条目。
        struct ReadyEntry {
            std::coroutine_handle<> handle;
            uint64_t generation;
        };

        struct TimerEntry {
            std::chrono::steady_clock::time_point deadline;
            ReadyEntry entry;
            std::shared_ptr<std::atomic<bool>> token; // 已取消/已消费标记 (堆上共享)

            bool operator<(const TimerEntry& other) const {
                return deadline > other.deadline; // min-heap: 早的优先
            }
        };

        // ---- 轻量协程 FIFO (vector + 头索引) ----
        //
        // 替代 std::queue<coroutine_handle<>> (底层 deque):
        // MSVC deque 对 8 字节元素每 16 字节块只装 2 个 —— 每 2 次 push
        // 一次堆分配; vector 摊销为每上百次 push 一次, 且配合「成员 batch
        // + swap」后, 容量在 batch 与 ready_queue_ 之间往复保留,
        // 稳态零分配。
        struct HandleQueue {
            std::vector<ReadyEntry> items;
            size_t head = 0;

            void push(ReadyEntry entry) { items.push_back(entry); }
            bool empty() const { return head >= items.size(); }
            ReadyEntry front() const { return items[head]; }
            void pop() {
                ++head;
                if (head == items.size()) {
                    items.clear(); // 整队列耗尽: 一次回收 (保留容量)
                    head = 0;
                }
            }
            void swap(HandleQueue& other) noexcept {
                items.swap(other.items);
                std::swap(head, other.head);
            }
        };

        // ---- 成员变量 ----

        HandleQueue ready_queue_;                   // 就绪协程 FIFO
        HandleQueue batch_;                         // 本轮批量消费缓冲 (容量跨迭代复用)
        std::vector<ReadyEntry> timer_expired_buf_; // process_timers 复用缓冲 (容量跨迭代复用)
        mutable std::mutex queue_mutex_;            // 保护 ready_queue_/scheduled_set_/all_tasks_ (跨线程)

        // 已在就绪队列中的句柄集合 (schedule 幂等去重)。
        // 为什么需要: 同一句柄可能被多个来源同时调度, 例如
        //   1) 任务完成 → final_suspend 调度等待它的协程 W
        //   2) 同一时刻 W 被 cancel → suspended_ 仍为 true → cancel 也调度 W
        // 若双入队, W 第一次 resume 后帧销毁, 第二次 pop 到它时 done() 是 UB。
        // schedule 时查重, 出队 (批量 swap) 时移除。
        std::unordered_set<const void*> scheduled_set_;

        // 始终启用的帧存活表，与可选调试注册表分离。出队前检查代次，
        // 不对已释放帧调用 done()；地址复用也不能让旧条目恢复新帧。
        std::unordered_map<const void*, uint64_t> live_frames_;
        uint64_t next_generation_ = 0;

        // 已废弃但帧仍存活的协程句柄集合 (Task 析构时注册, 事件循环清理)。
        // 仅事件循环线程访问 (mark_abandoned 也在事件循环线程调用)。
        std::unordered_set<const void*> abandoned_handles_;

        // 跨线程投递的普通函数队列 (dispatch 用), queue_mutex_ 保护
        std::queue<std::function<void()>> fn_queue_;

        std::priority_queue<TimerEntry> timer_heap_; // 定时器最小堆 (仅事件循环线程)

        // 运行标志 (atomic: stop() 可从其他线程调用, 必须避免数据竞争)
        std::atomic<bool> running_ = false;

        // 活跃协程数: 已启动且尚未完成的协程 (含挂起中等待唤醒的)
        // 跨线程路径也会修改 (on_coroutine_finished 在事件循环线程,
        // on_coroutine_started 可能在任意线程) → atomic
        std::atomic<size_t> active_coroutines_ = 0;

        // 活跃任务帧地址注册表 (all_tasks 快照用), queue_mutex_ 保护。
        // 默认编译关闭 (见 on_coroutine_started), 避免热路径锁开销。
#ifdef CORO_TASK_REGISTRY
        std::unordered_set<void*> all_tasks_;
#endif

        // 事件循环线程标记: 用于区分"同线程调度"和"跨线程调度"。
        // 同线程调度 (run() 内部) 不需要 wake (循环会自然处理就绪队列),
        // 否则每个 schedule 都投递一个唤醒包, 堆积成"假唤醒风暴"。
        std::thread::id loop_thread_id_;

        // 入睡协议标志 (seq_cst): 循环醒着时为 true。
        // 跨线程 schedule/dispatch 用 exchange(true) 抢占 —— 抢到 false
        // (循环刚声明入睡) 才投递唤醒包; 配合 run_impl 里「先声明入睡、
        // 再锁内复查队列」的顺序, 保证不丢唤醒也不滥发唤醒。
        std::atomic<bool> awake_{true};

        // fn_queue_ 非空标志: run 循环用它跳过空队列的加锁
        std::atomic<bool> has_pending_fn_{false};

        // 等待原语抽象 (默认由构造函数按平台选择)
        std::shared_ptr<EventSource> event_source_;

#ifdef _WIN32
        // IOCP 事件源类型化缓存 (install_event_source 维护; 零开销访问)
        net::IocpEventSource* iocp_source_ = nullptr;
#endif
#ifdef CORO_URING_ENABLED
        net::UringEventSource* uring_source_ = nullptr;
#endif

        // 将到期的定时器从堆中移入就绪队列 (仅事件循环线程)。
        // 返回本次使用的时钟点 (run_impl 复用, 省一次 now() 调用)
        std::chrono::steady_clock::time_point process_timers();

        // 跨线程且循环已入睡时才投递唤醒包 (入睡协议, 见 awake_ 定义)
        void cross_thread_wake_if_asleep();

        // 判断是否还有待处理的工作
        bool has_work();

        // run()/run_until_stopped() 的公共实现 (stay: 无工作时是否驻留等待)
        void run_impl(bool stay);
    };

    // ============================================================================
    // 内联实现
    // ============================================================================

    /// 跨线程唤醒辅助: 仅当「调用来自其他线程 且 循环在运行 且 已声明入睡」
    /// 时投递唤醒包。循环醒着时这里只是一次原子读, 零系统调用 ——
    /// 消灭旧实现「每个跨线程 schedule 一个 PostQueuedCompletionStatus,
    /// 循环忙碌时积压成假唤醒风暴」的问题。
    ///
    /// 线程安全: loop_thread_id_ 用 thread_local 缓存, 避免跨线程直接读
    /// (signal reader 线程调 schedule() → cross_thread_wake → 读主线程 TLS
    /// 会被 TSan 报 data race)。thread_local 在首次调用时拷贝一次, 之后
    /// 只访问本线程 TLS, 零竞争。
    inline void EventLoop::cross_thread_wake_if_asleep() {
        // schedule() 经常由另一个线程调用，不能把目标 loop 缓存在
        // 调用线程的 TLS 中；那会把第一次跨线程投递误判成同线程，
        // 之后目标 loop 睡眠时就永远收不到唤醒包。
        const bool same_loop = detail::t_current_loop == this;
        if (!same_loop && running_.load(std::memory_order_acquire) && !awake_.exchange(true))
            event_source_->wake();
    }

    inline void EventLoop::schedule(std::coroutine_handle<> h) {
        if (h) {
            {
                std::lock_guard lock(queue_mutex_);
                // 调度入口只接受已启动的存活帧；自定义协程也须配对调用
                // on_coroutine_started/on_coroutine_finished，且在 owner loop 销毁。
                auto live = live_frames_.find(h.address());
                if (live == live_frames_.end() || !scheduled_set_.insert(h.address()).second)
                    return;
                ready_queue_.push({h, live->second});
            }
            // 同线程调度 (run() 内部) 不唤醒: 循环会自然处理就绪队列。
            // 等价 Python: loop.call_soon() 不唤醒, call_soon_threadsafe() 才写 self-pipe
            // 性能: 本线程 ID 用 thread_local 缓存 (get_id 是系统调用, 高频调度下省掉)
            cross_thread_wake_if_asleep();
        }
    }

    inline void EventLoop::schedule_timer(std::coroutine_handle<> h, std::chrono::steady_clock::time_point deadline,
                                          std::shared_ptr<std::atomic<bool>> token) {
        if (h) {
            std::lock_guard lock(queue_mutex_);
            auto live = live_frames_.find(h.address());
            if (live != live_frames_.end())
                timer_heap_.push({deadline, {h, live->second}, std::move(token)});
        }
    }

    inline std::chrono::steady_clock::time_point EventLoop::process_timers() {
        // 第一步: 惰性清理僵尸条目。
        //   协程帧销毁时 sleep_awaiter 析构会置位 token, 但条目仍留在堆里;
        //   若不及时清理, 事件循环会被这些"已失效的 deadline"拖着空等到期
        //   (has_work 认为还有定时器), 表现为 cancel 后程序迟迟不退出。
        while (!timer_heap_.empty()) {
            auto& top = timer_heap_.top();
            if (top.token && *top.token) {
                timer_heap_.pop(); // 已失效: 无论是否到期都丢弃
                continue;
            }
            break; // 堆顶有效, 后面的 deadline 都更晚, 无需继续检查
        }

        auto now = std::chrono::steady_clock::now();

        // 第二步: 将到期的定时器移入就绪队列。
        // 先收集再「一次锁批量入队」—— 旧实现每个到期定时器一次锁,
        // 10 万个同时到期的场景就是 10 万次加锁。
        // 注意: 仍经过 scheduled_set_ 去重 (与 schedule() 保持一致),
        // 否则跨线程 schedule(h) 和定时器到期可能将同一句柄双入队 → double-resume UB
        // 优化: 用成员 timer_expired_buf_ 代替局部 vector, 跨迭代复用容量 (同 batch_ 策略)
        timer_expired_buf_.clear();
        {
            while (!timer_heap_.empty() && timer_heap_.top().deadline <= now) {
                auto entry = timer_heap_.top();
                timer_heap_.pop();
                if (!entry.token || !entry.token->load())
                    timer_expired_buf_.push_back(entry.entry);
            }
            if (!timer_expired_buf_.empty()) {
                std::lock_guard lock(queue_mutex_);
                for (auto entry : timer_expired_buf_) {
                    auto live = live_frames_.find(entry.handle.address());
                    if (live != live_frames_.end() && live->second == entry.generation &&
                        scheduled_set_.insert(entry.handle.address()).second)
                        ready_queue_.push(entry);
                }
            }
        }
        // 返回本次使用的时钟: run_impl 的超时计算直接复用 (省一次系统调用)
        return now;
    }

    inline bool EventLoop::has_work() {
        // ready_queue_ 需要加锁 (其他线程可能正在 schedule)
        // timer_heap_ 仅事件循环线程访问, 直接读
        // 挂起的 I/O 操作也是"工作": 有它们事件循环就不能退出
        // 活跃协程 > 0 也是"工作": 它们可能挂起在等待跨线程唤醒 (Future 等)
        std::lock_guard lock(queue_mutex_);
        return !ready_queue_.empty() || !fn_queue_.empty() || !timer_heap_.empty() || event_source_->has_pending() ||
               active_coroutines_ > 0;
    }

    inline void EventLoop::run() {
        run_impl(false);
    }

    inline void EventLoop::run_until_stopped() {
        run_impl(true);
    }

    inline void EventLoop::run_impl(bool stay) {
        if (running_)
            return; // 禁止嵌套 run()
        running_ = true;
        awake_.store(true, std::memory_order_seq_cst);
        bind(this);                                   // 当前线程绑定本 loop:
        loop_thread_id_ = std::this_thread::get_id(); // 事件源回调等内部路径路由正确

        while (running_) {
            // 非驻留模式: 无任何工作时退出 (驻留模式忽略此条件, 等 stop())
            if (!stay && !has_work())
                break;

            // 第 0 步: 执行跨线程投递的普通函数 (dispatch)
            // 它们可能创建新协程 → 之后再处理定时器/就绪队列。
            // 原子标志空判跳锁: 空队列时零加锁。
            if (has_pending_fn_.load(std::memory_order_acquire)) {
                // 先复位再排空: 复位之后的新 dispatch 会重新置位 → 下轮处理,
                // 不会漏 (反之「排空后复位」会覆盖并发置位 → 漏任务)
                has_pending_fn_.store(false, std::memory_order_release);
                std::vector<std::function<void()>> fns;
                {
                    std::lock_guard lock(queue_mutex_);
                    while (!fn_queue_.empty()) {
                        fns.push_back(std::move(fn_queue_.front()));
                        fn_queue_.pop();
                    }
                }
                for (auto& f : fns)
                    f();
            }

            // 第一步: 惰性清理僵尸定时器 + 将到期定时器移入就绪队列
            auto now = process_timers();

            // 第二步: 如果就绪队列 (和 fn 队列) 为空, 通过 EventSource 等待
            // 等价于 Python 的 selector.select(timeout):
            //   - 定时器到点 → 超时返回, 处理定时器
            //   - 其他线程 wake() → 立即返回 (不再睡死!)
            //   - I/O 完成 → 立即返回, 恢复对应协程
            //
            // 入睡协议 (防丢失唤醒, seq_cst 全序保证正确):
            //   1. 先声明入睡 (awake_ = false)
            //   2. 锁内复查队列 —— 覆盖「声明之后、复查之前」到达的入队:
            //      若生产者在它自己的 exchange 里读到 true (跳过唤醒),
            //      则其入队在 SC 全序上先于本 store, 复查必然看到;
            //      若读到 false, 它会投递唤醒包, wait_for 必然被打断
            //   3. 都空了才真正 wait_for
            {
                awake_.store(false, std::memory_order_seq_cst);
                bool empty;
                {
                    std::lock_guard lock(queue_mutex_);
                    empty = ready_queue_.empty() && fn_queue_.empty();
                }
                if (!empty) {
                    awake_.store(true, std::memory_order_seq_cst);
                } else if (!timer_heap_.empty()) {
                    auto next = timer_heap_.top().deadline;
                    if (next > now) {
                        // 向上取整: 剩余 <1ms 时等待 1ms 而非 0ms。
                        // IOCP/io_uring 的超时都是毫秒粒度, 截断为 0 会让
                        // wait_for 立即返回 → 循环忙转烧满一个核。
                        auto ms = std::chrono::ceil<std::chrono::milliseconds>(next - now);
                        event_source_->wait_for(ms);
                    }
                    // next <= now: 定时器已到期, 不等待直接进入下一轮
                    awake_.store(true, std::memory_order_seq_cst);
                } else if (stay || event_source_->has_pending() || has_active_coroutines()) {
                    // 有挂起的 I/O 操作 (如 IOCP accept/read) 或活跃协程
                    // (可能挂起在等待跨线程唤醒, 如 Future 的 set_value):
                    // 无限等待, I/O 完成/跨线程唤醒时事件源会唤醒我们
                    // (milliseconds::max() 在事件源内部会被 clamp 为最长等待)
                    // 驻留模式 (stay) 下即使无事也无限等待, 直到 stop()
                    event_source_->wait_for(std::chrono::milliseconds::max());
                    awake_.store(true, std::memory_order_seq_cst);
                } else {
                    // 队列、堆、I/O 都无事可做, 退出
                    awake_.store(true, std::memory_order_seq_cst);
                    break;
                }
            }

            // 第三步: 恢复就绪协程。
            // 优化: 把整个就绪队列一次性 swap 出来 (一次加锁), 再逐个 resume;
            //   否则每个句柄都要 pop_ready 加锁一次。
            //   batch_ 是成员: swap 后容量在 batch_/ready_queue_ 间往复保留,
            //   稳态运行零堆分配。
            //   行为说明: resume 期间新 schedule 的协程留在新队列里,
            //   由下一轮迭代处理 (等价 Python call_soon 的"下轮执行"语义,
            //   且不会引入延迟: 队列非空时下一轮不会进入等待)。
            {
                std::lock_guard lock(queue_mutex_);
                batch_.swap(ready_queue_);
                // 注意: 不在此处从 scheduled_set_ 移除!
                // batch 消费期间 (resume 之前) 句柄仍算"已调度",
                // 防止 batch 内前面的协程 cancel 后面的协程时重复入队。
            }
            while (!batch_.empty()) {
                auto entry = batch_.front();
                auto h = entry.handle;
                batch_.pop();
                {
                    // resume 前移除: 协程 resume 后若再次挂起, 允许重新入队。
                    // 必须逐个进行而非整批提前擦除 —— 这是防 double-schedule
                    // 的正确性机制 (见上方注释), 不能作为纯优化合并。
                    std::lock_guard lock(queue_mutex_);
                    auto live = live_frames_.find(h.address());
                    if (live == live_frames_.end() || live->second != entry.generation)
                        continue;
                    scheduled_set_.erase(h.address());
                }
                if (h && !h.done()) { // 跳过已完成的协程 (防止 double-resume)
                    if (is_abandoned(h)) {
                        // Task 已析构但帧仍存活 (在就绪队列中未被销毁):
                        // 安全销毁帧, 补记活跃计数递减 (对应 Task 析构时跳过的 on_coroutine_finished)
                        on_coroutine_finished(h);
                        cleanup_abandoned(h);
                        continue;
                    }
                    detail::t_current_task = h; // 设置当前任务 (对标 current_task)
                    h.resume();
                    detail::t_current_task = nullptr;
                }
            }
        }

        running_ = false;
        awake_.store(true, std::memory_order_seq_cst); // 退出后: 不再有人需要唤醒我们
    }

} // namespace coro
