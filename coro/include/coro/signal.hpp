#pragma once

#include "io.hpp"
#include "task.hpp"

#ifdef _WIN32
// windows.h 已由 io.hpp 引入; signal.h 由 <csignal> 提供
#elif defined(__linux__)
#include <csignal>
#include <sys/eventfd.h>
#include <poll.h>
#include <pthread.h>
#include <unistd.h>
#endif

#include <algorithm>
#include <array>
#include <atomic>
#include <csignal>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

#ifdef _WIN32
// Windows CRT 的 <csignal> 没有 SIGHUP: 用空闲编号 3 表示
// 「控制台窗口关闭」事件 (CTRL_CLOSE_EVENT 的映射目标)
#ifndef SIGHUP
#define SIGHUP 3
#endif
#endif

// ============================================================================
// coro::signal — 信号事件 (对标 asyncio 的 loop.add_signal_handler)
// ============================================================================
//
// API:
//   // 1. 单次等待 (可多次并发等待)
//   co_await coro::signal::wait(SIGINT);
//
//   // 2. 持续处理 (对标 add_signal_handler): 每次信号到达时
//   //    在「注册时的 loop」上执行 factory() 返回的协程
//   coro::signal::handle(SIGTERM, [] { return shutdown_task(); });
//
// 平台实现:
//   Windows → 无真信号。控制台事件 (SetConsoleCtrlHandler: Ctrl+C /
//             Ctrl+Break / 关窗) + CRT raise() (signal() 处理器) 双路桥接,
//             经「逐等待者 loop 路由」唤醒协程 (同 future.hpp 的模式)。
//   Linux   → 阻塞信号 + signalfd, 事件源 io_uring 持续读,
//             解析 signalfd_siginfo 分发到本 loop 的等待者。
//             (无等待者时读者协程自动取消, 不阻止事件循环退出)
//
// Windows 信号映射:
//   CTRL_C_EVENT → SIGINT   CTRL_BREAK_EVENT → SIGBREAK
//   CTRL_CLOSE_EVENT → SIGHUP   LOGOFF/SHUTDOWN → SIGTERM
//
// 注意 (Linux): 信号在首次 wait/handle 时于当前线程 pthread_sigmask 阻塞;
// 之后创建的线程继承掩码。多线程程序应在创建线程前先触发一次 wait
// (或自行阻塞), 否则其他线程可能收到默认处置。
// ============================================================================

namespace coro {
    namespace signal {

        namespace detail_signal {

            // 支持的信号集合 (两平台共用; 越界信号被拒绝)
#ifdef _WIN32
            inline constexpr int supported[] = {SIGINT, SIGTERM, SIGBREAK, SIGHUP};
            inline constexpr size_t NSLOT = 4;
#else
            inline constexpr int supported[] = {SIGINT, SIGTERM, SIGHUP};
            inline constexpr size_t NSLOT = 3;
#endif

            inline size_t slot_of(int sig) {
                for (size_t i = 0; i < NSLOT; ++i)
                    if (supported[i] == sig)
                        return i;
                return NSLOT; // 不支持
            }

#ifdef _WIN32

            // ==================================================================
            // Windows 信号管理器 (进程级单例; 控制台线程 / raise 线程投递)
            // ==================================================================
            struct waiter_entry {
                void* awaiter;                  // wait_awaiter 指针 (帧内, 稳定)
                std::coroutine_handle<> handle; // 等待协程
                EventLoop* loop;                // 等待者所在的 loop (唤醒路由)
            };

            class manager {
              public:
                static manager& get() {
                    static manager m;
                    return m;
                }

                /// 注册等待者 (首个等待者安装底层处理器)
                void add(int sig, void* awaiter, std::coroutine_handle<> h, EventLoop* loop) {
                    std::lock_guard lock(mtx_);
                    size_t slot = slot_of(sig);
                    if (slot == NSLOT)
                        return;
                    install_locked();
                    waiters_[slot].push_back({awaiter, h, loop});
                }

                /// 摘除等待者 (正常恢复或帧销毁路径)
                void remove(int sig, void* awaiter) {
                    std::lock_guard lock(mtx_);
                    size_t slot = slot_of(sig);
                    if (slot == NSLOT)
                        return;
                    std::erase_if(waiters_[slot], [awaiter](const waiter_entry& w) { return w.awaiter == awaiter; });
                }

                /// 投递信号: 唤醒该信号的全部当前等待者
                /// 可从控制台处理器线程或 raise() 线程调用。
                void deliver(int sig) {
                    size_t slot = slot_of(sig);
                    if (slot == NSLOT)
                        return;
                    std::vector<waiter_entry> wake;
                    {
                        std::lock_guard lock(mtx_);
                        wake.swap(waiters_[slot]);
                    }
                    for (auto& w : wake) {
                        if (w.loop)
                            w.loop->schedule(w.handle);
                        else
                            EventLoop::get().schedule(w.handle);
                    }
                }

              private:
                void install_locked() {
                    if (installed_)
                        return;
                    installed_ = true;
                    // 路径 1: 真实控制台事件 (Ctrl+C / Ctrl+Break / 关窗)
                    SetConsoleCtrlHandler(&manager::console_handler, TRUE);
                    // 路径 2: CRT raise() (测试 / 库代码主动发信号)。
                    // 处理器里每次重装 (MSVC raise 后会复位为 SIG_DFL)。
                    // 注意 SIGHUP(Windows 自定义号) 不是 CRT 合法信号号,
                    // 不能经 raise 到达 —— 它只来自控制台关窗事件 (路径 1)。
                    std::signal(SIGINT, &manager::c_handler);
                    std::signal(SIGBREAK, &manager::c_handler);
                    std::signal(SIGTERM, &manager::c_handler);
                }

                static BOOL WINAPI console_handler(DWORD type) {
                    int sig = 0;
                    switch (type) {
                        case CTRL_C_EVENT:
                            sig = SIGINT;
                            break;
                        case CTRL_BREAK_EVENT:
                            sig = SIGBREAK;
                            break;
                        case CTRL_CLOSE_EVENT:
                            sig = SIGHUP;
                            break;
                        case CTRL_LOGOFF_EVENT:
                        case CTRL_SHUTDOWN_EVENT:
                            sig = SIGTERM;
                            break;
                        default:
                            return FALSE;
                    }
                    get().deliver(sig);
                    return TRUE; // 已消费: 阻止默认终止行为 (优雅关停的前提)
                }

                static void c_handler(int sig) {
                    // MSVC CRT 经 raise() 投递后会把处理器复位为 SIG_DFL
                    // (实证: 第二次 raise 直接走默认动作 exit(3)), 每次重装。
                    // (真实控制台事件走 SetConsoleCtrlHandler 路径, 不受影响)
                    std::signal(sig, &manager::c_handler);
                    get().deliver(sig);
                }

                std::mutex mtx_;
                std::array<std::vector<waiter_entry>, NSLOT> waiters_;
                bool installed_ = false;
            };

#elif defined(__linux__)

            // ==================================================================
            // Linux 信号管理器
            // ==================================================================
            // 实现策略: sigaction + eventfd + 全局 reader 线程
            //
            // 背景: signalfd 的 poll() 只能看到调用线程的 pending 信号,
            //   跨线程 poll signalfd 无法看到其他线程 raise 的信号。
            //   io_uring 在 kernel <5.14 也不支持 signalfd。
            //   因此改用 sigaction 信号处理器 + eventfd 跨线程通知。
            //
            // 架构:
            //   1. 全局 eventfd (进程级单例)
            //   2. sigaction 处理器: 向 eventfd 写入信号编号 (async-signal-safe)
            //   3. 全局 reader 线程: poll eventfd → 分发到全部已注册的 manager
            //   4. 每 loop 一个 manager (thread_local): 维护等待者列表
            // ==================================================================

            struct waiter_entry {
                void* awaiter;
                std::coroutine_handle<> handle;
            };

            // ── 全局信号基础设施 (进程级单例) ──
            struct global_signal_state {
                int efd = -1;
                std::thread reader_thread;
                std::atomic<bool> stopped{false};
                std::mutex managers_mtx;
                std::vector<struct manager*> managers;
                std::once_flag init_flag;

                static global_signal_state& instance() {
                    static global_signal_state s;
                    return s;
                }

                ~global_signal_state() {
                    stopped.store(true, std::memory_order_release);
                    if (reader_thread.joinable())
                        reader_thread.join();
                    if (efd >= 0)
                        ::close(efd);
                }

                void ensure_initialized() {
                    std::call_once(init_flag, [this] {
                        efd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
                        // 安装 sigaction 处理器
                        struct sigaction sa{};
                        sa.sa_handler = &global_signal_state::signal_handler;
                        sa.sa_flags = 0; // 不用 SA_RESTART, 让 poll 可中断
                        sigemptyset(&sa.sa_mask);
                        for (int sig : supported)
                            sigaction(sig, &sa, nullptr);
                        // 启动 reader 线程
                        reader_thread = std::thread([this] { reader_loop(); });
                    });
                }

                static void signal_handler(int sig) {
                    auto& s = instance();
                    if (s.efd >= 0) {
                        uint64_t val = (uint64_t)sig;
                        // write() 是 async-signal-safe
                        ::write(s.efd, &val, sizeof(val));
                    }
                }

                void reader_loop(); // 定义在 manager 之后 (需要完整类型)

                void register_manager(manager* m) {
                    std::lock_guard lock(managers_mtx);
                    managers.push_back(m);
                }

                void unregister_manager(manager* m) {
                    std::lock_guard lock(managers_mtx);
                    std::erase(managers, m);
                }
            };

            class manager {
              public:
                static manager& get() {
                    static thread_local manager m;
                    return m;
                }

                void add(int sig, void* awaiter, std::coroutine_handle<> h) {
                    size_t slot = slot_of(sig);
                    if (slot == NSLOT)
                        return;
                    {
                        std::lock_guard lock(wmtx_);
                        waiters_[slot].push_back({awaiter, h});
                    }
                    loop_ = &EventLoop::get();
                    // 注册到全局基础设施
                    auto& gs = global_signal_state::instance();
                    gs.ensure_initialized();
                    {
                        std::lock_guard lock(gs.managers_mtx);
                        if (std::find(gs.managers.begin(), gs.managers.end(), this) == gs.managers.end())
                            gs.managers.push_back(this);
                    }
                }

                void remove(int sig, void* awaiter) {
                    size_t slot = slot_of(sig);
                    if (slot == NSLOT)
                        return;
                    {
                        std::lock_guard lock(wmtx_);
                        std::erase_if(waiters_[slot], [awaiter](const waiter_entry& w) { return w.awaiter == awaiter; });
                    }
                    // 无等待者时注销 (reader 线程不再分发到本 manager)
                    if (!has_waiters()) {
                        auto& gs = global_signal_state::instance();
                        std::lock_guard lock(gs.managers_mtx);
                        std::erase(gs.managers, this);
                    }
                }

                /// 投递: 唤醒该信号的全部当前等待者
                /// 从 reader 线程调用, 用 loop_ 调度到正确的 EventLoop
                void deliver(int sig) {
                    size_t slot = slot_of(sig);
                    if (slot == NSLOT)
                        return;
                    std::vector<waiter_entry> wake;
                    {
                        std::lock_guard lock(wmtx_);
                        if (waiters_[slot].empty())
                            return;
                        wake.swap(waiters_[slot]);
                    }
                    for (auto& w : wake) {
                        if (w.handle && loop_)
                            loop_->schedule(w.handle);
                    }
                }

                bool has_waiters() const {
                    std::lock_guard lock(wmtx_);
                    for (auto& v : waiters_)
                        if (!v.empty())
                            return true;
                    return false;
                }

              private:
                std::array<std::vector<waiter_entry>, NSLOT> waiters_;
                mutable std::mutex wmtx_;
                EventLoop* loop_ = nullptr;
            };

            // reader_loop 定义 (需要 manager 完整类型)
            inline void global_signal_state::reader_loop() {
                while (!stopped.load(std::memory_order_acquire)) {
                    struct pollfd pfd = {.fd = efd, .events = POLLIN};
                    int pr = ::poll(&pfd, 1, 200);
                    if (pr <= 0) continue;
                    if (!(pfd.revents & POLLIN)) continue;
                    uint64_t val = 0;
                    ssize_t nr = ::read(efd, &val, sizeof(val));
                    if (nr == (ssize_t)sizeof(val)) {
                        int sig = (int)val;
                        std::vector<manager*> snapshot;
                        {
                            std::lock_guard lock(managers_mtx);
                            snapshot = managers;
                        }
                        for (auto* m : snapshot)
                            m->deliver(sig);
                    }
                }
            }

#endif

        } // namespace detail_signal

        // ==================================================================
        // wait — 单次等待信号
        // ==================================================================
        //
        // 用法: int s = co_await coro::signal::wait(SIGINT);
        //   - 多个协程可同时等待同一信号: Windows 下一次投递唤醒全部
        //     当前等待者; Linux signalfd 队列语义逐个唤醒
        //   - 返回值即信号编号
        //   - 等待期间协程被 cancel: 正常走取消路径 (等待者自动摘除)
        //   - 不支持的信号 (不在 {SIGINT, SIGTERM, SIGBREAK, SIGHUP}):
        //     立即抛 std::invalid_argument
        // ==================================================================
        struct wait_awaiter {
            int sig;

            bool await_ready() const {
                if (detail_signal::slot_of(sig) == detail_signal::NSLOT)
                    throw std::invalid_argument("coro::signal::wait: unsupported signal");
                return false;
            }

            void await_suspend(std::coroutine_handle<> h) {
#ifdef _WIN32
                detail_signal::manager::get().add(sig, this, h, &EventLoop::get());
#else
                detail_signal::manager::get().add(sig, this, h);
#endif
            }

            int await_resume() { return sig; }

            /// 等待者帧被销毁 (cancel 注入路径): 从等待列表摘除自己
            void on_waiter_destroyed(std::coroutine_handle<>) noexcept {
#ifdef _WIN32
                detail_signal::manager::get().remove(sig, this);
#else
                detail_signal::manager::get().remove(sig, this);
#endif
            }
        };

        inline wait_awaiter wait(int sig) {
            return wait_awaiter{sig};
        }

        // ==================================================================
        // handle — 持续处理信号 (对标 asyncio add_signal_handler)
        // ==================================================================
        //
        // 用法: coro::signal::handle(SIGTERM, [] { return shutdown_task(); });
        //   - 每次信号到达, 在「注册时的 loop」上执行 factory() 返回的协程
        //   - 处理是串行的 (上一个完成前新信号排队: 底层是等待者队列)
        //   - 常驻设施: 内部循环协程自持有, 生命周期到事件循环结束
        //   - factory 抛出的异常由 detached 任务报告机制兜底
        // ==================================================================
        namespace detail_signal {
            struct handle_state {
                int sig;
                std::function<Task<>()> factory;
                Task<> self; // 常驻循环协程 (由 handler 注册对象持有)
            };

            inline Task<> handle_loop(std::shared_ptr<handle_state> st) {
                try {
                    while (true) {
                        co_await wait(st->sig);
                        auto t = st->factory();
                        co_await std::move(t); // 串行执行, 避免处理重入
                    }
                } catch (const CancelledError&) {
                    // handler 注销 (stop/析构): 常驻循环被取消, 正常收尾
                }
            }
        } // namespace detail_signal

        /// 持续处理器注册对象 (RAII): 持有期间信号到达就执行 factory;
        /// 析构 (或调用 stop()) 时注销常驻循环, 不再阻止事件循环退出。
        class handler {
          public:
            handler() = default;
            explicit handler(std::shared_ptr<detail_signal::handle_state> st) : st_(std::move(st)) {}

            handler(handler&&) noexcept = default;
            handler& operator=(handler&&) noexcept = default;
            handler(const handler&) = delete;
            handler& operator=(const handler&) = delete;

            /// 手动注销 (幂等)
            void stop() {
                if (st_ && st_->self.handle() != nullptr)
                    st_->self.cancel(); // 挂起在 wait 上的循环协程被取消收尾
                st_.reset();
            }

            ~handler() { stop(); }

          private:
            std::shared_ptr<detail_signal::handle_state> st_;
        };

        inline handler handle(int sig, std::function<Task<>()> factory) {
            auto st = std::make_shared<detail_signal::handle_state>();
            st->sig = sig;
            st->factory = std::move(factory);
            st->self = detail_signal::handle_loop(st);
            st->self.start();
            return handler(st);
        }

    } // namespace signal
} // namespace coro
