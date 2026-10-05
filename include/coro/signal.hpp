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
#include <atomic>
#include <csignal>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <set>
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
//   // 0. 自定义信号: 先注册 (进程级, 幂等; 默认信号无需注册)
//   coro::signal::allow(SIGUSR1);
//
//   // 1. 单次等待 (可多次并发等待)
//   co_await coro::signal::wait(SIGINT);
//
//   // 2. 持续处理 (对标 add_signal_handler): 每次信号到达时
//   //    在「注册时的 loop」上执行 factory() 返回的协程
//   coro::signal::handle(SIGTERM, [] { return shutdown_task(); });
//
//   // 3. 库内投递 (测试 / 组件触发, 效果等同信号到达)
//   coro::signal::notify(SIGUSR1);
//
// 默认白名单 (开箱即用, 无需 allow):
//   Linux   { SIGINT, SIGTERM, SIGHUP }
//   Windows { SIGINT, SIGTERM, SIGBREAK, SIGHUP }
//
// 自定义信号的可注册范围:
//   Linux   1..31 (SIGKILL/SIGSTOP 不可捕获, 拒绝) + SIGRTMIN..SIGRTMAX;
//           32/33 被 glibc 线程库保留, 拒绝注册。注册后 raise()/kill()
//           均可到达等待者。
//   Windows 任意 1..63 编号。CRT 不认识的编号 (如 42) 无法经 raise()
//           到达, 只能经 notify() 投递; CRT 认识的额外信号
//           (SIGABRT/SIGFPE/SIGILL/SIGSEGV) 注册后 raise() 可达。
//
// disallow(sig) 注销 (默认信号同样可注销): Linux 恢复 SIG_DFL,
// Windows CRT 处理器恢复 SIG_DFL; 存在等待者时拒绝并返回 false。
//
// 平台实现:
//   Windows → 无真信号。控制台事件 (SetConsoleCtrlHandler: Ctrl+C /
//             Ctrl+Break / 关窗) + CRT raise() (signal() 处理器) 双路桥接,
//             经「逐等待者 loop 路由」唤醒协程 (同 future.hpp 的模式)。
//   Linux   → sigaction 处理器把信号号写入 eventfd (async-signal-safe),
//             全局 reader 线程 poll eventfd → 分发到各线程的 manager,
//             manager 按信号号唤醒本 loop 的等待者。
//             信号不阻塞: 任意线程收到都进 handler → eventfd → 分发,
//             多线程程序无需提前 pthread_sigmask。
//
// Windows 信号映射 (控制台事件, 与注册表无关, 常驻):
//   CTRL_C_EVENT → SIGINT   CTRL_BREAK_EVENT → SIGBREAK
//   CTRL_CLOSE_EVENT → SIGHUP   LOGOFF/SHUTDOWN → SIGTERM
// ============================================================================

namespace coro {
    namespace signal {

        namespace detail_signal {

            // ── 进程级信号白名单 (默认信号 + allow() 注册的自定义信号) ──
            // magic static 初始化只放默认集合; 之后经 registry_mtx 保护增删。
            inline std::mutex registry_mtx;

            inline std::set<int>& registry() {
#ifdef _WIN32
                static std::set<int> s = {SIGINT, SIGTERM, SIGBREAK, SIGHUP};
#else
                static std::set<int> s = {SIGINT, SIGTERM, SIGHUP};
#endif
                return s;
            }

            inline bool is_allowed(int sig) {
                std::lock_guard lock(registry_mtx);
                return registry().count(sig) > 0;
            }

            /// 编号是否可注册 (与是否已在白名单无关)
            inline bool registrable(int sig) {
#ifdef _WIN32
                // Windows 无真信号: 编号仅是投递键, 1..63 均可占用
                return sig > 0 && sig < 64;
#else
                // Linux: 标准信号 1..31 (SIGKILL/SIGSTOP 不可捕获) +
                // 实时信号 SIGRTMIN..SIGRTMAX; 32/33 是 glibc 线程库保留号
                if (sig == SIGKILL || sig == SIGSTOP)
                    return false;
                if (sig > 0 && sig < 32)
                    return true;
                return sig >= SIGRTMIN && sig <= SIGRTMAX;
#endif
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

                /// 注册等待者 (同时确保 CRT 处理器已安装)
                void add(int sig, void* awaiter, std::coroutine_handle<> h, EventLoop* loop) {
                    install(sig); // 幂等: 确保 CRT handler + console handler 已安装
                    std::lock_guard lock(mtx_);
                    waiters_[sig].push_back({awaiter, h, loop});
                }

                /// 摘除等待者 (正常恢复或帧销毁路径)
                void remove(int sig, void* awaiter) {
                    std::lock_guard lock(mtx_);
                    erase_locked(sig, awaiter);
                }

                bool has_waiters(int sig) {
                    std::lock_guard lock(mtx_);
                    auto it = waiters_.find(sig);
                    return it != waiters_.end() && !it->second.empty();
                }

                /// 投递信号: 唤醒该信号的全部当前等待者。
                /// 可从控制台处理器线程、raise() 线程或 notify() 调用。
                void deliver(int sig) {
                    std::vector<waiter_entry> wake;
                    {
                        std::lock_guard lock(mtx_);
                        auto it = waiters_.find(sig);
                        if (it == waiters_.end() || it->second.empty())
                            return;
                        wake.swap(it->second);
                        waiters_.erase(it);
                    }
                    for (auto& w : wake) {
                        if (w.loop)
                            w.loop->schedule(w.handle);
                        else
                            EventLoop::get().schedule(w.handle);
                    }
                }

                /// CRT raise() 能到达的信号 (std::signal 的合法集合)
                static bool crt_reachable(int sig) {
                    switch (sig) {
                        case SIGINT:
                        case SIGILL:
                        case SIGFPE:
                        case SIGSEGV:
                        case SIGABRT:
                        case SIGTERM:
                        case SIGBREAK:
                            return true;
                        default:
                            return false;
                    }
                }

                /// 为信号安装 CRT 处理器 (allow 时调用; 控制台处理器一并确保)
                void install(int sig) {
                    std::lock_guard lock(mtx_);
                    if (!console_installed_) {
                        console_installed_ = true;
                        // 路径 1: 真实控制台事件 (Ctrl+C / Ctrl+Break / 关窗)
                        SetConsoleCtrlHandler(&manager::console_handler, TRUE);
                    }
                    if (crt_reachable(sig))
                        std::signal(sig, &manager::c_handler);
                }

                /// 卸载 CRT 处理器 (disallow 时调用; 控制台处理器保留)
                void uninstall(int sig) {
                    std::lock_guard lock(mtx_);
                    if (crt_reachable(sig))
                        std::signal(sig, SIG_DFL);
                }

              private:
                void erase_locked(int sig, void* awaiter) {
                    auto it = waiters_.find(sig);
                    if (it == waiters_.end())
                        return;
                    std::erase_if(it->second, [awaiter](const waiter_entry& w) { return w.awaiter == awaiter; });
                    if (it->second.empty())
                        waiters_.erase(it);
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
                std::map<int, std::vector<waiter_entry>> waiters_;
                bool console_installed_ = false;
            };

#elif defined(__linux__)

            // ==================================================================
            // Linux 信号基础设施
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

                /// 建立 eventfd + reader 线程 (不装任何 sigaction;
                /// sigaction 由 ensure_installed 按信号逐个安装)
                void ensure_initialized() {
                    std::call_once(init_flag, [this] {
                        efd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
                        reader_thread = std::thread([this] { reader_loop(); });
                    });
                }

                /// 幂等为 sig 安装 sigaction (先确保 efd/reader 线程存在)。
                /// 重复安装同一 handler 无副作用, 无需去重集合。
                void ensure_installed(int sig) {
                    ensure_initialized();
                    struct sigaction sa {};
                    sa.sa_handler = &global_signal_state::signal_handler;
                    sa.sa_flags = 0; // 不用 SA_RESTART, 让 poll 可中断
                    sigemptyset(&sa.sa_mask);
                    sigaction(sig, &sa, nullptr);
                }

                /// 恢复默认处置 (disallow 且无等待者时)
                void restore_default(int sig) {
                    struct sigaction sa {};
                    sa.sa_handler = SIG_DFL;
                    sigemptyset(&sa.sa_mask);
                    sigaction(sig, &sa, nullptr);
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

                /// 任意线程的 manager 是否仍有 sig 的等待者 (disallow 前检查)
                bool any_waiters(int sig);

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
                    auto& gs = global_signal_state::instance();
                    gs.ensure_installed(sig); // 幂等: sigaction + efd + reader 线程
                    {
                        std::lock_guard lock(wmtx_);
                        waiters_[sig].push_back({awaiter, h});
                    }
                    loop_ = &EventLoop::get();
                    // 注册到全局基础设施
                    {
                        std::lock_guard lock(gs.managers_mtx);
                        if (std::find(gs.managers.begin(), gs.managers.end(), this) == gs.managers.end())
                            gs.managers.push_back(this);
                    }
                }

                void remove(int sig, void* awaiter) {
                    {
                        std::lock_guard lock(wmtx_);
                        auto it = waiters_.find(sig);
                        if (it != waiters_.end()) {
                            std::erase_if(it->second,
                                          [awaiter](const waiter_entry& w) { return w.awaiter == awaiter; });
                            if (it->second.empty())
                                waiters_.erase(it);
                        }
                    }
                    // 无任何等待者时注销 (reader 线程不再分发到本 manager)
                    if (!has_waiters()) {
                        auto& gs = global_signal_state::instance();
                        std::lock_guard lock(gs.managers_mtx);
                        std::erase(gs.managers, this);
                    }
                }

                /// 投递: 唤醒该信号的全部当前等待者
                /// 从 reader 线程调用, 用 loop_ 调度到正确的 EventLoop
                void deliver(int sig) {
                    std::vector<waiter_entry> wake;
                    {
                        std::lock_guard lock(wmtx_);
                        auto it = waiters_.find(sig);
                        if (it == waiters_.end() || it->second.empty())
                            return;
                        wake.swap(it->second);
                        waiters_.erase(it);
                    }
                    for (auto& w : wake) {
                        if (w.handle && loop_)
                            loop_->schedule(w.handle);
                    }
                }

                /// 本 manager 是否还有任何等待者
                bool has_waiters() const {
                    std::lock_guard lock(wmtx_);
                    return !waiters_.empty();
                }

                /// 本 manager 是否有 sig 的等待者 (disallow 检查用)
                bool has_waiters_sig(int sig) const {
                    std::lock_guard lock(wmtx_);
                    auto it = waiters_.find(sig);
                    return it != waiters_.end() && !it->second.empty();
                }

              private:
                std::map<int, std::vector<waiter_entry>> waiters_;
                mutable std::mutex wmtx_;
                EventLoop* loop_ = nullptr;
            };

            // reader_loop 定义 (需要 manager 完整类型)
            inline void global_signal_state::reader_loop() {
                while (!stopped.load(std::memory_order_acquire)) {
                    struct pollfd pfd = {.fd = efd, .events = POLLIN};
                    int pr = ::poll(&pfd, 1, 200);
                    if (pr <= 0)
                        continue;
                    if (!(pfd.revents & POLLIN))
                        continue;
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

            inline bool global_signal_state::any_waiters(int sig) {
                std::vector<manager*> snapshot;
                {
                    std::lock_guard lock(managers_mtx);
                    snapshot = managers;
                }
                for (auto* m : snapshot)
                    if (m->has_waiters_sig(sig))
                        return true;
                return false;
            }

#endif

        } // namespace detail_signal

        // ==================================================================
        // allow / disallow / notify — 自定义信号注册与库内投递 (进程级)
        // ==================================================================
        //
        // 默认信号开箱即用; 其余信号先 allow 再 wait/handle:
        //   coro::signal::allow(SIGUSR1);
        //   co_await coro::signal::wait(SIGUSR1);
        //
        // allow: 幂等。不可注册的编号 (SIGKILL/SIGSTOP/glibc 保留的
        //   32/33/超范围) 返回 false。成功时安装平台处理器
        //   (Linux sigaction / Windows CRT + 控制台处理器)。
        //
        // disallow: 注销并恢复 SIG_DFL (默认信号同样可注销)。
        //   存在等待者时拒绝 (返回 false, 不改变任何状态)。
        //   并发「另一线程正在 wait」与 disallow 之间的窗口由调用方
        //   约束: 先停止等待方再注销。
        //
        // notify: 库内投递, 效果等同信号到达 (Linux 与真实信号同路径,
        //   经 eventfd + reader 线程分发; Windows 直接 deliver 到各
        //   等待者的 loop)。未注册或无等待者时是 no-op。测试与组件
        //   内触发专用 —— Windows 上 raise() 到不了的自定义编号靠它。
        // ==================================================================
        inline bool allow(int sig) {
            if (!detail_signal::registrable(sig))
                return false;
            std::lock_guard lock(detail_signal::registry_mtx);
            auto& reg = detail_signal::registry();
            if (reg.count(sig))
                return true; // 幂等
#ifdef _WIN32
            detail_signal::manager::get().install(sig);
#else
            detail_signal::global_signal_state::instance().ensure_installed(sig);
#endif
            reg.insert(sig);
            return true;
        }

        inline bool disallow(int sig) {
            std::lock_guard lock(detail_signal::registry_mtx);
            auto& reg = detail_signal::registry();
            if (!reg.count(sig))
                return false;
#ifdef _WIN32
            if (detail_signal::manager::get().has_waiters(sig))
                return false; // 有等待者: 拒绝注销
            detail_signal::manager::get().uninstall(sig);
#else
            if (detail_signal::global_signal_state::instance().any_waiters(sig))
                return false; // 有等待者: 拒绝注销
            detail_signal::global_signal_state::instance().restore_default(sig);
#endif
            reg.erase(sig);
            return true;
        }

        inline void notify(int sig) {
#ifdef _WIN32
            detail_signal::manager::get().deliver(sig);
#else
            // 与 signal_handler 同路径: 写 eventfd 由 reader 线程分发,
            // 调用线程无关紧要, 天然线程安全
            auto& gs = detail_signal::global_signal_state::instance();
            gs.ensure_initialized();
            if (gs.efd >= 0) {
                uint64_t val = (uint64_t)sig;
                ::write(gs.efd, &val, sizeof(val));
            }
#endif
        }

        // ==================================================================
        // wait — 单次等待信号
        // ==================================================================
        //
        // 用法: int s = co_await coro::signal::wait(SIGINT);
        //   - 多个协程可同时等待同一信号: 一次投递唤醒全部当前等待者
        //   - 返回值即信号编号
        //   - 等待期间协程被 cancel: 正常走取消路径 (等待者自动摘除)
        //   - 未注册的信号 (不在白名单): 立即抛 std::invalid_argument,
        //     提示先调 coro::signal::allow(sig)
        // ==================================================================
        struct wait_awaiter {
            int sig;

            bool await_ready() const {
                if (!detail_signal::is_allowed(sig))
                    throw std::invalid_argument(
                        "coro::signal::wait: signal not allowed (call coro::signal::allow(sig) first)");
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
                detail_signal::manager::get().remove(sig, this);
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
        //   - 未注册的信号: 同步抛 std::invalid_argument (不等首信号)
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
            if (!detail_signal::is_allowed(sig))
                throw std::invalid_argument(
                    "coro::signal::handle: signal not allowed (call coro::signal::allow(sig) first)");
            auto st = std::make_shared<detail_signal::handle_state>();
            st->sig = sig;
            st->factory = std::move(factory);
            st->self = detail_signal::handle_loop(st);
            st->self.start();
            return handler(st);
        }

    } // namespace signal
} // namespace coro
