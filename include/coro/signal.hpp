#pragma once

#include "io.hpp"
#include "sync.hpp"
#include "task.hpp"

#ifdef _WIN32
// windows.h 已由 io.hpp 引入; signal.h 由 <csignal> 提供
#elif defined(__linux__)
#include <cerrno>
#include <csignal>
#include <fcntl.h>
#include <poll.h>
#include <pthread.h>
#include <unistd.h>
#endif

#include <algorithm>
#include <array>
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
//   Linux   → sigaction 处理器把信号号逐字节写入 self-pipe (async-signal-safe),
//             全局 reader 线程读管道 → 分发到各线程的 manager,
//             manager 按信号号唤醒本 loop 的等待者。
//             不用 eventfd: 它是累加计数器, 并发/连续信号会相加成另一个编号。
//             信号不阻塞: 任意线程收到都进 handler → 管道 → 分发,
//             多线程程序无需提前 pthread_sigmask。
//             管道满时按信号号合并 (不承诺实时信号的逐次不丢)。
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
            class manager; // 前向声明: 桥接的泵线程需要调用 manager::deliver

            struct waiter_entry {
                void* awaiter;                  // wait_awaiter 指针 (帧内, 稳定)
                std::coroutine_handle<> handle; // 等待协程
                EventLoop* loop;                // 等待者所在的 loop (唤醒路由)
            };

            /// 信号 → 事件循环的安全桥接。
            /// CRT/控制台处理器里**不能**加锁、分配或调用调度器 (它们可能在
            /// 任意线程、甚至正在持锁的循环线程上被调用, 直接 deliver 会死锁)。
            /// 处理器只置 atomic 位 + SetEvent; 专用 pump 线程负责真正的分发。
            class signal_bridge {
              public:
                static signal_bridge& instance() {
                    static signal_bridge b;
                    return b;
                }

                /// 幂等建立事件对象与泵线程
                void ensure_started() {
                    std::call_once(once_, [this] {
                        event_ = CreateEventW(nullptr, FALSE, FALSE, nullptr);
                        if (event_ == nullptr)
                            return;
                        pump_ = std::thread([this] { pump_loop(); });
                    });
                }

                /// 信号处理器专用: 只做原子置位 + 唤醒 (不锁、不分配)
                void post(int sig) noexcept {
                    if (sig <= 0 || sig >= 64 || event_ == nullptr)
                        return;
                    pending_[static_cast<std::size_t>(sig)].store(true, std::memory_order_release);
                    SetEvent(event_);
                }

                ~signal_bridge() {
                    stop_.store(true, std::memory_order_release);
                    if (event_ != nullptr)
                        SetEvent(event_);
                    if (pump_.joinable())
                        pump_.join();
                    if (event_ != nullptr)
                        CloseHandle(event_);
                }

              private:
                void pump_loop(); // 定义在 manager 之后 (需要完整类型)

                std::array<std::atomic<bool>, 64> pending_{};
                HANDLE event_ = nullptr;
                std::thread pump_;
                std::atomic<bool> stop_{false};
                std::once_flag once_;
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
                    // waiter_entry.loop 由 add() 以 &EventLoop::get() 写入, 恒非空
                    for (auto& w : wake)
                        w.loop->schedule(w.handle);
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
                    signal_bridge::instance().ensure_started(); // 泵线程就绪后再装处理器
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
                    // 控制台处理器运行在系统注入的线程上: 同样只置位 + 唤醒
                    signal_bridge::instance().post(sig);
                    return TRUE; // 已消费: 阻止默认终止行为 (优雅关停的前提)
                }

                static void c_handler(int sig) {
                    // MSVC CRT 经 raise() 投递后会把处理器复位为 SIG_DFL
                    // (实证: 第二次 raise 直接走默认动作 exit(3)), 每次重装。
                    // (真实控制台事件走 SetConsoleCtrlHandler 路径, 不受影响)
                    std::signal(sig, &manager::c_handler);
                    signal_bridge::instance().post(sig);
                }

                std::mutex mtx_;
                std::map<int, std::vector<waiter_entry>> waiters_;
                bool console_installed_ = false;
            };

            inline void signal_bridge::pump_loop() {
                while (!stop_.load(std::memory_order_acquire)) {
                    WaitForSingleObject(event_, 200);
                    for (int sig = 1; sig < 64; ++sig) {
                        if (pending_[static_cast<std::size_t>(sig)].exchange(false, std::memory_order_acq_rel))
                            manager::get().deliver(sig);
                    }
                }
            }

#elif defined(__linux__)

            // ==================================================================
            // Linux 信号基础设施
            // ==================================================================
            // 实现策略: sigaction + self-pipe + 全局 reader 线程
            //
            // 为什么不是 eventfd: eventfd 是累加计数器, 写进去的不是“一条消息”。
            // 连续写入信号号 n1、n2 后读出的是 n1+n2 —— 会被当成第三个信号,
            // 同一信号两次也会变成另一个编号。信号号必须逐字节传递。
            //
            // 架构:
            //   1. 进程级非阻塞 self-pipe (O_CLOEXEC), handler 侧只 write 一个字节
            //   2. 管道满 (EAGAIN) 时记入 per-signal 的 lock-free 待决位图,
            //      由 reader 线程在排空管道后补发 (合并语义, 不承诺逐次不丢)
            //   3. 全局 reader 线程 poll → 分发给各 loop 的 manager
            //   4. 每 loop 一个 manager (thread_local), 维护等待者列表
            // ==================================================================

            struct waiter_entry {
                void* awaiter;
                std::coroutine_handle<> handle;
            };

            /// 每个事件循环的等待集合。用 shared_ptr 保活: reader 线程只持有
            /// weak_ptr 快照, thread_local manager 随线程退出销毁时, 不会留下
            /// 可被解引用的悬空指针。
            struct manager_core {
                std::map<int, std::vector<waiter_entry>> waiters;
                mutable std::mutex mtx;
                EventLoop* loop = nullptr; ///< 构造时确定 (本线程 loop), 之后只读

                void add(int sig, void* awaiter, std::coroutine_handle<> h) {
                    std::lock_guard lock(mtx);
                    waiters[sig].push_back({awaiter, h});
                }

                void remove(int sig, void* awaiter) {
                    std::lock_guard lock(mtx);
                    auto it = waiters.find(sig);
                    if (it == waiters.end())
                        return;
                    std::erase_if(it->second, [awaiter](const waiter_entry& w) { return w.awaiter == awaiter; });
                    if (it->second.empty())
                        waiters.erase(it);
                }

                /// 投递: 唤醒该信号的全部当前等待者 (从 reader 线程调用)
                void deliver(int sig) {
                    std::vector<waiter_entry> wake;
                    {
                        std::lock_guard lock(mtx);
                        auto it = waiters.find(sig);
                        if (it == waiters.end() || it->second.empty())
                            return;
                        wake.swap(it->second);
                        waiters.erase(it);
                    }
                    for (auto& w : wake) {
                        if (w.handle && loop_ != nullptr)
                            loop_->schedule(w.handle);
                    }
                }

                bool has_waiters() const {
                    std::lock_guard lock(mtx);
                    return !waiters.empty();
                }

                bool has_waiters_sig(int sig) const {
                    std::lock_guard lock(mtx);
                    auto it = waiters.find(sig);
                    return it != waiters.end() && !it->second.empty();
                }

                EventLoop* loop_ptr() const noexcept { return loop_; }
                void set_loop(EventLoop* l) noexcept { loop_ = l; }

              private:
                EventLoop* loop_ = nullptr;
            };

            // ── 全局信号基础设施 (进程级单例) ──
            struct global_signal_state {
                int rfd = -1; ///< reader 端 (非阻塞)
                int wfd = -1; ///< handler 端 (非阻塞)
                std::thread reader_thread;
                std::atomic<bool> stopped{false};
                std::mutex managers_mtx;
                std::vector<std::weak_ptr<manager_core>> managers;
                std::once_flag init_flag;
                /// 管道满时的兜底位图 (每个信号一位, 1..63)。
                /// atomic<bool> 恒定 lock-free, 在信号处理器中读写安全。
                std::array<std::atomic<bool>, 64> pending{};

                static global_signal_state& instance() {
                    static global_signal_state s;
                    return s;
                }

                ~global_signal_state() {
                    stopped.store(true, std::memory_order_release);
                    if (reader_thread.joinable())
                        reader_thread.join();
                    if (rfd >= 0)
                        ::close(rfd);
                    if (wfd >= 0)
                        ::close(wfd);
                }

                /// 建立 self-pipe + reader 线程 (不装任何 sigaction;
                /// sigaction 由 ensure_installed 按信号逐个安装)
                void ensure_initialized() {
                    std::call_once(init_flag, [this] {
                        int fds[2] = {-1, -1};
                        if (::pipe2(fds, O_NONBLOCK | O_CLOEXEC) != 0)
                            return; // 拿不到管道: 后续 write 分支自然 no-op, 不崩溃
                        rfd = fds[0];
                        wfd = fds[1];
                        reader_thread = std::thread([this] { reader_loop(); });
                    });
                }

                /// 幂等为 sig 安装 sigaction (先确保 pipe/reader 线程存在)。
                /// 重复安装同一 handler 无副作用, 无需去重集合。
                void ensure_installed(int sig) {
                    ensure_initialized();
                    struct sigaction sa{};
                    sa.sa_handler = &global_signal_state::signal_handler;
                    // SA_RESTART: 处理器是进程级的 —— 没有 RESTART 时, 任何
                    // 线程收到允许的信号都会让该线程当前的阻塞系统调用以
                    // EINTR 失败 (to_thread 工作者的读写、用户自己的 syscall
                    // 随机被打断)。reader 线程的 poll 本来就带 200ms 超时,
                    // 不依赖信号打断 (self-pipe 写入让它立即返回)。
                    sa.sa_flags = SA_RESTART;
                    sigemptyset(&sa.sa_mask);
                    sigaction(sig, &sa, nullptr);
                }

                /// 恢复默认处置 (disallow 且无等待者时)
                void restore_default(int sig) {
                    struct sigaction sa{};
                    sa.sa_handler = SIG_DFL;
                    sigemptyset(&sa.sa_mask);
                    sigaction(sig, &sa, nullptr);
                }

                /// 投递一条记录。信号上下文与普通上下文共用; 失败走合并位图。
                void post(int sig) noexcept {
                    if (wfd < 0)
                        return;
                    const unsigned char rec = static_cast<unsigned char>(sig & 0x7F);
                    if (::write(wfd, &rec, 1) != 1)
                        mark_pending(sig);
                }

                void mark_pending(int sig) noexcept {
                    if (sig > 0 && sig < 64)
                        pending[static_cast<std::size_t>(sig)].store(true, std::memory_order_release);
                }

                static void signal_handler(int sig) noexcept {
                    // 处理器内只做: 保存 errno → write/原子位 → 恢复 errno。
                    // 不加锁、不分配、不调调度器 (那些在 reader 线程做)。
                    const int saved = errno;
                    instance().post(sig);
                    errno = saved;
                }

                void reader_loop(); // 定义在 manager 之后 (需要完整类型)

                /// 把一个信号分发给当前所有 manager (副本持有 shared_ptr, 保证存活)
                void dispatch(int sig) {
                    std::vector<std::shared_ptr<manager_core>> snapshot;
                    {
                        std::lock_guard lock(managers_mtx);
                        std::erase_if(managers, [](const std::weak_ptr<manager_core>& w) { return w.expired(); });
                        snapshot.reserve(managers.size());
                        for (auto& w : managers) {
                            if (auto core = w.lock())
                                snapshot.push_back(std::move(core));
                        }
                    }
                    for (auto& core : snapshot)
                        core->deliver(sig);
                }

                /// 任意线程的 manager 是否仍有 sig 的等待者 (disallow 前检查)
                bool any_waiters(int sig) {
                    std::vector<std::shared_ptr<manager_core>> snapshot;
                    {
                        std::lock_guard lock(managers_mtx);
                        for (auto& w : managers) {
                            if (auto core = w.lock())
                                snapshot.push_back(std::move(core));
                        }
                    }
                    for (auto& core : snapshot) {
                        if (core->has_waiters_sig(sig))
                            return true;
                    }
                    return false;
                }

                void register_manager(const std::shared_ptr<manager_core>& core) {
                    std::lock_guard lock(managers_mtx);
                    managers.push_back(core);
                }
            };

            class manager {
              public:
                /// 首次使用时创建并登记 core; 线程退出时 weak_ptr 自动失效
                static manager& get() {
                    static thread_local manager m;
                    return m;
                }

                manager() : core_(std::make_shared<manager_core>()) {
                    core_->set_loop(&EventLoop::get());
                    global_signal_state::instance().register_manager(core_);
                }

                void add(int sig, void* awaiter, std::coroutine_handle<> h) {
                    global_signal_state::instance().ensure_installed(sig); // 幂等
                    core_->add(sig, awaiter, h);
                }

                void remove(int sig, void* awaiter) { core_->remove(sig, awaiter); }

                void deliver(int sig) { core_->deliver(sig); }

                bool has_waiters() const { return core_->has_waiters(); }

                bool has_waiters_sig(int sig) const { return core_->has_waiters_sig(sig); }

              private:
                std::shared_ptr<manager_core> core_;
            };

            // reader_loop 定义 (需要 manager 完整类型)
            inline void global_signal_state::reader_loop() {
                while (!stopped.load(std::memory_order_acquire)) {
                    struct pollfd pfd = {.fd = rfd, .events = POLLIN};
                    const int pr = ::poll(&pfd, 1, 200);
                    if (pr > 0 && (pfd.revents & POLLIN)) {
                        unsigned char rec = 0;
                        while (::read(rfd, &rec, 1) == 1)
                            dispatch(rec); // 逐字节: 每个字节就是一条独立记录
                    }
                    // 管道满期间被合并的信号在这里补发
                    for (int sig = 1; sig < 64; ++sig) {
                        if (pending[static_cast<std::size_t>(sig)].exchange(false, std::memory_order_acq_rel))
                            dispatch(sig);
                    }
                }
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
            // 与 signal_handler 同路径: 写 self-pipe 由 reader 线程分发,
            // 调用线程无关紧要, 天然线程安全
            auto& gs = detail_signal::global_signal_state::instance();
            gs.ensure_initialized();
            gs.post(sig);
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
        //   - 收集与执行分离: 收集协程持续注册等待, 因此 factory 运行期间的
        //     信号不会被丢掉, 而是计数合并后逐个执行 (标准信号允许合并)
        //   - 常驻设施: 内部协程自持有, 生命周期到 handler 注销/事件循环结束
        //   - factory 抛出的异常由 detached 任务报告机制兜底
        //   - 未注册的信号: 同步抛 std::invalid_argument (不等首信号)
        // ==================================================================
        namespace detail_signal {
            struct handle_state {
                int sig = 0;
                std::function<Task<>()> factory;
                Event wake;                  ///< 收集端→执行端的唤醒事件
                std::atomic<int> pending{0}; ///< 待执行次数 (合并计数)
                Task<> collector;            ///< 持续等待信号的收集协程
                Task<> self;                 ///< 串行执行 factory 的循环协程
            };

            /// 收集端: 始终处于“等待下一个信号”的状态, 因此业务执行期间的
            /// 信号也能被记录 (旧实现只在两次 factory 之间注册, 会丢信号)。
            inline Task<> handle_collect(std::shared_ptr<handle_state> st) {
                try {
                    while (true) {
                        co_await wait(st->sig);
                        st->pending.fetch_add(1, std::memory_order_release);
                        st->wake.set();
                    }
                } catch (const CancelledError&) {
                    // handler 注销: 收集端被取消, 正常收尾
                }
            }

            inline Task<> handle_loop(std::shared_ptr<handle_state> st) {
                try {
                    while (true) {
                        if (st->pending.load(std::memory_order_acquire) > 0) {
                            st->wake.clear();
                            int n = st->pending.exchange(0, std::memory_order_acq_rel);
                            for (int i = 0; i < n; ++i) {
                                auto t = st->factory();
                                co_await std::move(t); // 串行执行, 避免处理重入
                            }
                            continue;
                        }
                        co_await st->wake.wait();
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

            handler(handler&& other) noexcept : st_(std::move(other.st_)) {}

            /// 移动赋值必须先停掉旧注册, 否则旧状态的常驻协程会永远留在 loop 里,
            /// 阻止事件循环退出 (旧实现用默认移动赋值直接覆盖 st_)。
            handler& operator=(handler&& other) noexcept {
                if (this != &other) {
                    stop();
                    st_ = std::move(other.st_);
                }
                return *this;
            }
            handler(const handler&) = delete;
            handler& operator=(const handler&) = delete;

            /// 手动注销 (幂等)
            void stop() {
                if (!st_)
                    return;
                if (st_->self.handle() != nullptr)
                    st_->self.cancel(); // 挂起在 wait 上的循环协程被取消收尾
                if (st_->collector.handle() != nullptr)
                    st_->collector.cancel();
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
            st->collector = detail_signal::handle_collect(st);
            st->collector.start();
            st->self = detail_signal::handle_loop(st);
            st->self.start();
            return handler(st);
        }

    } // namespace signal
} // namespace coro
