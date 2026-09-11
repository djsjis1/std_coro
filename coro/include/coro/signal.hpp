#pragma once

#include "io.hpp"
#include "task.hpp"

#ifdef _WIN32
// windows.h 已由 io.hpp 引入; signal.h 由 <csignal> 提供
#elif defined(__linux__)
#include <csignal>
#include <sys/signalfd.h>
#include <pthread.h>
#include <unistd.h>
#endif

#include <array>
#include <csignal>
#include <functional>
#include <memory>
#include <mutex>
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
            inline constexpr int supported[] = {SIGINT, SIGTERM, SIGBREAK, SIGHUP};
            inline constexpr size_t NSLOT = 4;

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
            // Linux 信号管理器 (每 loop 一个: thread_local; 全部访问都在本
            // loop 线程上 —— add/remove 来自协程挂起/恢复, deliver 来自读者协程)
            // ==================================================================
            // 注意: 本仓库在 Windows 上开发, 本节需要 Linux 环境编译验证。

            struct reader_state; // 前向声明 (定义在下方)

            struct waiter_entry {
                void* awaiter;
                std::coroutine_handle<> handle;
            };

            class manager {
              public:
                static manager& get() {
                    static thread_local manager m; // 每 loop (线程) 一个
                    return m;
                }

                int sfd() const { return sfd_; }

                /// 注册等待者; 必要时阻塞信号 + 创建/更新 signalfd + 启动读者
                void add(int sig, void* awaiter, std::coroutine_handle<> h) {
                    size_t slot = slot_of(sig);
                    if (slot == NSLOT)
                        return;
                    waiters_[slot].push_back({awaiter, h});

                    // 阻塞本线程的该信号 (signalfd 的前提; 之后创建的线程继承)
                    sigset_t set;
                    sigemptyset(&set);
                    sigaddset(&set, sig);
                    pthread_sigmask(SIG_BLOCK, &set, nullptr);

                    // signalfd 掩码累计 (首次创建, 之后原地更新)
                    sigaddset(&mask_, sig);
                    if (sfd_ < 0)
                        sfd_ = signalfd(-1, &mask_, SFD_NONBLOCK | SFD_CLOEXEC);
                    else
                        signalfd(sfd_, &mask_, SFD_NONBLOCK | SFD_CLOEXEC);

                    ensure_reader();
                }

                void remove(int sig, void* awaiter) {
                    size_t slot = slot_of(sig);
                    if (slot == NSLOT)
                        return;
                    std::erase_if(waiters_[slot], [awaiter](const waiter_entry& w) { return w.awaiter == waiter; });
                    maybe_stop_reader();
                }

                /// 投递 (读者协程在 loop 线程调用): 唤醒该信号的一个等待者。
                /// 无等待者余留时停掉读者 (挂起的 signalfd 读被取消,
                /// 事件循环不会被常驻读者拖住无法退出)。
                void deliver(int sig) {
                    size_t slot = slot_of(sig);
                    if (slot == NSLOT || waiters_[slot].empty())
                        return;
                    waiter_entry w = waiters_[slot].front();
                    waiters_[slot].erase(waiters_[slot].begin());
                    if (w.handle)
                        EventLoop::get().schedule(w.handle);
                    maybe_stop_reader();
                }

                bool has_waiters() const {
                    for (auto& v : waiters_)
                        if (!v.empty())
                            return true;
                    return false;
                }

                // 读者协程的自持有状态 (避免 reader_task_ 未初始化先被引用)
                std::shared_ptr<reader_state> rstate;

              private:
                void ensure_reader();
                void maybe_stop_reader();

                std::array<std::vector<waiter_entry>, NSLOT> waiters_;
                sigset_t mask_{};
                int sfd_ = -1;
                bool reader_started_ = false;
            };

            // signalfd 的一次 io_uring 读 (结构同 pipe/fs 的读)
            struct sfd_read_awaiter {
                int fd;
                void* buf;
                size_t len;
                detail::uring_op op;

                bool await_ready() const noexcept { return false; }

                static void cancel_op(void* self) {
                    auto* aw = static_cast<sfd_read_awaiter*>(self);
                    if (auto* u = EventLoop::get().uring()) {
                        io_uring_sqe* sqe = io_uring_get_sqe(u->handle());
                        if (sqe) {
                            io_uring_prep_cancel(sqe, &aw->op, 0);
                            io_uring_submit(u->handle());
                        }
                    }
                }

                void await_suspend(std::coroutine_handle<> h) {
                    op.continuation = h;
                    auto* u = EventLoop::get().uring();
                    if (!u) {
                        op.result = -ENOTSUP;
                        EventLoop::get().schedule(h);
                        return;
                    }
                    io_uring_sqe* sqe = io_uring_get_sqe(u->handle());
                    if (!sqe) {
                        op.result = -ENOBUFS;
                        EventLoop::get().schedule(h);
                        return;
                    }
                    io_uring_prep_read(sqe, fd, buf, (unsigned)len, -1);
                    detail::uring_submit(u, sqe, &op);
                }

                int await_resume() { return op.error ? -1 : op.result; }
            };

            // 读者协程状态: 自持有 (rstate 由帧和 manager 共享)
            struct reader_state {
                int sfd = -1;
                Task<> self; // 读者协程自身 (停止时 cancel)
            };

            // 读者循环: 持续读 signalfd, 每条 signalfd_siginfo 分发一个等待者。
            // 无等待者时被 cancel → CancelledError → 收尾退出 (不阻止 loop 退出)
            inline Task<> reader_loop(std::shared_ptr<reader_state> st) {
                try {
                    while (true) {
                        signalfd_siginfo si;
                        int n = co_await sfd_read_awaiter{st->sfd, &si, sizeof(si), {}};
                        if (n != (int)sizeof(si))
                            break; // fd 关闭或错误: 退出读者
                        manager::get().deliver((int)si.ssi_signo);
                    }
                } catch (const CancelledError&) {
                    // 正常停止路径 (最后一个等待者离开时被 cancel)
                }
            }

            inline void manager::ensure_reader() {
                if (reader_started_)
                    return;
                reader_started_ = true;
                rstate = std::make_shared<reader_state>();
                rstate->sfd = sfd_;
                rstate->self = reader_loop(rstate);
                rstate->self.start(); // 常驻: 挂起在 signalfd 读上
            }

            inline void manager::maybe_stop_reader() {
                if (!reader_started_ || has_waiters())
                    return;
                // 无等待者: 取消读者 (挂起的 uring 读被 ASYNC_CANCEL,
                // 读者协程收到 CancelledError 退出), signalfd 留待下次复用
                rstate->self.cancel();
                reader_started_ = false;
                rstate.reset();
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

            int await_resume() {
                return sig;
            }

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
