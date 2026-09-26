#pragma once

#include <coro/coro.hpp>
#include <coro/net.hpp>
#include <coro/task_registry.hpp>

#include <atomic>
#include <chrono>
#include <functional>
#include <iostream>
#include <memory>

// ============================================================================
// coro::TcpServer — 高级 TCP 服务端框架
// ============================================================================
//
// 基于协程的 TCP 服务端: 每个连接由独立协程处理, 且这些协程**有主**。
//
// 调用流程:
//   main()
//     ├─ server.set_handler(my_handler)    // 注册: 新连接到达后干什么
//     ├─ server.set_error_handler(on_err)  // 注册: bind/accept/handler 出错时干什么
//     ├─ server.start("0.0.0.0", 8080)     // 绑定 + 登记 accept_loop 与 supervisor
//     │     ├─ accept_loop  : accept → 交给 task_registry 派生 handler 协程
//     │     └─ supervisor   : 观察到停止请求 → 排空 → 到点取消 → 报告
//     ├─ server.run_forever(...)           // 自己驱动事件循环, 收尾完成后返回
//     └─ 或: co_await server.shutdown(1s)  // 由宿主决定等待多久
//
// 关闭语义 (三个阶段, 刻意的差别):
//   stop_request() —— 只停"接入", 在途 handler 继续跑完 (优雅排空的前提)。
//   shutdown(grace)—— 可 await: 先等在途 handler 自然结束, 宽限期到点才请求取消,
//                      取消后再给 cancel_grace 收尾; 仍不结束就如实报告 unfinished。
//                      grace 是"何时开始取消"的时刻, 不是"可以强杀协程帧"的时刻。
//   stop()         —— 保留的兼容入口: stop_request() + 立即请求取消在途 handler。
//                      **不再调用 EventLoop::stop()**: 同一进程里的其他服务/任务
//                      不该因为本服务关停而被连带终止 (旧行为)。
//
// 所有权: 连接任务全部登记在 task_registry 中, 不再 spawn(...).detach();
//         handler 与错误回调通过 shared state 传递, 因此 Server 对象可以先于
//         滞留的 handler 协程析构而不会让协程访问悬空 this。
//
// 特性:
//   - 每连接一协程, 并发上限可配置 (超限的新连接被关闭并计数)
//   - 优雅关闭可 await, 有真实排空与未完成报告
//   - 计数 RAII: handler 抛异常也不漏
// ============================================================================

namespace coro {

    class TcpServer {
      public:
        // 连接处理协程: 参数是已建立的连接, 返回 Task<> 支持 co_await 异步读写
        using handler_t = std::function<Task<>(net::TcpStream)>;
        // 错误回调: 参数是错误描述字符串
        using error_handler_t = std::function<void(const std::string&)>;

        /// 生命周期阶段: idle → running → draining → cancelling → stopped
        enum class phase { idle, running, draining, cancelling, stopped };

        // 服务端配置
        struct Config {
            std::string bind_addr = "0.0.0.0";  // 绑定地址
            unsigned short port = 0;            // 绑定端口 (0 = 系统分配, 启动后 port() 回查实际值)
            int backlog = SOMAXCONN;            // listen 队列长度, 真正传给 listener
            size_t max_concurrent_handlers = 0; // 在途 handler 上限 (0 = 不限制)
            std::chrono::milliseconds shutdown_grace{1000}; // 排空在途 handler 的宽限期
            std::chrono::milliseconds cancel_grace{500};    // 请求取消后再等待的上限
        };

        TcpServer() = default;
        TcpServer(const TcpServer&) = delete;
        TcpServer& operator=(const TcpServer&) = delete;

        /// 注册连接处理协程 — 每个新连接到达时调用一次
        void set_handler(handler_t handler) { handler_ = std::move(handler); }

        /// 注册错误回调 — bind 失败 / accept 出错 / handler 抛异常时触发
        void set_error_handler(error_handler_t handler) { state_->on_error = std::move(handler); }

        /// 应用配置; 并发上限对后续 accept 立即生效
        void set_config(Config config) {
            config_ = std::move(config);
            state_->registry.set_capacity(config_.max_concurrent_handlers);
        }

        const Config& config() const noexcept { return config_; }

        /// 启动服务端 (非阻塞) — 设置地址端口后绑定并登记后台协程
        bool start(const std::string& addr, unsigned short port) {
            config_.bind_addr = addr;
            config_.port = port;
            return start();
        }

        /// 启动服务端 (用已设置的 Config)
        /// 流程: 幂等检查 → bind_listen(带 backlog) → 回查实际端口 → 登记入口协程
        bool start() {
            if (state_->phase_value.load() != phase::idle && state_->phase_value.load() != phase::stopped)
                return false;
            if (handler_ == nullptr)
                return false;

            if (!listener_.bind_listen(config_.bind_addr.c_str(), config_.port, config_.backlog)) {
                report_error("bind_listen failed");
                return false;
            }
            // 端口 0 必须由系统分配后回查, 否则调用方无法得知实际端口
            unsigned short actual = listener_.local_port();
            if (actual != 0)
                config_.port = actual;

            state_->phase_value.store(phase::running);
            // 入口协程由本对象持有 (不登记进 handler 的 registry): 否则 supervisor
            // 等待排空时会等待自己, 形成自锁。
            accept_task_ = spawn(accept_loop());
            supervisor_task_ = spawn(supervisor());
            return true;
        }

        /// 阻塞运行 — start() 后驱动事件循环, 直到本服务完成收尾才返回。
        /// 不再依赖"谁调 stop() 就把全局循环停掉"。
        void run_forever(const std::string& addr, unsigned short port) {
            if (!start(addr, port))
                return;
            EventLoop::get().run();
        }

        /// 停止接入: 关闭 listener 打断挂起的 accept, 在途 handler 继续跑完。
        void stop_request() {
            auto current = phase::running;
            if (!state_->phase_value.compare_exchange_strong(current, phase::draining))
                return; // 幂等: 已经请求过或已停止
            listener_.close();
        }

        /// 兼容入口: 停止接入并立即请求取消在途 handler。
        /// 与旧行为的差别: 不再停止所属 EventLoop, 因此不会连带终止同进程的其他服务。
        void stop() {
            stop_request();
            state_->registry.request_cancel_all();
        }

        /// 可 await 的优雅关闭: 触发停止接入, 然后等 supervisor 走完"排空 → 到点取消"。
        /// 排空状态机只有一处实现 (supervisor), 不让两条路径各自等待同一份计数。
        Task<detail::shutdown_report> shutdown(std::chrono::milliseconds grace,
                                               std::chrono::milliseconds cancel_grace) {
            state_->grace_ms.store(grace.count());
            state_->cancel_grace_ms.store(cancel_grace.count());
            stop_request();
            while (state_->phase_value.load() != phase::stopped)
                co_await coro::sleep(std::chrono::milliseconds(5));
            co_return last_report_;
        }

        Task<detail::shutdown_report> shutdown() { return shutdown(config_.shutdown_grace, config_.cancel_grace); }

        /// 本服务绑定的端口; 传 0 时返回系统实际分配的端口
        unsigned short port() const noexcept { return config_.port; }

        /// 当前在途 handler 数 (由登记表计数, handler 抛异常也不会漏减)
        size_t connection_count() const noexcept { return state_->registry.size(); }

        /// 累计受理过的连接数
        size_t total_connections() const noexcept { return state_->handled.load(); }

        /// 因并发上限被拒绝的连接数
        size_t rejected_connections() const noexcept { return state_->registry.rejected(); }

        /// 当前生命周期阶段
        phase current_phase() const noexcept { return state_->phase_value.load(); }

        /// 最近一次收尾结果 (排空是否完成、还有几个任务未结束)
        detail::shutdown_report last_shutdown_report() const noexcept { return last_report_; }

      private:
        /// 与协程共享的服务状态: 让 handler 协程只依赖 state, 不依赖 Server 的 this。
        /// on_error 必须在 start() 前设置好, 之后不再替换 (协程线程会无锁读取)。
        struct state {
            detail::task_registry registry;
            std::atomic<phase> phase_value{phase::idle};
            std::atomic<size_t> handled{0};
            std::atomic<int64_t> grace_ms{1000};
            std::atomic<int64_t> cancel_grace_ms{500};
            error_handler_t on_error;

            void note_error(const std::string& message) {
                if (on_error)
                    on_error(message);
            }
        };

        void report_error(const std::string& message) { state_->note_error(message); }

        /// accept 主循环 — 每有新连接就登记一个 handler 协程
        Task<> accept_loop() {
            auto st = state_;
            handler_t handler = handler_; // 复制进帧: 协程可能活过本对象
            while (st->phase_value.load() == phase::running) {
                auto conn = co_await listener_.accept();
                if (!conn.valid()) {
                    if (st->phase_value.load() == phase::running)
                        report_error("accept failed");
                    break;
                }
                st->handled.fetch_add(1);
                auto leased = st->registry.spawn(
                    [&conn, handler, st] { return connection_handler(std::move(conn), handler, st); });
                if (!leased.valid())
                    conn.close(); // 被容量拒绝: 连接还没被协程接管, 由这里负责关闭
            }
            co_return;
        }

        /// 单个连接的处理协程 (static: 不触碰 Server 的 this)
        static Task<> connection_handler(net::TcpStream conn, handler_t handler, std::shared_ptr<state> st) {
            try {
                co_await handler(std::move(conn));
            } catch (const CancelledError&) {
                // 关停请求导致的正常退出, 不作为故障上报
            } catch (const std::exception& e) {
                st->note_error(std::string("handler exception: ") + e.what());
            } catch (...) {
                st->note_error("handler exception: unknown");
            }
            co_return;
        }

        /// 收尾协程 — 唯一的停止状态机: running → draining → (到点) cancelling → stopped
        /// run_forever 依赖它把事件循环驱动到"本服务再无任务"。
        Task<> supervisor() {
            auto st = state_;
            while (st->phase_value.load() == phase::running)
                co_await coro::sleep(std::chrono::milliseconds(5));

            // 宽限期内不打扰在途 handler, 只等它们自己结束
            const auto drain_deadline =
                std::chrono::steady_clock::now() + std::chrono::milliseconds(st->grace_ms.load());
            while (!st->registry.empty() && std::chrono::steady_clock::now() < drain_deadline &&
                   st->phase_value.load() != phase::cancelling)
                co_await coro::sleep(std::chrono::milliseconds(5));

            if (!st->registry.empty()) {
                st->phase_value.store(phase::cancelling);
                st->registry.request_cancel_all(); // 到点才请求取消
                const auto cancel_deadline =
                    std::chrono::steady_clock::now() + std::chrono::milliseconds(st->cancel_grace_ms.load());
                while (!st->registry.empty() && std::chrono::steady_clock::now() < cancel_deadline)
                    co_await coro::sleep(std::chrono::milliseconds(5));
            }

            // 不配合取消的业务代码不能被强杀: 如实报告并保留所有权 (帧仍由 registry 持有)
            last_report_.drained = st->registry.empty();
            last_report_.unfinished = st->registry.size();
            st->phase_value.store(phase::stopped);
            listener_.close(); // 幂等: 确保 accept 挂起点被打断, 循环得以退出
            co_return;
        }

        Config config_;
        net::TcpListener listener_; // 必须比 accept_task_ 后析构
        handler_t handler_;
        std::shared_ptr<state> state_ = std::make_shared<state>();
        detail::shutdown_report last_report_{};
        Task<void> accept_task_;     // accept 主循环句柄 (入口任务, 由本对象持有)
        Task<void> supervisor_task_; // 收尾协程句柄
    };

} // namespace coro
