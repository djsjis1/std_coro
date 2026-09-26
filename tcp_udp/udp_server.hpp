#pragma once

#include <coro/coro.hpp>
#include <coro/net.hpp>
#include <coro/task_registry.hpp>

#include <atomic>
#include <chrono>
#include <functional>
#include <iostream>
#include <memory>
#include <vector>

#ifdef _WIN32
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#endif

// ============================================================================
// coro::UdpServer — 高级 UDP 服务端框架
// ============================================================================
//
// 基于协程的 UDP 服务端: 每个数据报由独立协程处理, 且这些协程**有主**。
//
// 调用流程:
//   main()
//     ├─ server.set_handler(my_handler)    // 注册: 收到数据报后干什么
//     ├─ server.set_error_handler(on_err)  // 注册: bind/recvfrom/handler 出错时干什么
//     ├─ server.start("0.0.0.0", 8801)     // 绑定 + 登记 recv_loop 与 supervisor
//     │     ├─ recv_loop : recvfrom → 拷贝独立缓冲 → 交给 task_registry 派生协程
//     │     └─ supervisor: 观察到停止请求 → 排空 → 到点取消 → 报告
//     ├─ server.run_forever(...)           // 自己驱动事件循环, 收尾完成后返回
//     └─ 或: co_await server.shutdown(1s)
//
// 关闭语义与 TcpServer 一致 (stop_request / shutdown(grace) / stop):
//   stop() 不再停止所属 EventLoop, 因此不会连带终止同进程里的其他服务。
//
// 背压与丢弃 (UDP 没有连接, 满载时必须显式取舍):
//   max_concurrent_handlers —— 在途 handler 上限, 超限的数据报被丢弃并计入 dropped_datagrams()
//   max_pending_bytes       —— 在途 handler 持有的缓冲总量上限, 超限同样丢弃并计数
//   两条策略都在"数据报已被取出"的时刻生效: 内核侧的 socket 缓冲由 OS 负责, 本类不假装能管住它。
//
// 特性:
//   - 每数据报一协程, 每数据报独立缓冲拷贝 (异步 handler 期间不会被下一个包覆盖)
//   - 优雅关闭可 await, 有真实排空与未完成报告
//   - 计数 RAII: handler 抛异常也不漏
// ============================================================================

namespace coro {

    class UdpServer {
      public:
        // 数据报处理协程: (数据内容, 长度, 发送方地址)
        using handler_t = std::function<Task<>(const char*, size_t, const sockaddr_in&)>;
        using error_handler_t = std::function<void(const std::string&)>;

        /// 生命周期阶段: idle → running → draining → cancelling → stopped
        enum class phase { idle, running, draining, cancelling, stopped };

        // 服务端配置
        struct Config {
            std::string bind_addr = "0.0.0.0";  // 绑定地址
            unsigned short port = 0;            // 绑定端口 (0 = 系统分配, 启动后 port() 回查)
            size_t max_datagram_size = 65535;   // 单个数据报缓冲上限, 同时决定接收缓冲大小
            size_t max_concurrent_handlers = 0; // 在途 handler 上限 (0 = 不限制)
            size_t max_pending_bytes = 0;       // 在途缓冲总量上限 (0 = 不限制)
            std::chrono::milliseconds shutdown_grace{1000}; // 排空在途 handler 的宽限期
            std::chrono::milliseconds cancel_grace{500};    // 请求取消后再等待的上限
        };

        UdpServer() = default;
        UdpServer(const UdpServer&) = delete;
        UdpServer& operator=(const UdpServer&) = delete;

        /// 注册数据报处理协程 — 每收到一个 UDP 数据报调用一次
        void set_handler(handler_t handler) { handler_ = std::move(handler); }

        /// 注册错误回调 — bind 失败 / recvfrom 出错 / handler 抛异常时触发
        void set_error_handler(error_handler_t handler) { state_->on_error = std::move(handler); }

        /// 应用配置; 上限对后续数据报立即生效。接收缓冲在 start() 时按配置分配。
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
        bool start() {
            auto current = state_->phase_value.load();
            if (current != phase::idle && current != phase::stopped)
                return false;
            if (handler_ == nullptr)
                return false;

            // 接收缓冲按配置分配: 旧实现硬编码 65535, Config 里的上限形同虚设。
            if (recv_buf_.size() != config_.max_datagram_size)
                recv_buf_.assign(config_.max_datagram_size, '\0');

            if (!socket_.bind_listen(config_.bind_addr.c_str(), config_.port)) {
                report_error("UDP bind failed");
                return false;
            }
            // 端口 0 必须回查系统实际分配值, 否则调用方无从得知
            unsigned short actual = socket_.local_port();
            if (actual != 0)
                config_.port = actual;

            state_->phase_value.store(phase::running);
            // 入口协程由本对象持有 (不登记进 handler 的 registry): 否则 supervisor
            // 等待排空时会等待自己, 形成自锁。
            recv_task_ = spawn(recv_loop());
            supervisor_task_ = spawn(supervisor());
            return true;
        }

        /// 阻塞运行 — start() 后驱动事件循环, 直到本服务完成收尾才返回
        void run_forever(const std::string& addr, unsigned short port) {
            if (!start(addr, port))
                return;
            EventLoop::get().run();
        }

        /// 停止接入: 关闭 socket 打断挂起的 recvfrom, 在途 handler 继续跑完
        void stop_request() {
            auto current = phase::running;
            if (!state_->phase_value.compare_exchange_strong(current, phase::draining))
                return; // 幂等
            socket_.close();
        }

        /// 兼容入口: 停止接入并立即请求取消在途 handler (不再停止所属 EventLoop)
        void stop() {
            stop_request();
            state_->registry.request_cancel_all();
        }

        /// 可 await 的优雅关闭: 触发停止接入, 然后等 supervisor 走完"排空 → 到点取消"
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

        /// 累计收到的数据报数 (含被丢弃的; 与旧 datagram_count 语义一致: 已受理数)
        size_t datagram_count() const noexcept { return state_->handled.load(); }

        /// 当前在途 handler 数
        size_t active_handlers() const noexcept { return state_->registry.size(); }

        /// 因并发上限或字节上限被丢弃的数据报数
        size_t dropped_datagrams() const noexcept { return state_->registry.rejected() + state_->bytes_dropped.load(); }

        /// 当前生命周期阶段
        phase current_phase() const noexcept { return state_->phase_value.load(); }

        /// 最近一次收尾结果
        detail::shutdown_report last_shutdown_report() const noexcept { return last_report_; }

        /// 获取底层 UdpSocket — 供 handler 调用 sendto 回复数据
        net::UdpSocket& socket() { return socket_; }

      private:
        /// 与协程共享的服务状态: handler 协程只依赖 state, 不依赖 Server 的 this。
        struct state {
            detail::task_registry registry;
            std::atomic<phase> phase_value{phase::idle};
            std::atomic<size_t> handled{0};
            std::atomic<size_t> bytes_dropped{0};
            std::atomic<size_t> pending_bytes{0};
            std::atomic<int64_t> grace_ms{1000};
            std::atomic<int64_t> cancel_grace_ms{500};
            error_handler_t on_error;

            void note_error(const std::string& message) {
                if (on_error)
                    on_error(message);
            }

            /// 预留待处理字节额度; limit = 0 表示不限制。失败即调用方应丢弃该数据报。
            bool reserve_bytes(size_t need, size_t limit) {
                if (limit == 0)
                    return true;
                size_t current = pending_bytes.load();
                for (;;) {
                    if (current + need > limit)
                        return false;
                    if (pending_bytes.compare_exchange_weak(current, current + need))
                        return true;
                }
            }

            void release_bytes(size_t freed) { pending_bytes.fetch_sub(freed); }
        };

        void report_error(const std::string& message) { state_->note_error(message); }

        /// 接收主循环 — 每收到一个数据报就拷贝独立缓冲并登记处理协程
        Task<> recv_loop() {
            auto st = state_;
            handler_t handler = handler_; // 复制进帧: 协程可能活过本对象
            while (st->phase_value.load() == phase::running) {
                sockaddr_in sender{}; // 内核填入发送方 IP 与端口
                int n = co_await socket_.recvfrom(recv_buf_.data(), recv_buf_.size(), &sender);
                if (n < 0) {
                    if (st->phase_value.load() == phase::running)
                        report_error("recvfrom failed");
                    break;
                }
                // 零长度数据报也是合法输入, 计入并交给 handler (旧实现直接 continue 吞掉)
                st->handled.fetch_add(1);
                if (handler == nullptr)
                    continue;

                std::vector<char> data(recv_buf_.begin(), recv_buf_.begin() + n);
                const size_t bytes = data.size();
                if (!st->reserve_bytes(bytes, config_.max_pending_bytes)) {
                    st->bytes_dropped.fetch_add(1); // 在途数据量已达上限: 显式丢弃并计数
                    continue;
                }
                auto leased =
                    st->registry.spawn([data = std::move(data), handler, sender, st, bytes]() mutable -> Task<> {
                        return datagram_handler(std::move(data), handler, sender, st, bytes);
                    });
                if (!leased.valid())
                    st->release_bytes(bytes); // 容量拒绝时 registry 已计入 rejected, 这里只归还额度
            }
            co_return;
        }

        /// 单个数据报的处理协程 (static: 不触碰 Server 的 this); 字节额度由 RAII 归还
        static Task<> datagram_handler(std::vector<char> data, handler_t handler, sockaddr_in sender,
                                       std::shared_ptr<state> st, size_t bytes) {
            struct byte_scope {
                std::shared_ptr<state> st;
                size_t bytes;
                ~byte_scope() { st->release_bytes(bytes); }
            } budget{st, bytes};

            try {
                co_await handler(data.data(), data.size(), sender);
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
        Task<> supervisor() {
            auto st = state_;
            while (st->phase_value.load() == phase::running)
                co_await coro::sleep(std::chrono::milliseconds(5));

            const auto drain_deadline =
                std::chrono::steady_clock::now() + std::chrono::milliseconds(st->grace_ms.load());
            while (!st->registry.empty() && std::chrono::steady_clock::now() < drain_deadline &&
                   st->phase_value.load() != phase::cancelling)
                co_await coro::sleep(std::chrono::milliseconds(5));

            if (!st->registry.empty()) {
                st->phase_value.store(phase::cancelling);
                st->registry.request_cancel_all();
                const auto cancel_deadline =
                    std::chrono::steady_clock::now() + std::chrono::milliseconds(st->cancel_grace_ms.load());
                while (!st->registry.empty() && std::chrono::steady_clock::now() < cancel_deadline)
                    co_await coro::sleep(std::chrono::milliseconds(5));
            }

            last_report_.drained = st->registry.empty();
            last_report_.unfinished = st->registry.size();
            st->phase_value.store(phase::stopped);
            socket_.close(); // 幂等: 打断挂起的 recvfrom, 让 recv_loop 退出
            co_return;
        }

        Config config_;
        net::UdpSocket socket_;
        handler_t handler_;
        std::shared_ptr<state> state_ = std::make_shared<state>();
        detail::shutdown_report last_report_{};
        // 接收缓冲必须先于任务声明: 逆序析构时任务先析构, 挂起的 recvfrom 会在缓冲
        // 仍存活时被取消并标记帧废弃。
        std::vector<char> recv_buf_ = std::vector<char>(config_.max_datagram_size, '\0');
        Task<void> recv_task_;
        Task<void> supervisor_task_;
    };

} // namespace coro
