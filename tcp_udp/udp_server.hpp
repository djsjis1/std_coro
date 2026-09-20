#pragma once

#include <coro/coro.hpp>
#include <coro/net.hpp>

#include <atomic>
#include <functional>
#include <iostream>
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
// 基于协程的 UDP 服务端，每个数据报由独立的协程处理。
//
// 用法:
//   coro::UdpServer server;
//   server.set_handler([](const char* data, size_t len,
//                         const sockaddr_in& sender) -> coro::Task<> {
//       // 处理数据报
//   });
//   server.run_forever("0.0.0.0", 9000);
//
// 特性:
//   - 每个数据报独立协程处理 (并发)
//   - 支持优雅关闭 (stop())
//   - 数据报计数跟踪
//   - 错误回调支持
//
// ============================================================================

namespace coro {

    class UdpServer {
      public:
        using handler_t = std::function<Task<>(const char*, size_t, const sockaddr_in&)>;
        using error_handler_t = std::function<void(const std::string&)>;

        struct Config {
            std::string bind_addr = "0.0.0.0";
            unsigned short port = 0;
            size_t max_datagram_size = 65535;
        };

        /// 设置数据报处理协程
        void set_handler(handler_t handler) {
            handler_ = std::move(handler);
        }

        /// 设置错误回调 (可选)
        void set_error_handler(error_handler_t handler) {
            error_handler_ = std::move(handler);
        }

        /// 启动服务端 (非阻塞，立即返回)
        bool start(const std::string& addr, unsigned short port) {
            config_.bind_addr = addr;
            config_.port = port;
            return start();
        }

        bool start() {
            if (running_.load())
                return false;

            if (!socket_.bind_listen(config_.bind_addr.c_str(), config_.port)) {
                if (error_handler_)
                    error_handler_("UDP bind failed");
                return false;
            }

            running_.store(true);
            recv_task_ = spawn(recv_loop());
            return true;
        }

        /// 阻塞运行直到 stop() 被调用
        void run_forever(const std::string& addr, unsigned short port) {
            if (!start(addr, port))
                return;
            EventLoop::get().run_until_stopped();
        }

        /// 停止服务端
        void stop() {
            if (!running_.exchange(false))
                return;
            socket_.close(); // 中断 recvfrom
            EventLoop::get().stop();
        }

        /// 获取绑定的端口
        unsigned short port() const { return config_.port; }

        /// 获取已处理的数据报数
        size_t datagram_count() const { return datagram_count_.load(); }

        /// 获取底层 socket (用于 sendto 等)
        net::UdpSocket& socket() { return socket_; }

      private:
        Task<> recv_loop() {
            while (running_.load()) {
                sockaddr_in sender{};
                int n = co_await socket_.recvfrom(recv_buf_.data(), recv_buf_.size(), &sender);
                if (n < 0) {
                    if (running_.load()) {
                        if (error_handler_)
                            error_handler_("recvfrom failed");
                    }
                    break;
                }
                if (n == 0)
                    continue; // 空数据报

                ++datagram_count_;
                if (handler_) {
                    spawn(datagram_handler(recv_buf_.data(), (size_t)n, sender));
                }
            }
        }

        Task<> datagram_handler(const char* data, size_t len, sockaddr_in sender) {
            if (handler_) {
                try {
                    co_await handler_(data, len, sender);
                } catch (const CancelledError&) {
                    // 协程被取消，正常退出
                } catch (const std::exception& e) {
                    if (error_handler_)
                        error_handler_(std::string("handler exception: ") + e.what());
                }
            }
        }

        Config config_;
        net::UdpSocket socket_;
        handler_t handler_;
        error_handler_t error_handler_;
        std::atomic<bool> running_{false};
        std::atomic<size_t> datagram_count_{0};
        Task<void> recv_task_;
        std::vector<char> recv_buf_ = std::vector<char>(static_cast<size_t>(65535));
    };

} // namespace coro
