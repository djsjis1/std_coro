#pragma once

#include <coro/coro.hpp>
#include <coro/net.hpp>

#include <atomic>
#include <functional>
#include <iostream>

// ============================================================================
// coro::TcpServer — 高级 TCP 服务端框架
// ============================================================================
//
// 基于协程的 TCP 服务端，每个连接由独立的协程处理。
// 类似 Python asyncio 的 StreamWriter/StreamReader 模式。
//
// 用法:
//   coro::TcpServer server;
//   server.set_handler([](coro::net::TcpStream conn) -> coro::Task<> {
//       char buf[1024];
//       int n = co_await conn.read(buf, sizeof(buf));
//       co_await conn.write(buf, n);
//   });
//   server.run_forever("0.0.0.0", 8080);
//
// 特性:
//   - 每个连接独立协程处理 (并发)
//   - 支持优雅关闭 (stop())
//   - 连接计数跟踪
//   - 错误回调支持
//
// ============================================================================

namespace coro {

    class TcpServer {
      public:
        using handler_t = std::function<Task<>(net::TcpStream)>;
        using error_handler_t = std::function<void(const std::string&)>;

        struct Config {
            std::string bind_addr = "0.0.0.0";
            unsigned short port = 0;
            int backlog = SOMAXCONN;
        };

        /// 设置连接处理协程 (每个新连接调用一次)
        void set_handler(handler_t handler) {
            handler_ = std::move(handler);
        }

        /// 设置错误回调 (可选)
        void set_error_handler(error_handler_t handler) {
            error_handler_ = std::move(handler);
        }

        /// 启动服务端 (非阻塞，立即返回)
        /// 内部创建 accept 循环协程并 detach
        bool start(const std::string& addr, unsigned short port) {
            config_.bind_addr = addr;
            config_.port = port;
            return start();
        }

        bool start() {
            if (running_.load())
                return false;

            if (!listener_.bind_listen(config_.bind_addr.c_str(), config_.port)) {
                if (error_handler_)
                    error_handler_("bind_listen failed");
                return false;
            }

            running_.store(true);
            accept_task_ = spawn(accept_loop());
            return true;
        }

        /// 阻塞运行直到 stop() 被调用
        void run_forever(const std::string& addr, unsigned short port) {
            if (!start(addr, port))
                return;
            EventLoop::get().run_until_stopped();
        }

        /// 停止服务端 (优雅关闭)
        void stop() {
            if (!running_.exchange(false))
                return;
            listener_.close(); // 中断 accept
            EventLoop::get().stop();
        }

        /// 获取绑定的端口 (start 后可用)
        unsigned short port() const { return config_.port; }

        /// 获取当前活跃连接数
        size_t connection_count() const { return connection_count_.load(); }

      private:
        Task<> accept_loop() {
            while (running_.load()) {
                auto conn = co_await listener_.accept();
                if (!conn.valid()) {
                    if (running_.load()) {
                        if (error_handler_)
                            error_handler_("accept failed");
                    }
                    break;
                }

                ++connection_count_;
                spawn(connection_handler(std::move(conn)));
            }
        }

        Task<> connection_handler(net::TcpStream conn) {
            if (handler_) {
                try {
                    co_await handler_(std::move(conn));
                } catch (const CancelledError&) {
                    // 协程被取消，正常退出
                } catch (const std::exception& e) {
                    if (error_handler_)
                        error_handler_(std::string("handler exception: ") + e.what());
                }
            }
            --connection_count_;
        }

        Config config_;
        net::TcpListener listener_;
        handler_t handler_;
        error_handler_t error_handler_;
        std::atomic<bool> running_{false};
        std::atomic<size_t> connection_count_{0};
        Task<void> accept_task_;
    };

} // namespace coro
