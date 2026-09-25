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
// 调用流程:
//   main()
//     ├─ server.set_handler(my_handler)    // 注册: 新连接后干什么
//     ├─ server.set_error_handler(on_err)  // 注册: 出错后干什么
//     ├─ server.start("0.0.0.0", 8080)     // 绑定端口 + spawn accept_loop
//     │     └─ accept_loop (后台持续运行)
//     │           ├─ accept() 连接A到达 → spawn(connection_handler(A))
//     │           ├─ accept() 连接B到达 → spawn(connection_handler(B))
//     │           └─ ... 循环
//     └─ EventLoop::run_until_stopped()    // 阻塞，让事件循环跑起来
//           └─ server.stop()               // 某处调用 → 关闭 listener → 停止循环
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
//   - 连接计数跟踪 (原子, 线程安全)
//   - 错误回调支持
//
// ============================================================================

namespace coro {

    class TcpServer {
      public:
        // 连接处理协程的类型
        //   参数: TcpStream 封装了已建立的 TCP 连接 (可读/写/关闭)
        //   返回: Task<> 协程，支持 co_await 异步读写
        using handler_t = std::function<Task<>(net::TcpStream)>;
        // 错误回调类型: 参数是错误描述字符串
        using error_handler_t = std::function<void(const std::string&)>;

        // 服务端配置
        struct Config {
            std::string bind_addr = "0.0.0.0"; // 绑定地址
            unsigned short port = 0;           // 绑定端口（0 = 系统分配）
            int backlog = SOMAXCONN;           // listen 队列长度 (内核上限)
        };

        /// 注册连接处理协程 — 每个新连接到达时调用一次 handler
        void set_handler(handler_t handler) { handler_ = std::move(handler); }

        /// 注册错误回调 — bind 失败 / accept 出错 / handler 抛异常时触发
        void set_error_handler(error_handler_t handler) { error_handler_ = std::move(handler); }

        /// 启动服务端（非阻塞）— 设置地址端口后绑定 + spawn 接收协程
        bool start(const std::string& addr, unsigned short port) {
            config_.bind_addr = addr;
            config_.port = port;
            return start();
        }

        /// 启动服务端（用已设置的 Config）
        /// 流程: 检查运行状态 → bind+listen → spawn accept_loop 协程 → 立即返回
        bool start() {
            if (running_.load())
                return false;

            if (!listener_.bind_listen(config_.bind_addr.c_str(), config_.port)) {
                if (error_handler_)
                    error_handler_("bind_listen failed");
                return false;
            }

            running_.store(true);
            accept_task_ = spawn(accept_loop()); // 启动后台 accept 循环
            return true;
        }

        /// 阻塞运行 — 先 start()，然后阻塞在事件循环上，直到 stop() 被调用
        void run_forever(const std::string& addr, unsigned short port) {
            if (!start(addr, port))
                return;
            EventLoop::get().run_until_stopped();
        }

        /// 停止服务端
        /// 流程: 原子置 running_=false → 关闭 listener（中断挂起的 accept）→ 停止事件循环
        void stop() {
            if (!running_.exchange(false))
                return;
            listener_.close(); // 关闭 listener 使挂起的 accept 立即返回错误
            EventLoop::get().stop();
        }

        /// 获取绑定的端口号
        unsigned short port() const { return config_.port; }

        /// 获取当前活跃连接数（原子计数器）
        size_t connection_count() const { return connection_count_.load(); }

      private:
        /// accept 主循环协程 — 持续 accept，每有新连接就 spawn 独立处理协程
        Task<> accept_loop() {
            while (running_.load()) {
                auto conn = co_await listener_.accept();
                if (!conn.valid()) {
                    // accept 出错（通常是 stop() 关闭 listener 导致的）
                    if (running_.load()) {
                        if (error_handler_)
                            error_handler_("accept failed");
                    }
                    break; // 退出 accept 循环
                }

                ++connection_count_;
                // 为每个连接 spawn 独立处理协程。
                // spawn 返回的 Task 必须显式 detach: 临时对象析构会把已启动
                // 未完成的帧标记废弃, 连接处理协程将永远不执行。
                spawn(connection_handler(std::move(conn))).detach();
            }
        }

        /// 单个连接的处理协程 — 包装用户 handler，捕获异常防止崩溃
        Task<> connection_handler(net::TcpStream conn) {
            if (handler_) {
                try {
                    co_await handler_(std::move(conn)); // 调用用户注册的连接处理逻辑
                } catch (const CancelledError&) {
                    // 协程被取消（比如 stop 时），正常退出不报错
                } catch (const std::exception& e) {
                    // handler 抛了其他异常，交给错误回调处理
                    if (error_handler_)
                        error_handler_(std::string("handler exception: ") + e.what());
                }
            }
            --connection_count_; // 连接结束，计数 -1
        }

        Config config_;                         // 配置（绑定地址、端口）
        net::TcpListener listener_;             // 底层 TCP 监听器
        handler_t handler_;                     // 用户注册的连接处理协程
        error_handler_t error_handler_;         // 用户注册的错误回调
        std::atomic<bool> running_{false};      // 运行标志（原子，线程安全）
        std::atomic<size_t> connection_count_{0}; // 当前活跃连接计数
        Task<void> accept_task_;                // accept 主循环的协程句柄
    };

} // namespace coro
