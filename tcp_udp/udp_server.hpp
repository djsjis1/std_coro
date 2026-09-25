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
// 调用流程:
//   main()
//     ├─ server.set_handler(my_handler)    // 注册: 收到数据后干什么
//     ├─ server.set_error_handler(on_err)  // 注册: 出错后干什么
//     ├─ server.start("0.0.0.0", 8801)     // 绑定端口 + spawn recv_loop
//     │     └─ recv_loop (后台持续运行)
//     │           ├─ recvfrom() 收到包A → spawn(datagram_handler(A))
//     │           ├─ recvfrom() 收到包B → spawn(datagram_handler(B))
//     │           └─ ... 循环
//     └─ EventLoop::run_until_stopped()    // 阻塞，让事件循环跑起来
//           └─ server.stop()               // 某处调用 → 关闭 socket → 停止循环
//
// 特性:
//   - 每个数据报独立协程处理 (并发)
//   - 每个数据报拷贝独立缓冲，异步 handler 安全
//   - 支持优雅关闭 (stop())
//   - 数据报计数跟踪
//   - 错误回调支持
//
// ============================================================================

namespace coro {

    class UdpServer {
      public:
        // 数据报处理协程的类型
        //   参数: (数据内容, 数据长度, 发送方地址)
        //   返回: Task<> 协程，支持 co_await 异步操作（如 sendto 回包）
        using handler_t = std::function<Task<>(const char*, size_t, const sockaddr_in&)>;
        // 错误回调类型: 参数是错误描述字符串
        using error_handler_t = std::function<void(const std::string&)>;

        // 服务端配置
        struct Config {
            std::string bind_addr = "0.0.0.0"; // 绑定地址
            unsigned short port = 0;           // 绑定端口（0 = 系统分配）
            size_t max_datagram_size = 65535;  // 最大数据报大小 (UDP 上限)
        };

        /// 注册数据报处理协程 — 每收到一个 UDP 数据报就调用一次 handler
        void set_handler(handler_t handler) { handler_ = std::move(handler); }

        /// 注册错误回调 — bind 失败 / recvfrom 出错 / handler 抛异常时触发
        void set_error_handler(error_handler_t handler) { error_handler_ = std::move(handler); }

        /// 启动服务端（非阻塞）— 设置地址端口后绑定 + spawn 接收协程
        bool start(const std::string& addr, unsigned short port) {
            config_.bind_addr = addr;
            config_.port = port;
            return start();
        }

        /// 启动服务端（用已设置的 Config）
        /// 流程: 检查运行状态 → bind socket → spawn recv_loop 协程 → 立即返回
        bool start() {
            if (running_.load())
                return false;

            if (!socket_.bind_listen(config_.bind_addr.c_str(), config_.port)) {
                if (error_handler_)
                    error_handler_("UDP bind failed");
                return false;
            }

            running_.store(true);
            recv_task_ = spawn(recv_loop()); // 启动后台接收协程
            return true;
        }

        /// 阻塞运行 — 先 start()，然后阻塞在事件循环上，直到 stop() 被调用
        void run_forever(const std::string& addr, unsigned short port) {
            if (!start(addr, port))
                return;
            EventLoop::get().run_until_stopped();
        }

        /// 停止服务端
        /// 流程: 原子置 running_=false → 关闭 socket（中断挂起的 recvfrom）→ 停止事件循环
        void stop() {
            if (!running_.exchange(false))
                return;
            socket_.close(); // 关闭 socket 使挂起的 recvfrom 立即返回错误
            EventLoop::get().stop();
        }

        /// 获取绑定的端口号
        unsigned short port() const { return config_.port; }

        /// 获取已处理的数据报总数（原子计数器）
        size_t datagram_count() const { return datagram_count_.load(); }

        /// 获取底层 UdpSocket — 用于在 handler 中调用 sendto 回复数据
        net::UdpSocket& socket() { return socket_; }

      private:
        /// 接收主循环协程 — 持续 recvfrom，每收到一个数据报就拷贝数据并 spawn 处理协程
        Task<> recv_loop() {
            while (running_.load()) {
                sockaddr_in sender{}; // 内核自动填入发送方的 IP 和端口
                int n = co_await socket_.recvfrom(recv_buf_.data(), recv_buf_.size(), &sender);
                if (n < 0) {
                    // 接收出错（通常是 stop() 关闭 socket 导致的）
                    if (running_.load()) {
                        if (error_handler_)
                            error_handler_("recvfrom failed");
                    }
                    break; // 退出接收循环
                }
                if (n == 0)
                    continue; // 空数据报，跳过不处理

                ++datagram_count_; // 计数 +1
                if (handler_) {
                    // 为每个数据报拷贝独立缓冲，异步 handler 期间不会被下一个包覆盖。
                    // 按值传递 vector: 一次分配；同时拷贝 handler，
                    // 避免服务器先于滞留的处理协程析构时访问成员。
                    // spawn 返回的 Task 必须显式 detach: 临时对象析构会把
                    // 已启动未完成的帧标记废弃, 处理协程将永远不执行。
                    spawn(
                        datagram_handler(std::vector<char>(recv_buf_.begin(), recv_buf_.begin() + n), handler_, sender))
                        .detach();
                }
            }
        }

        /// 单个数据报的处理协程 — 包装用户 handler，捕获异常防止崩溃
        Task<> datagram_handler(std::vector<char> data, handler_t handler, sockaddr_in sender) {
            try {
                co_await handler(data.data(), data.size(), sender); // 调用用户注册的处理逻辑
            } catch (const CancelledError&) {
                // 协程被取消（比如 stop 时），正常退出不报错
            } catch (const std::exception& e) {
                // handler 抛了其他异常，交给错误回调处理
                if (error_handler_)
                    error_handler_(std::string("handler exception: ") + e.what());
            }
        }

        Config config_;                         // 配置（绑定地址、端口）
        net::UdpSocket socket_;                 // 底层 UDP socket
        handler_t handler_;                     // 用户注册的数据报处理协程
        error_handler_t error_handler_;         // 用户注册的错误回调
        std::atomic<bool> running_{false};      // 运行标志（原子，线程安全）
        std::atomic<size_t> datagram_count_{0}; // 已处理数据报计数
        // 接收缓冲先于 recv_task_ 声明: 逆序析构时先析构任务，
        // Task 析构会在缓冲仍存活时取消挂起的 recvfrom 并标记帧废弃。
        std::vector<char> recv_buf_ = std::vector<char>(static_cast<size_t>(65535)); // 临时接收缓冲区（64KB）
        Task<void> recv_task_;                                                       // 接收主循环的协程句柄
    };

} // namespace coro
