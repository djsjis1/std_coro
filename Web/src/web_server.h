#pragma once

#include <coro/coro.hpp>
#include <coro/net.hpp>

#include <atomic>
#include <chrono>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_set>

#include "http_types.h"
#include "router.h"

// ============================================================================
// web_server.h — 基于 coro::net + llhttp 的多线程 HTTP 服务器
// ============================================================================
//
// 架构(对标 asyncio + llhttp, 多核并行):
//
//   事件循环线程(主)        worker 线程 × N (Scheduler, 每线程一个 EventLoop)
//   ┌──────────────────┐     ┌────────────────────────────────────────────┐
//   │ serve():         │     │ handle_connection() 协程(连接处理)          │
//   │  accept 循环      │────▶│   读 → llhttp 解析 → 路由分发 → 响应       │
//   │  spawn_any 分发   │工厂 │   连接分散在各 worker 上, 并行处理         │
//   └──────────────────┘     └────────────────────────────────────────────┘
//
// 连接迁移的关键:
//   - 协程帧必须在 worker 线程创建/销毁 → 用工厂模式 (Scheduler::spawn_any,
//     工厂在 worker 线程内被调用)
//   - Windows 上 socket 与创建它的线程的 IOCP 关联 → 工厂内先 reattach()
//     把 socket 转移到 worker 的 IOCP, 否则 I/O 完成包投错端口, 协程挂死
//   - Linux (io_uring) 无关联概念, reattach() 为空操作
//
// 并发模型: 每连接一个协程, 由 Scheduler 按「活跃协程数 + 累计分发数」
// 两级负载均衡分发; 连接协程挂起(读/写/sleep)不占 CPU。
// ============================================================================

class web_server {
  public:
    /// workers: 连接处理线程数; 0 = 硬件并发数
    explicit web_server(size_t workers = 0);

    /// 绑定并监听; 失败返回 false
    bool listen(const char* ip, unsigned short port);

    /// 路由表(注册 handler / 静态目录)
    router& routes() { return router_; }

    /// 消息体上限(默认 8MB), 超出时 llhttp 中止解析 → 400
    void set_max_body(size_t bytes) { max_body_ = bytes; }

    /// 访问日志开关(默认开; 压测等高频场景可关闭, 错误日志始终打印)
    void set_verbose(bool v) { verbose_ = v; }

    /// 连续两次读取之间的最长空闲时间(默认 30s; <=0 关闭)
    void set_idle_timeout(std::chrono::milliseconds timeout) { idle_timeout_ms_.store(timeout.count()); }

    /// 从首字节开始接收完整请求的总时限(默认 30s; <=0 关闭)
    void set_request_timeout(std::chrono::milliseconds timeout) { request_timeout_ms_.store(timeout.count()); }

    /// 写完一份响应的总时限(默认 30s; <=0 关闭)
    void set_write_timeout(std::chrono::milliseconds timeout) { write_timeout_ms_.store(timeout.count()); }

    /// accept 循环(常驻, 直到 stop())。在事件循环线程运行。
    coro::Task<> serve();

    /// 请求停止(线程安全): 关闭监听和全部活动连接，唤醒 accept/read/write。
    void stop();

    /// 阻塞等待所有连接协程完成(析构前调用, 防止挂起帧泄漏)
    void wait_all();

    size_t worker_count() const { return scheduler_.worker_count(); }

  private:
    /// 单连接处理: 增量解析请求并逐个响应, 直到连接关闭或 keep-alive 结束。
    /// 注意: 在 worker 线程上运行 (Scheduler 分发)。
    coro::Task<> handle_connection(std::shared_ptr<coro::net::TcpStream> conn);
    coro::Task<> process_connection(coro::net::TcpStream& conn);

    bool register_connection(const std::shared_ptr<coro::net::TcpStream>& conn);
    void unregister_connection(const std::shared_ptr<coro::net::TcpStream>& conn);

    /// 路由分发 + 异常兑底(handler 抛异常 → 500)。
    /// req 为非 const 引用: 动态路由捕获的参数由 router 写入 req.params
    coro::Task<http_response> dispatch(http_request& req);

    coro::Scheduler scheduler_; // worker 池 (构造即启动, 析构自动 join)
    coro::net::TcpListener listener_;
    router router_;
    std::atomic<bool> running_ = false;
    std::mutex connections_mutex_;
    std::unordered_set<std::shared_ptr<coro::net::TcpStream>> connections_;
    std::atomic<long long> idle_timeout_ms_{30000};
    std::atomic<long long> request_timeout_ms_{30000};
    std::atomic<long long> write_timeout_ms_{30000};
    size_t max_body_ = 8 * 1024 * 1024;
    bool verbose_ = true;
};
