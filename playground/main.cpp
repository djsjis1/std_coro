// ============================================================================
// main.cpp — 高并发多线程 TCP 回显服务器 (单端口, SO_REUSEPORT)
// ============================================================================
// 协议: [4字节长度(网络序)] + [N字节数据]
//   服务器按消息边界读取完整消息后原样回显, 避免 TCP 流合并导致的数据错乱
// ============================================================================
#include <coro/coro.hpp>
#include <coro/sleep.hpp>
#include <tcp_udp/tcp_server.hpp>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#ifdef _WIN32
#include <winsock2.h>
#else
#include <arpa/inet.h>
#endif

using namespace std::chrono_literals;

static std::atomic<bool> g_running{true};

static void on_signal(int) {
    g_running.store(false, std::memory_order_release);
}

struct WorkerStats {
    size_t handled = 0;
    size_t rejected = 0;
};

// 精确读取 n 字节, 返回实际读到的字节数 (0=对端关闭)
static coro::Task<int> read_exact(coro::net::TcpStream& conn, char* buf, int n) {
    int total = 0;
    while (total < n) {
        int r = co_await conn.read(buf + total, n - total);
        if (r <= 0)
            co_return total;
        total += r;
    }
    co_return total;
}

static WorkerStats worker_func(int id, unsigned short port) {
    auto& loop = coro::EventLoop::get();
    auto server = std::make_unique<coro::TcpServer>();

    coro::TcpServer::Config cfg;
    cfg.bind_addr = "0.0.0.0";
    cfg.port = port;
    cfg.max_concurrent_handlers = 10000;
    cfg.shutdown_grace = 2s;
    cfg.cancel_grace = 1s;
    server->set_config(cfg);

    static std::atomic<int> conn_count{0};
    server->set_handler([id](coro::net::TcpStream conn) -> coro::Task<> {
        int n = conn_count.fetch_add(1, std::memory_order_relaxed);
        if (n < 10)
            std::printf("[worker-%d] connection #%d accepted\n", id, n);

        // 协议: [4字节 uint32 网络序长度] + [数据]
        char len_buf[4];
        char data_buf[65536];

        while (true) {
            // 1. 读 4 字节长度头
            int r = co_await read_exact(conn, len_buf, 4);
            if (r < 4)
                break;

            uint32_t msg_len = ntohl(*(uint32_t*)len_buf);
            if (msg_len == 0 || msg_len > sizeof(data_buf))
                break;

            // 2. 读完整消息体
            r = co_await read_exact(conn, data_buf, msg_len);
            if (r < (int)msg_len)
                break;

            // 3. 回显: 先发长度头, 再发数据体
            uint32_t net_len = htonl(msg_len);
            int w1 = co_await conn.write((const char*)&net_len, 4);
            if (w1 <= 0)
                break;

            int written = 0;
            while (written < (int)msg_len) {
                int nw = co_await conn.write(data_buf + written, msg_len - written);
                if (nw <= 0)
                    break;
                written += nw;
            }
            if (written < (int)msg_len)
                break;
        }
        co_return;
    });

    server->set_error_handler(
        [id](const std::string& msg) { std::fprintf(stderr, "[worker-%d] error: %s\n", id, msg.c_str()); });

    auto* srv = server.get();

    if (!srv->start()) {
        std::fprintf(stderr, "[worker-%d] start() failed on port %u\n", id, port);
        return {};
    }

    std::printf("[worker-%d] ready on 0.0.0.0:%u\n", id, srv->port());

    auto watchdog = [id, &loop, srv]() -> coro::Task<> {
        while (g_running.load(std::memory_order_acquire))
            co_await coro::sleep(std::chrono::milliseconds(100));
        auto report = co_await srv->shutdown();
        std::printf("[worker-%d] shutdown: drained=%s, unfinished=%zu, total_conn=%zu\n", id,
                    report.drained ? "yes" : "no", report.unfinished, srv->total_connections());
        loop.stop();
        co_return;
    };

    auto wt = watchdog();
    wt.start();
    loop.run();

    WorkerStats s;
    s.handled = srv->total_connections();
    s.rejected = srv->rejected_connections();
    server.reset();
    return s;
}

int main(int argc, char* argv[]) {
#ifdef _WIN32
    SetConsoleOutputCP(65001);
    SetConsoleCP(65001);
#endif

    unsigned short port = 9000;
    size_t num_workers = std::thread::hardware_concurrency();
    if (num_workers == 0)
        num_workers = 4;

    if (argc > 1)
        port = static_cast<unsigned short>(std::atoi(argv[1]));
    if (argc > 2)
        num_workers = static_cast<size_t>(std::atoi(argv[2]));

    std::printf("==========================================================\n");
    std::printf("  高并发多线程 TCP 回显服务器 (长度前缀协议)\n");
    std::printf("==========================================================\n");
    std::printf("  端口:       %u (所有 worker 共享, SO_REUSEPORT)\n", port);
    std::printf("  Worker 数:  %zu\n", num_workers);
    std::printf("  并发上限:   10000 connections / worker\n");
    std::printf("  协议:       [4B length][payload]\n");
    std::printf("==========================================================\n");

    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);

    std::vector<std::thread> threads;
    std::vector<WorkerStats> stats(num_workers);
    threads.reserve(num_workers);

    auto t0 = std::chrono::steady_clock::now();

    for (size_t i = 0; i < num_workers; ++i) {
        threads.emplace_back([&stats, i, port] { stats[i] = worker_func(static_cast<int>(i), port); });
    }

    std::this_thread::sleep_for(1s);

    std::printf("==========================================================\n");
    std::printf("  %zu workers 就绪, 端口 %u, Ctrl+C 退出\n", num_workers, port);
    std::printf("==========================================================\n");

    while (g_running.load(std::memory_order_acquire))
        std::this_thread::sleep_for(200ms);

    std::printf("\n[main] 收到退出信号, 等待 worker 优雅关闭...\n");

    for (auto& t : threads)
        t.join();

    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();

    size_t total_handled = 0, total_rejected = 0;
    std::printf("\n==========================================================\n");
    std::printf("  Worker 统计\n");
    std::printf("==========================================================\n");
    for (size_t i = 0; i < num_workers; ++i) {
        std::printf("  worker-%zu  connections=%-8zu  rejected=%zu\n", i, stats[i].handled, stats[i].rejected);
        total_handled += stats[i].handled;
        total_rejected += stats[i].rejected;
    }
    std::printf("----------------------------------------------------------\n");
    std::printf("  合计      connections=%-8zu  rejected=%zu\n", total_handled, total_rejected);
    std::printf("  运行时间: %lld ms\n", (long long)elapsed);
    std::printf("==========================================================\n");

    return 0;
}
