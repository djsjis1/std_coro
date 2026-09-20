// ============================================================================
// tcp_udp/example/main.cpp — TCP/UDP 协程服务端框架演示
// ============================================================================
//
// 演示内容:
//   1. TcpServer — TCP echo 服务端 (每连接一个协程)
//   2. UdpServer — UDP echo 服务端 (每数据报一个协程)
//   3. 内置测试客户端验证两个服务端
//
// 运行: build/Debug/tcp_udp_example.exe
// ============================================================================

#include "tcp_server.hpp"
#include "udp_server.hpp"

#include <coro/coro.hpp>
#include <coro/net.hpp>

#include <cstring>
#include <iostream>
#include <string>

using namespace std::chrono_literals;

// ============================================================================
// TCP echo 处理协程: 读到的数据原样写回
// ============================================================================
coro::Task<> tcp_echo_handler(coro::net::TcpStream conn) {
    char buf[1024];
    while (true) {
        int n = co_await conn.read(buf, sizeof(buf));
        if (n <= 0)
            break; // 对端关闭或出错
        std::cout << "  [tcp] recv " << n << " bytes" << std::endl;
        int sent = co_await conn.write(buf, (size_t)n);
        if (sent < 0)
            break;
    }
    std::cout << "  [tcp] connection closed" << std::endl;
}

// ============================================================================
// UDP echo 处理协程: 收到的数据报原样发回
// ============================================================================
coro::Task<> udp_echo_handler(const char* data, size_t len,
                              const sockaddr_in& sender,
                              coro::net::UdpSocket* sock) {
    std::cout << "  [udp] recv " << len << " bytes from "
              << inet_ntoa(sender.sin_addr) << ":" << ntohs(sender.sin_port)
              << std::endl;
    int sent = co_await sock->sendto(data, len, sender);
    if (sent < 0)
        std::cout << "  [udp] sendto failed" << std::endl;
}

// ============================================================================
// TCP 测试客户端
// ============================================================================
coro::Task<> tcp_test_client() {
    co_await coro::sleep(100ms); // 等服务端就绪

    std::cout << "[tcp-client] connecting..." << std::endl;
    auto conn = co_await coro::net::TcpStream::connect("127.0.0.1", 8800);
    if (!conn.valid()) {
        std::cout << "[tcp-client] connect FAILED" << std::endl;
        co_return;
    }
    std::cout << "[tcp-client] connected!" << std::endl;

    const char* msg = "Hello TCP from coro!";
    co_await conn.write(msg, strlen(msg));

    char buf[1024];
    int n = co_await conn.read(buf, sizeof(buf));
    if (n > 0) {
        std::string echo(buf, (size_t)n);
        std::cout << "[tcp-client] echo: \"" << echo << "\""
                  << (echo == msg ? " OK" : " MISMATCH") << std::endl;
    }
    conn.close();
}

// ============================================================================
// UDP 测试客户端
// ============================================================================
coro::Task<> udp_test_client(coro::net::UdpSocket* sock) {
    co_await coro::sleep(100ms); // 等服务端就绪

    const char* msg = "Hello UDP from coro!";
    sockaddr_in dest{};
    dest.sin_family = AF_INET;
    dest.sin_port = htons(8801);
    dest.sin_addr.s_addr = inet_addr("127.0.0.1");

    co_await sock->sendto(msg, strlen(msg), dest);
    std::cout << "[udp-client] sent datagram" << std::endl;

    char buf[1024];
    sockaddr_in from{};
    int n = co_await sock->recvfrom(buf, sizeof(buf), &from);
    if (n > 0) {
        std::string echo(buf, (size_t)n);
        std::cout << "[udp-client] echo: \"" << echo << "\""
                  << (echo == msg ? " OK" : " MISMATCH") << std::endl;
    }
}

// ============================================================================
// 主协程: 启动 TCP/UDP 服务端 + 测试客户端, 3 秒后关闭
// ============================================================================
coro::Task<> main_task() {
    std::cout << "=== TCP/UDP Server Framework Demo ===" << std::endl;

    // ---- TCP echo 服务端 ----
    coro::TcpServer tcp_server;
    tcp_server.set_handler(tcp_echo_handler);
    tcp_server.set_error_handler([](const std::string& err) {
        std::cout << "[tcp-server] error: " << err << std::endl;
    });
    if (!tcp_server.start("127.0.0.1", 8800)) {
        std::cout << "TCP start FAILED" << std::endl;
        co_return;
    }
    std::cout << "[tcp-server] listening on 127.0.0.1:8800" << std::endl;

    // ---- UDP echo 服务端 ----
    coro::UdpServer udp_server;
    coro::net::UdpSocket* udp_sock_ptr = &udp_server.socket();
    udp_server.set_handler(
        [udp_sock_ptr](const char* data, size_t len,
                       const sockaddr_in& sender) -> coro::Task<> {
            co_await udp_echo_handler(data, len, sender, udp_sock_ptr);
        });
    if (!udp_server.start("127.0.0.1", 8801)) {
        std::cout << "UDP start FAILED" << std::endl;
        co_return;
    }
    std::cout << "[udp-server] listening on 127.0.0.1:8801" << std::endl;

    // ---- 启动测试客户端 ----
    auto tcp_client = coro::spawn(tcp_test_client());
    auto udp_client = coro::spawn(udp_test_client(udp_sock_ptr));

    // 运行 3 秒
    co_await coro::sleep(3s);

    // ---- 停止服务端 ----
    std::cout << "\n--- shutting down ---" << std::endl;
    tcp_server.stop();
    udp_server.stop();

    std::cout << "TCP connections handled: " << tcp_server.connection_count() << std::endl;
    std::cout << "UDP datagrams handled: " << udp_server.datagram_count() << std::endl;
    std::cout << "=== Demo done ===" << std::endl;
}

int main() {
    coro::run(main_task());
    return 0;
}
