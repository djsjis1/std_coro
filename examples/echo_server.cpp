// ============================================================================
// echo_server.cpp — IOCP 网络层自测: TCP echo 服务器 + 内置客户端
// ============================================================================
//
// 零配置: EventLoop 在 Windows 上默认使用 IOCP, 无需任何初始化。
// 就像 Python 的 asyncio.run(main()) 一样直接可用。
//
// 流程:
//   1. 服务器监听 127.0.0.1:8888
//   2. 客户端协程异步 connect → 发送 "hello" → 读取回显
//   3. 服务器 accept → echo handler 把收到的数据原样写回
//   4. 客户端验证回显内容一致
//
// 运行: build/Debug/example_echo.exe
// ============================================================================

#include <coro/coro.hpp>
#include <coro/net.hpp>

#include <iostream>
#include <string>

using namespace std::chrono_literals;

// ---- echo 处理器: 读到的数据原样写回 ----
coro::Task<> echo_handler(coro::net::TcpStream conn) {
    char buf[1024];
    while (true) {
        int n = co_await conn.read(buf, sizeof(buf));
        if (n <= 0)
            break; // 对端关闭或出错
        std::cout << "  [server] received " << n << " bytes" << std::endl;
        co_await conn.write(buf, (size_t)n);
    }
    std::cout << "  [server] connection closed" << std::endl;
}

// ---- 内置客户端: connect → 发送 → 验证回显 ----
coro::Task<> client() {
    std::cout << "  [client] connecting to 127.0.0.1:8888..." << std::endl;
    auto conn = co_await coro::net::TcpStream::connect("127.0.0.1", 8888);
    if (!conn.valid()) {
        std::cout << "  [client] connect FAILED (errno=" << errno << ", "
#ifdef _WIN32
                  << (errno == WSAECONNREFUSED ? "refused" : "other")
#else
                  << (errno == ECONNREFUSED ? "refused" : "other")
#endif
                  << ")" << std::endl;
        co_return;
    }
    std::cout << "  [client] connected!" << std::endl;

    const char* msg = "hello from coro client";
    int sent = co_await conn.write(msg, strlen(msg));
    std::cout << "  [client] sent " << sent << " bytes" << std::endl;

    char buf[1024];
    int n = co_await conn.read(buf, sizeof(buf));
    if (n > 0) {
        std::string echo(buf, (size_t)n);
        std::cout << "  [client] echo received: \"" << echo << "\"" << std::endl;
        std::cout << "  [client] " << (echo == msg ? "MATCH ✓" : "MISMATCH ✗") << std::endl;
    } else {
        std::cout << "  [client] read returned " << n << " (connection issue)" << std::endl;
    }
    conn.close();
    std::cout << "  [client] done" << std::endl;
}

// ---- 主协程 ----
coro::Task<> main_task() {
    std::cout << "=== IOCP Echo Server Test (zero-config) ===" << std::endl;

    // 启动服务器 (零配置: EventLoop 默认就是 IOCP)
    coro::net::TcpListener listener;
    if (!listener.bind_listen("127.0.0.1", 8888)) {
        std::cout << "bind_listen FAILED (端口被占用?)" << std::endl;
        co_return;
    }
    std::cout << "[server] listening on 127.0.0.1:8888" << std::endl;

    // 并发: 客户端连接 + 服务器 accept
    auto client_task = coro::spawn(client());

    std::cout << "[server] waiting for connection..." << std::endl;
    auto conn = co_await listener.accept();
    if (!conn.valid()) {
        std::cout << "[server] accept FAILED" << std::endl;
        co_return;
    }
    std::cout << "[server] accepted!" << std::endl;

    // 处理这个连接 (echo)
    co_await echo_handler(std::move(conn));

    // 等客户端完成
    co_await std::move(client_task);

    std::cout << "=== Echo test done ===" << std::endl;
}

int main() {
    coro::run(main_task());
    return 0;
}
