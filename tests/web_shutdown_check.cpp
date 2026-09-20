// web_shutdown_check.cpp — 验证常驻模式接线: serve + signal::handle + stop + wait_all
#include <coro/coro.hpp>
#include <coro/net.hpp>
#include <coro/signal.hpp>
#include <web_server.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <iostream>
#include <thread>

coro::Task<> shutdown_task(web_server* s) {
    std::cout << "[check] shutdown signal received\n";
    s->stop();
    co_return;
}

coro::Task<int> read_one(coro::net::TcpStream* stream, char* byte) {
    co_return co_await stream->read(byte, 1);
}

coro::Task<bool> peer_closed(coro::net::TcpStream* stream) {
    char byte = 0;
    try {
        co_return co_await coro::wait_for(read_one(stream, &byte), std::chrono::seconds(1)) <= 0;
    } catch (const coro::TimeoutError&) {
        co_return false;
    }
}

int main() {
    constexpr unsigned short kPort = 18082;
    web_server server;
    server.set_verbose(false);
    server.set_idle_timeout(std::chrono::milliseconds(150));
    server.set_request_timeout(std::chrono::seconds(1));
    if (!server.listen("127.0.0.1", kPort))
        return 1;

    std::atomic<bool> exited{false};
    // 先留时间跑完空闲/慢请求检查，2s 后模拟 Ctrl+C。
    std::thread t([&] {
        std::this_thread::sleep_for(std::chrono::seconds(2));
        std::raise(SIGINT);
    });

    coro::run([](web_server& s, std::atomic<bool>& ok) -> coro::Task<> {
        auto srv = coro::spawn(s.serve());
        auto h1 = coro::signal::handle(SIGINT, [&s] { return shutdown_task(&s); });
#ifdef _WIN32
        auto h2 = coro::signal::handle(SIGBREAK, [&s] { return shutdown_task(&s); });
#endif
        co_await coro::sleep(std::chrono::milliseconds(50));
        auto idle_timeout = co_await coro::net::TcpStream::connect("127.0.0.1", kPort);
        co_await coro::sleep(std::chrono::milliseconds(350));
        const bool idle_closed = idle_timeout.valid() && co_await peer_closed(&idle_timeout);

        // 每个请求的总时限不会被零星字节重置。
        s.set_idle_timeout(std::chrono::seconds(1));
        s.set_request_timeout(std::chrono::milliseconds(150));
        auto slow_request = co_await coro::net::TcpStream::connect("127.0.0.1", kPort);
        const char prefix = 'G';
        const int written = slow_request.valid() ? co_await slow_request.write(&prefix, 1) : -1;
        co_await coro::sleep(std::chrono::milliseconds(350));
        const bool request_closed = written == 1 && slow_request.valid() && co_await peer_closed(&slow_request);

        // 恢复长超时后建立空闲连接：stop() 必须主动唤醒 worker 上
        // 挂起的 read，否则下面的 wait_all() 会永久等待。
        s.set_idle_timeout(std::chrono::seconds(10));
        s.set_request_timeout(std::chrono::seconds(10));
        auto idle = co_await coro::net::TcpStream::connect("127.0.0.1", kPort);
        co_await std::move(srv);
        s.wait_all();
        ok = idle_closed && request_closed && idle.valid();
    }(server, exited));

    t.join();
    if (!exited) {
        std::cout << "[check] FAILED: server did not exit\n";
        return 1;
    }
    std::cout << "[check] OK: timeouts and graceful shutdown work\n";
    return 0;
}
