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

int main() {
    web_server server;
    if (!server.listen("127.0.0.1", 18080))
        return 1;

    std::atomic<bool> exited{false};
    // 1.5s 后模拟 Ctrl+C
    std::thread t([&] {
        std::this_thread::sleep_for(std::chrono::milliseconds(1500));
        std::raise(SIGINT);
    });

    coro::run([](web_server& s, std::atomic<bool>& ok) -> coro::Task<> {
        auto srv = coro::spawn(s.serve());
        auto h1 = coro::signal::handle(SIGINT, [&s] { return shutdown_task(&s); });
        auto h2 = coro::signal::handle(SIGBREAK, [&s] { return shutdown_task(&s); });
        co_await std::move(srv);
        s.wait_all();
        ok = true;
    }(server, exited));

    t.join();
    if (!exited) {
        std::cout << "[check] FAILED: server did not exit\n";
        return 1;
    }
    std::cout << "[check] OK: graceful shutdown works\n";
    return 0;
}
