// dns_demo.cpp — coro::net::resolve / connect 示例 (计划 M4b)
//
// 演示三件事:
//   1. 解析主机名与服务名为可连接端点 (解析在线程池里跑, 不占事件循环线程);
//   2. 用解析结果直接建立连接 (endpoint 适配回 net.hpp 的 ip+port 接口);
//   3. 解析失败是可区分的异常 (带 getaddrinfo 返回码), 而不是"返回空列表"这种含糊结果。
#include <coro/coro.hpp>
#include <coro/dns.hpp>
#include <coro/net.hpp>

#include <iostream>
#include <string>

coro::Task<> accept_one(coro::net::TcpListener* listener) {
    auto conn = co_await listener->accept();
    (void)conn; // 示例只关心连接是否建立成功
    co_return;
}

int main() {
    return coro::run([]() -> coro::Task<int> {
        // --- 1: 解析主机名 + 真实监听端口 ---
        coro::net::TcpListener listener;
        int port = 0;
        for (unsigned short candidate = 19820; candidate < 19840; ++candidate) {
            if (listener.bind_listen("127.0.0.1", candidate)) {
                port = listener.local_port();
                break;
            }
        }
        if (port == 0) {
            std::cerr << "bind failed" << std::endl;
            co_return 1;
        }
        auto acceptor = coro::spawn(accept_one(&listener));

        auto eps = co_await coro::net::resolve("localhost", std::to_string(port));
        std::cout << "resolved " << eps.size() << " endpoint(s)" << std::endl;
        for (const auto& ep : eps)
            std::cout << "  " << ep.address << ':' << ep.port << std::endl;
        if (eps.empty())
            co_return 2;
        if (eps[0].port != static_cast<uint16_t>(port))
            co_return 3; // 端口必须是主机字节序, 否则后续 connect 会连错

        // --- 2: 按解析结果建立连接 ---
        auto client = co_await coro::net::connect(eps[0]);
        std::cout << "connect via endpoint: " << (client.valid() ? "ok" : "failed") << std::endl;
        co_await std::move(acceptor);
        listener.close();
        if (!client.valid())
            co_return 4;

        // --- 3: 失败必须可区分 ---
        bool reported = false;
        try {
            // 非法服务名: 由 getaddrinfo 直接判错, 不依赖外网 (用未知主机会被本机的
            // DNS 拦截器解析成任意地址, 结果不确定)
            (void)co_await coro::net::resolve("127.0.0.1", "not-a-service-name");
        } catch (const coro::detail::DnsResolutionError& e) {
            std::cout << "resolution error: " << e.what() << " (code=" << e.code << ')' << std::endl;
            reported = true;
        }
        if (!reported)
            co_return 5;

        std::cout << "ALL DNS CASES PASSED" << std::endl;
        co_return 0;
    }());
}
