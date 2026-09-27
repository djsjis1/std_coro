// test_dns.cpp — coro::net::resolve / connect 的解析与适配语义 (计划 M4b)
//
// 刻意只用离线可判定的输入 (localhost / 数字地址 / RFC 6761 保留的 .invalid),
// 避免把 CI 绑到外部 DNS 服务器上。"不阻塞事件循环"这一性质由 to_thread 保证,
// 其本身已有 ThreadTest 覆盖, 这里不重复测线程调度。
#include <gtest/gtest.h>

#include <coro/coro.hpp>
#include <coro/dns.hpp>
#include <coro/net.hpp>

// CORO_HAS_DNS 由 dns.hpp 自己定义, 所以守卫必须放在 include **之后** ——
// 放在前面时宏还不存在, 整个文件会被静默编译掉 (实测 0 tests 匹配)。
#if defined(CORO_HAS_DNS) && CORO_HAS_DNS

#include <string>
#include <thread>
#include <vector>

using namespace std::chrono_literals;

namespace {

    std::vector<coro::net::resolved_endpoint> do_resolve(const std::string& host, const std::string& service) {
        std::vector<coro::net::resolved_endpoint> out;
        coro::run([&]() -> coro::Task<> { out = co_await coro::net::resolve(host, service); }());
        return out;
    }

    coro::Task<> accept_once(coro::net::TcpListener* listener) {
        auto conn = co_await listener->accept();
        (void)conn;
        co_return;
    }

    /// 数字主机 + 真实监听端口: 解析结果直接喂给 connect 应当走得通
    coro::Task<> connect_via_endpoint(int* connected_port) {
        coro::net::TcpListener listener;
        if (!listener.bind_listen("127.0.0.1", 0))
            co_return;
        const int port = listener.local_port();

        auto acceptor = coro::spawn(accept_once(&listener));
        auto eps = co_await coro::net::resolve("127.0.0.1", std::to_string(port));
        if (eps.empty())
            co_return;
        auto client = co_await coro::net::connect(eps[0]);
        if (client.valid())
            *connected_port = port;
        co_await std::move(acceptor);
        listener.close();
        co_return;
    }

} // namespace

// 刻意用数字主机而不是 "localhost": Windows 的解析器会把 localhost 优先给 ::1,
// 被 v4 过滤器滤掉后结果就是空, 那测的是 OS 行为而不是本模块的映射逻辑
// (本仓库既有约定: DNS 测试限定于回环地址与数字端口)。
TEST(DnsTest, NumericHostRoundTripsToSameAddress) {
    auto eps = do_resolve("127.0.0.1", "");
    ASSERT_FALSE(eps.empty()) << "数字回环地址应当直接产出一个端点";
    bool saw_loopback = false;
    for (const auto& ep : eps) {
        if (ep.address == "127.0.0.1")
            saw_loopback = true;
        // v1 明确只返回 IPv4: 出现冒号说明过滤器没生效
        EXPECT_EQ(ep.address.find(':'), std::string::npos) << "不该返回 IPv6 字面量: " << ep.address;
    }
    EXPECT_TRUE(saw_loopback) << "地址文本未经 inet_ntop 正确还原, 实得 " << eps[0].address;
}

TEST(DnsTest, NumericServiceBecomesHostOrderPort) {
    auto eps = do_resolve("127.0.0.1", "8123");
    ASSERT_FALSE(eps.empty());
    bool matched = false;
    for (const auto& ep : eps)
        if (ep.port == 8123) // 端口必须是主机字节序, 否则 connect 会连错端口
            matched = true;
    EXPECT_TRUE(matched);
}

TEST(DnsTest, ConnectUsesResolvedEndpoint) {
    int connected_port = 0;
    coro::run([&]() -> coro::Task<> {
        auto job = coro::spawn(connect_via_endpoint(&connected_port));
        co_await std::move(job);
    }());
    EXPECT_GT(connected_port, 0) << "按解析结果建立连接未走通";
}

TEST(DnsTest, BadServiceThrowsWithGetaddrinfoCode) {
    // 刻意用非法**服务名**触发失败: 它由 getaddrinfo 在服务解析阶段直接判错,
    // 不查 DNS。用"未知主机"做断言在这台机器上是偶发的 —— 本机存在会把任意
    // 域名(含 RFC 6761 保留的 .invalid)解析到 198.18.0.8 的拦截器。
    bool threw = false;
    int code = 0;
    coro::run([&]() -> coro::Task<> {
        try {
            (void)co_await coro::net::resolve("127.0.0.1", "not-a-service-name");
        } catch (const coro::detail::DnsResolutionError& e) {
            threw = true;
            code = e.code;
        }
    }());
    EXPECT_TRUE(threw) << "解析失败必须抛 DnsResolutionError, 而不是静默返回空";
    EXPECT_NE(code, 0) << "错误里要带上 getaddrinfo 的返回码";
}

#endif // CORO_HAS_DNS
