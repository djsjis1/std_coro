// package_smoke — 安装包消费者自检
//
// 默认模式 (CORO_SMOKE_COMPONENT=coro): 跑一个返回 42 的 Task, 证明聚合入口可用。
// http_client 模式: 额外证明安装包里的 coro::http_client 能被独立链接、构造、关闭,
// 并且关闭后的请求被拒绝 —— 不需要网络对端, 也不碰 TLS 以外的平台依赖。
#include <coro/coro.hpp>

#include <cstdio>

#if defined(CORO_SMOKE_HTTP_CLIENT) && CORO_SMOKE_HTTP_CLIENT
#include <coro/http_client.hpp>
#endif

coro::Task<int> answer() {
    co_return 42;
}

#if defined(CORO_SMOKE_HTTP_CLIENT) && CORO_SMOKE_HTTP_CLIENT

// 关闭之后再 await: 必须被拒绝 (HttpError), 而不是去连 127.0.0.1:1。
coro::Task<int> reject_after_close(coro::HttpClient* client) {
    try {
        (void)co_await client->get("http://127.0.0.1:1/");
    } catch (const coro::HttpError&) {
        co_return 0;
    } catch (...) {
        co_return 2;
    }
    co_return 1; // 没抛异常 = 关闭没有生效
}

#endif

int main() {
    if (coro::run(answer()) != 42) {
        std::fprintf(stderr, "[package-smoke] core task returned an unexpected value\n");
        return 1;
    }

#if defined(CORO_SMOKE_HTTP_CLIENT) && CORO_SMOKE_HTTP_CLIENT
    coro::HttpClient client;
    if (client.closed()) {
        std::fprintf(stderr, "[package-smoke] HttpClient reports closed right after construction\n");
        return 1;
    }
#if defined(CORO_SMOKE_EXPECT_TLS)
    const bool tls = coro::HttpClient::supports_tls();
    if (tls != (CORO_SMOKE_EXPECT_TLS != 0)) {
        std::fprintf(stderr, "[package-smoke] supports_tls()=%d, expected %d\n", tls ? 1 : 0, CORO_SMOKE_EXPECT_TLS);
        return 1;
    }
#endif
    client.close();
    if (!client.closed()) {
        std::fprintf(stderr, "[package-smoke] close() did not take effect\n");
        return 1;
    }
    if (coro::run(reject_after_close(&client)) != 0) {
        std::fprintf(stderr, "[package-smoke] request after close() was not rejected\n");
        return 1;
    }
    std::printf("[package-smoke] http_client component linked (tls=%d)\n", coro::HttpClient::supports_tls() ? 1 : 0);
#endif

    std::printf("[package-smoke] ok\n");
    return 0;
}
