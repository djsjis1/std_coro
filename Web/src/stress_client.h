#pragma once

#include <coro/coro.hpp>
#include <http_parse.h>

#include <chrono>
#include <cstddef>
#include <string>

// 压测客户端内部工具: 与回归测试共用, 不属于核心 coro API。
namespace web_stress {

    // Stream 只需异步 read/write; 测试用短读写替身确定性覆盖 TCP 的部分完成语义。
    // 名字刻意避开 "exchange": 与 std 类型参数同用会被 ADL 引入 std::exchange,
    // 遮蔽本函数 (曾致编译期把 request 移入 conn 返回 conn 本身)。
    template <typename Stream> coro::Task<bool> roundtrip(Stream& conn, const std::string& request) {
        size_t sent = 0;
        while (sent < request.size()) {
            int n = co_await conn.write(request.data() + sent, request.size() - sent);
            if (n <= 0 || static_cast<size_t>(n) > request.size() - sent)
                co_return false;
            sent += static_cast<size_t>(n);
        }

        http_parse parser(HTTP_RESPONSE);
        int completed = 0;
        bool expected_status = true;
        parser.message_complete = [&] {
            ++completed;
            expected_status = expected_status && parser.status_code == 200;
        };
        char buffer[4096];
        while (completed == 0) {
            int n = co_await conn.read(buffer, sizeof(buffer));
            if (n <= 0 || !parser.feed(buffer, static_cast<size_t>(n)))
                co_return false;
        }
        co_return completed == 1 && expected_status;
    }

    // 时限覆盖完整请求的写入与读取, 零星字节不会重置 deadline。
    template <typename Stream>
    coro::Task<bool> exchange_with_timeout(Stream& conn, std::string request, std::chrono::milliseconds timeout) {
        try {
            co_return co_await coro::wait_for(roundtrip(conn, request), timeout);
        } catch (const coro::TimeoutError&) {
            co_return false;
        }
    }

    inline int exit_code(long long succeeded, long long connect_failures, long long io_failures, long long expected) {
        return succeeded == expected && connect_failures == 0 && io_failures == 0 ? 0 : 1;
    }

} // namespace web_stress
