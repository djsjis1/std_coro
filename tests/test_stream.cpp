// test_stream.cpp — coro::stream_reader / stream_writer 的边界语义 (计划 M4a)
//
// 用可编程的内存源拿到确定性: 短读、跨 fill 边界的分隔符、EOF 残留、IO 错误、
// 短写、写不动时的"零进展"。真实 socket 只用一条集成用例覆盖端到端接线。
#if defined(_WIN32) || (defined(__linux__) && defined(CORO_HAS_URING) && CORO_HAS_URING)

#include <gtest/gtest.h>

#include <coro/coro.hpp>
#include <coro/net.hpp> // 集成用例需要真实 socket: 聚合头不包含 IO 模块
#include <coro/stream.hpp>

#include "test_util.h"

#include <cerrno>
#include <coroutine>
#include <cstring>
#include <string>
#include <vector>

namespace {

    /// 立即完成的 awaiter: 让 mock 满足"成员返回可 await 的整型"这一形状要求
    struct sync_int {
        int value;
        bool await_ready() const noexcept { return true; }
        void await_suspend(std::coroutine_handle<>) const noexcept {}
        int await_resume() const noexcept { return value; }
    };

    /// 按脚本演出的假源。reads 里每个元素: >0 交付该字节数, 0 表示 EOF, -1 表示 IO 错误。
    struct mock_source {
        std::vector<int> reads;
        std::string payload;     // 依次交付的原始字节
        bool endless = false;    // 脚本用尽后仍按最后一个长度继续产数据 (模拟永不结束的流)
        std::vector<int> writes; // 每次 write 的返回值脚本
        std::string received;    // 累计被写出的内容
        std::size_t rpos = 0;    // payload 读游标
        std::size_t step = 0;    // reads 游标
        std::size_t wstep = 0;   // writes 游标
        int error_number = ECONNRESET;

        sync_int read(char* buf, std::size_t n) {
            if (step >= reads.size()) {
                if (!endless)
                    return sync_int{0};                      // 脚本耗尽 = 对端关闭
                step = reads.empty() ? 1 : reads.size() - 1; // 重复最后一次交付
                rpos = 0;                                    // 数据循环供给
            }
            const int action = reads[step++];
            if (action < 0) {
                errno = error_number;
                return sync_int{-1};
            }
            if (action == 0)
                return sync_int{0};
            // 交付长度要同时受脚本、调用方缓冲、payload 剩余量三者约束: 少最后一项,
            // 脚本里写 100/64 就会读到 payload 之外的内存, 结果变成看堆内容的假失败。
            std::size_t take = static_cast<std::size_t>(action) < n ? static_cast<std::size_t>(action) : n;
            if (rpos >= payload.size())
                rpos = endless ? 0 : payload.size();
            const std::size_t avail = payload.size() - rpos;
            if (take > avail)
                take = avail;
            if (take == 0)
                return sync_int{0};
            std::memcpy(buf, payload.data() + rpos, take);
            rpos += take;
            return sync_int{static_cast<int>(take)};
        }

        sync_int write(const char* buf, std::size_t n) {
            if (wstep >= writes.size())
                return sync_int{0};
            const int action = writes[wstep++];
            if (action < 0) {
                errno = error_number;
                return sync_int{-1};
            }
            const std::size_t take = static_cast<std::size_t>(action) < n ? static_cast<std::size_t>(action) : n;
            received.append(buf, take);
            return sync_int{static_cast<int>(take)};
        }
    };

    mock_source make_source(std::string data, std::vector<int> reads) {
        mock_source m;
        m.payload = std::move(data);
        m.reads = std::move(reads);
        return m;
    }

    std::string read_lines(std::string data, std::vector<int> reads) {
        std::string joined;
        auto src = make_source(std::move(data), std::move(reads));
        coro::run([&src, &joined]() -> coro::Task<> {
            coro::stream_reader reader(src);
            while (auto line = co_await reader.read_line())
                joined += *line + "|";
        }());
        return joined;
    }

    /// 跨 fill 边界的分隔符 + 多行: 每条 read_line 都可能在任意字节处断掉
    std::string read_lines_one_byte_at_a_time(std::string data) {
        std::string joined;
        auto src = make_source(std::move(data), std::vector<int>(64, 1));
        coro::run([&src, &joined]() -> coro::Task<> {
            coro::stream_reader reader(src, 4); // 小缓冲逼出压缩与跨块逻辑
            while (auto line = co_await reader.read_line())
                joined += *line + "|";
        }());
        return joined;
    }

    bool read_exactly(std::string data, std::vector<int> reads, std::string& out, std::size_t want) {
        bool incomplete = false;
        auto src = make_source(std::move(data), std::move(reads));
        out.assign(want, '\0');
        coro::run([&]() -> coro::Task<> {
            coro::stream_reader reader(src);
            try {
                co_await reader.read_exactly(out.data(), want);
            } catch (const coro::IncompleteStreamError&) {
                incomplete = true;
            }
        }());
        return incomplete;
    }

    /// 服务端侧: 接受一个连接并按块写出 HTTP 头 (带 CRLF, 交给 reader 切行)。
    /// 必须是命名协程 —— 临时 lambda 协程的闭包会在挂起点之前销毁。
    coro::Task<> accept_and_write(coro::net::TcpListener* listener) {
        auto server = co_await listener->accept();
        if (!server.valid())
            co_return;
        coro::stream_writer w(server);
        (void)co_await w.write_all("HTTP/1.1 204 No Content\r\nX-Stream: yes\r\n\r\n");
        co_await coro::sleep(std::chrono::milliseconds(30));
        co_return;
    }

    coro::Task<> tcp_round_trip(std::string* got_line) {
        coro::net::TcpListener listener;
        bool bound = false;
        for (unsigned short port = 19600; port < 19620; ++port) {
            if (listener.bind_listen("127.0.0.1", port)) {
                bound = true;
                break;
            }
        }
        if (!bound)
            co_return;

        auto acceptor = coro::spawn(accept_and_write(&listener));

        auto client = co_await coro::net::TcpStream::connect("127.0.0.1", listener.local_port());
        if (!client.valid()) {
            co_await std::move(acceptor);
            co_return;
        }
        coro::stream_reader r(client);
        std::string first;
        while (auto line = co_await r.read_line()) {
            if (first.empty())
                first = *line;
            else
                break; // 第二个空行即头部结束
        }
        *got_line = first;
        co_await std::move(acceptor);
    }

} // namespace

TEST(StreamTest, ReadLineSplitsOnNewline) {
    EXPECT_EQ(read_lines("alpha\nbeta\n", {100}), "alpha|beta|");
}

TEST(StreamTest, ReadLineAcceptsCRLF) {
    EXPECT_EQ(read_lines("one\r\ntwo\r\n", {100}), "one|two|");
}

TEST(StreamTest, ReadLineReturnsTrailingFragmentOnEof) {
    // 最后一行没有换行: 对端结束时也必须交给调用方, 不能吞掉
    EXPECT_EQ(read_lines("a\nlastbit", {100}), "a|lastbit|");
}

TEST(StreamTest, ShortReadsAreAbsorbed) {
    // 每次只给 1 字节, 逐字喂进缓冲: 结果必须与一次给完完全一致
    EXPECT_EQ(read_lines_one_byte_at_a_time("x\nyy\nzzz\n"), "x|yy|zzz|");
}

TEST(StreamTest, SmallBufferStillFindsDelimiter) {
    // 缓冲只有 4 字节而分隔符在更远处: 依赖缓冲生长与跨块扫描
    EXPECT_EQ(read_lines_one_byte_at_a_time("prefix-and-a-long-line\nnext\n"), "prefix-and-a-long-line|next|");
}

TEST(StreamTest, ReadUntilCustomDelimiter) {
    std::string out;
    auto src = make_source("hdr1::hdr2::", {3, 100});
    coro::run([&src, &out]() -> coro::Task<> {
        coro::stream_reader r(src);
        auto a = co_await r.read_until("::");
        auto b = co_await r.read_until("::");
        if (a)
            out += *a + "|";
        if (b)
            out += *b + "|";
    }());
    EXPECT_EQ(out, "hdr1|hdr2|");
}

TEST(StreamTest, ReadExactlyFillsAcrossChunks) {
    std::string got;
    EXPECT_FALSE(read_exactly("abcdefgh", {3, 5}, got, 8));
    EXPECT_EQ(got, "abcdefgh");
}

TEST(StreamTest, ReadExactlyReportsIncompleteWithCounts) {
    bool incomplete = false;
    std::size_t want = 0, have = 0;
    auto src = make_source("abc", {3, 0});
    std::string out(10, '\0');
    coro::run([&]() -> coro::Task<> {
        coro::stream_reader r(src);
        try {
            co_await r.read_exactly(out.data(), 10);
        } catch (const coro::IncompleteStreamError& e) {
            incomplete = true;
            want = e.requested;
            have = e.received;
        }
    }());
    EXPECT_TRUE(incomplete) << "读不满必须报 IncompleteStreamError, 而不是伪装成 EOF";
    EXPECT_EQ(want, 10u);
    EXPECT_EQ(have, 3u);
}

TEST(StreamTest, IoErrorThrowsSystemErrorWithErrno) {
    bool threw = false;
    int code = 0;
    auto src = make_source("", {-1});
    src.error_number = EPIPE;
    coro::run([&]() -> coro::Task<> {
        coro::stream_reader r(src);
        try {
            (void)co_await r.read_line();
        } catch (const std::system_error& e) {
            threw = true;
            code = e.code().value();
        }
    }());
    EXPECT_TRUE(threw) << "IO 错误应与 EOF 区分: 走 system_error";
    EXPECT_EQ(code, EPIPE);
}

TEST(StreamTest, WriterHandlesShortWrites) {
    std::string received;
    auto src = mock_source{};
    src.writes = {2, 3, 100}; // 两次短写后写完
    coro::run([&src, &received]() -> coro::Task<> {
        coro::stream_writer w(src);
        EXPECT_TRUE(co_await w.write_all("hello"));
        received = src.received;
    }());
    EXPECT_EQ(received, "hello");
}

TEST(StreamTest, WriterStopsWhenPeerCannotAccept) {
    bool progressed = true;
    auto src = mock_source{};
    src.writes = {2, 0}; // 写了两个字节后流不可写: 必须停下而不是死循环
    coro::run([&src, &progressed]() -> coro::Task<> {
        coro::stream_writer w(src);
        progressed = co_await w.write_all("hello");
    }());
    EXPECT_FALSE(progressed);
    EXPECT_EQ(src.received, "he") << "已写出的部分不回退, 调用方需知道协议已不一致";
}

TEST(StreamTest, WriterThrowsOnIoError) {
    bool threw = false;
    auto src = mock_source{};
    src.writes = {-1};
    src.error_number = ENOTCONN;
    coro::run([&src, &threw]() -> coro::Task<> {
        coro::stream_writer w(src);
        try {
            (void)co_await w.write_all("x");
        } catch (const std::system_error& e) {
            threw = true;
            EXPECT_EQ(e.code().value(), ENOTCONN);
        }
    }());
    EXPECT_TRUE(threw);
}

TEST(StreamTest, WriteLineAppendsNewline) {
    auto src = mock_source{};
    src.writes = {100, 100};
    coro::run([&src]() -> coro::Task<> {
        coro::stream_writer w(src);
        (void)co_await w.write_line("done");
    }());
    EXPECT_EQ(src.received, "done\n");
}

TEST(StreamTest, MissingDelimiterWithinBufferLimitFailsFast) {
    // 关键回归: 没有上限时, 遇到"一行极长/永无分隔符"的流会把整条流读进内存。
    // endless 源从不返 0, 所以唯一正确的行为是撞上限后报错。
    bool overflowed = false;
    std::size_t limit_seen = 0;
    mock_source src;
    src.payload = std::string(64, 'x'); // 不含换行
    src.reads = std::vector<int>(4096, 64);
    src.endless = true;
    coro::run([&]() -> coro::Task<> {
        // 上限 = 初始容量: 缓冲完全没有生长余地, 第一次 fill 就必须报超限
        // (若给它生长空间, 就要依赖 vector 的 capacity 行为, 断言会变得含糊)
        coro::stream_reader r(src, 64, 64);
        try {
            (void)co_await r.read_line();
        } catch (const coro::StreamOverflowError& e) {
            overflowed = true;
            limit_seen = e.limit;
        }
    }());
    EXPECT_TRUE(overflowed) << "缓冲必须有可预期的硬上限, 否则就是无界吃内存";
    EXPECT_EQ(limit_seen, 64u);
    EXPECT_LT(src.rpos, 4096u) << "撞上限后必须立即停下, 不该继续拉数据";
}

TEST(StreamTest, WorksOverRealTcpSocket) {
    std::string first;
    test_util::run_task([&] { return tcp_round_trip(&first); });
    EXPECT_EQ(first, "HTTP/1.1 204 No Content") << "真实 socket 上的按行读取未走通";
}

#endif // _WIN32 || io_uring
