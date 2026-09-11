// test_pipe.cpp — 异步管道: 回环 / 背压 / 对端关闭唤醒
#if defined(_WIN32) || defined(__linux__)
#include <gtest/gtest.h>

#include <coro/coro.hpp>
#include <coro/pipe.hpp>

#include "test_util.h"

#include <cstring>
#include <string>

namespace {

    // ── 命名协程函数 (结果经指针带出) ──

    // 基本回环: 写 → 读 → 比对
    coro::Task<> loopback_case(bool* write_ok, std::string* read_back, int* n_write) {
        auto [rd, wr] = coro::pipe::pair();
        const char msg[] = "hello, coro::pipe!";
        *n_write = co_await wr.write(msg, sizeof(msg) - 1);
        *write_ok = (*n_write == (int)sizeof(msg) - 1);

        char buf[64];
        int n = co_await rd.read(buf, sizeof(buf));
        read_back->assign(buf, (size_t)(n > 0 ? n : 0));
    }

    // 背压: 缓冲区 4KB, 先写 16KB (必然挂起), 读者随后分批读完
    coro::Task<> writer_fills(coro::pipe::PipeEnd wr, const std::string* data, int* total_written, bool*) {
        size_t off = 0;
        while (off < data->size()) {
            int n = co_await wr.write(data->data() + off, data->size() - off);
            if (n <= 0)
                co_return;
            off += (size_t)n;
        }
        *total_written = (int)off;
        wr.close(); // 写完关闭 → 读者将收到 EOF (0)
    }

    coro::Task<> reader_drains(coro::pipe::PipeEnd rd, std::string* out, bool* saw_eof) {
        char buf[1024];
        while (true) {
            int n = co_await rd.read(buf, sizeof(buf));
            if (n < 0)
                co_return;
            if (n == 0) { // 写端已关闭: EOF
                *saw_eof = true;
                co_return;
            }
            out->append(buf, (size_t)n);
        }
    }

    coro::Task<> backpressure_case(const std::string* data, std::string* read_back, int* total_written, bool* saw_eof) {
        auto [rd, wr] = coro::pipe::pair(4096); // 4KB 缓冲: 16KB 必然触发背压
        // 写者先行 (写满缓冲后挂起), 读者并发排空 → 写者被唤醒继续
        auto writer = coro::spawn(writer_fills(std::move(wr), data, total_written, nullptr));
        co_await reader_drains(std::move(rd), read_back, saw_eof);
        co_await std::move(writer);
    }

    // 读端挂起 → 写端关闭 → 读端收 0 唤醒
    coro::Task<> sleeper(coro::pipe::PipeEnd rd, int* n_read) {
        char buf[16];
        *n_read = co_await rd.read(buf, sizeof(buf)); // 挂起 (无数据)
    }

    coro::Task<> close_wakes_reader_case(int* n_read) {
        auto [rd, wr] = coro::pipe::pair();
        auto t = coro::spawn(sleeper(std::move(rd), n_read));
        co_await coro::sleep(std::chrono::milliseconds(50)); // 确保 reader 已挂起
        wr.close();                                          // 关闭写端
        co_await std::move(t);
    }

    // 生产者-消费者: 消息队列语义 (write 侧多条短消息, read 侧按序收)
    coro::Task<> produce(coro::pipe::PipeEnd wr, int count) {
        for (int i = 0; i < count; i++) {
            char msg[32];
            int len = std::snprintf(msg, sizeof(msg), "msg-%03d;", i);
            co_await wr.write(msg, (size_t)len);
        }
        wr.close();
    }

    coro::Task<> produce_consume_case(int count, int* received, bool* in_order) {
        auto [rd, wr] = coro::pipe::pair();
        auto prod = coro::spawn(produce(std::move(wr), count));

        std::string all;
        char buf[256];
        while (true) {
            int n = co_await rd.read(buf, sizeof(buf));
            if (n <= 0)
                break;
            all.append(buf, (size_t)n);
        }
        co_await std::move(prod);

        *received = 0;
        *in_order = true;
        size_t pos = 0;
        for (int i = 0; i < count; i++) {
            char expect[32];
            int len = std::snprintf(expect, sizeof(expect), "msg-%03d;", i);
            if (all.compare(pos, (size_t)len, expect) != 0) {
                *in_order = false;
                break;
            }
            pos += (size_t)len;
            (*received)++;
        }
    }

} // namespace

TEST(PipeTest, LoopbackWriteRead) {
    bool write_ok = false;
    std::string read_back;
    int n_write = 0;
    test_util::run_task([&] { return loopback_case(&write_ok, &read_back, &n_write); });
    EXPECT_TRUE(write_ok);
    EXPECT_EQ(n_write, 18);
    EXPECT_EQ(read_back, "hello, coro::pipe!");
}

TEST(PipeTest, BackpressureAndEof) {
    std::string data(16 * 1024, '\0');
    for (size_t i = 0; i < data.size(); ++i)
        data[i] = (char)('a' + (i % 26));
    std::string read_back;
    int total_written = 0;
    bool saw_eof = false;
    test_util::run_task([&] { return backpressure_case(&data, &read_back, &total_written, &saw_eof); });
    EXPECT_EQ(total_written, (int)data.size()); // 16KB 全部写出 (中途被背压挂起)
    EXPECT_EQ(read_back.size(), data.size());   // 全部读回
    EXPECT_EQ(read_back, data);
    EXPECT_TRUE(saw_eof); // 写端关闭后读到 EOF
}

TEST(PipeTest, CloseWriterWakesSuspendedReader) {
    int n_read = -99;
    test_util::run_task([&] { return close_wakes_reader_case(&n_read); });
    EXPECT_EQ(n_read, 0); // 写端关闭 → 挂起的读被唤醒并返回 0 (EOF)
}

TEST(PipeTest, ProducerConsumerOrdering) {
    int received = 0;
    bool in_order = false;
    test_util::run_task([&] { return produce_consume_case(200, &received, &in_order); });
    EXPECT_EQ(received, 200);
    EXPECT_TRUE(in_order);
}
#endif // _WIN32 || __linux__
