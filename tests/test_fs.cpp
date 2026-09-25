// test_fs.cpp — 异步文件 IO: open / read_at / write_at / stat / fsync / 便捷函数
#if defined(_WIN32) || (defined(__linux__) && defined(CORO_HAS_URING) && CORO_HAS_URING)
#include <gtest/gtest.h>

#include <coro/coro.hpp>
#include <coro/fs.hpp>

#include "test_util.h"

#include <cstdio>
#include <string>

namespace {

    // ── 测试用临时文件路径 (各用例名字唯一, 结束时清理) ──
    std::string tmp_path(const char* name) {
        const char* tmp = std::getenv("TEMP"); // Windows
        if (!tmp)
            tmp = std::getenv("TMPDIR"); // Linux
        std::string dir = (tmp ? tmp : ".");
        return dir + "/" + name + ".tmp";
    }

    // ── 命名协程函数 (结果经指针带出, 断言放 TEST 里) ──

    // 写读回环: write_all → read_all → 逐字节比对
    coro::Task<> roundtrip_case(const std::string* path, const std::string* content, bool* write_ok,
                                std::string* read_back) {
        *write_ok = co_await coro::fs::write_all(*path, *content);
        coro::io::clear_error();
        *read_back = co_await coro::fs::read_all(*path);
    }

    // 定位读: 在指定偏移读取, 验证返回内容与字节数
    coro::Task<> read_at_case(const std::string* path, std::string* chunk, int* at5, int* eof, int* past_eof) {
        auto f = co_await coro::fs::open(*path, coro::fs::mode::read);
        if (!f.valid())
            co_return;

        char buf[64];

        // 从 offset=5 读 10 字节
        *at5 = co_await f.read_at(buf, 10, 5);
        chunk->assign(buf, (size_t)(*at5 > 0 ? *at5 : 0));

        // 精确到文件尾 (offset = size): EOF → 0
        auto st = coro::fs::stat(*path);
        *eof = co_await f.read_at(buf, sizeof(buf), st.size);

        // 越过文件尾 (offset > size): 同样 EOF → 0
        *past_eof = co_await f.read_at(buf, sizeof(buf), st.size + 100);
    }

    // 并发分块读: 4 个协程各读文件的四分之一 (定位读无游标竞争)
    coro::Task<bool> read_chunk(const std::string* path, uint64_t off, size_t len, std::string* out) {
        auto f = co_await coro::fs::open(*path, coro::fs::mode::read);
        if (!f.valid())
            co_return false;
        out->resize(len);
        uint64_t done = 0;
        while (done < len) {
            int n = co_await f.read_at(out->data() + done, len - (size_t)done, off + done);
            if (n <= 0) {
                out->resize((size_t)done);
                co_return done == len;
            }
            done += (uint64_t)n;
        }
        co_return true;
    }

    coro::Task<> concurrent_case(const std::string* path, const std::string* content, bool* all_match) {
        size_t quarter = content->size() / 4;
        std::string a, b, c, d;
        // 四个分块读并发执行 (gather; 定位读无游标竞争)
        auto [ra, rb, rc, rd] =
            co_await coro::gather(read_chunk(path, 0, quarter, &a), read_chunk(path, quarter, quarter, &b),
                                  read_chunk(path, 2 * quarter, quarter, &c),
                                  read_chunk(path, 3 * quarter, content->size() - 3 * quarter, &d));
        *all_match = ra && rb && rc && rd && (a == content->substr(0, quarter)) &&
                     (b == content->substr(quarter, quarter)) && (c == content->substr(2 * quarter, quarter)) &&
                     (d == content->substr(3 * quarter));
    }

    // 追加模式: 两次 write_all, 内容应先后拼接
    coro::Task<> append_case(const std::string* path, std::string* final_content) {
        co_await coro::fs::write_all(*path, "hello,", coro::fs::mode::write);
        co_await coro::fs::write_all(*path, " world", coro::fs::mode::write | coro::fs::mode::append);
        *final_content = co_await coro::fs::read_all(*path);
    }

    // fsync + 独占创建
    coro::Task<> sync_excl_case(const std::string* path, bool* fsync_ok, bool* excl_first, bool* excl_second) {
        auto f =
            co_await coro::fs::open(*path, coro::fs::mode::write | coro::fs::mode::create | coro::fs::mode::exclusive);
        *excl_first = f.valid();
        if (f.valid()) {
            co_await f.write_at("sync-me", 7, 0);
            *fsync_ok = co_await f.fsync();
            f.close();
        }
        // 第二次独占创建同一文件 → 必须失败
        auto f2 =
            co_await coro::fs::open(*path, coro::fs::mode::write | coro::fs::mode::create | coro::fs::mode::exclusive);
        *excl_second = f2.valid();
    }

    // 打开不存在的文件 → 无效 File + 错误码
    coro::Task<> missing_case(const std::string* path, bool* invalid, int* err) {
        auto f = co_await coro::fs::open(*path, coro::fs::mode::read);
        *invalid = !f.valid();
        *err = coro::io::last_error();
        coro::io::clear_error();
        co_return;
    }

    // 写后截断重写 ("w" 语义)
    coro::Task<> truncate_case(const std::string* path, std::string* final_content) {
        co_await coro::fs::write_all(*path, "0123456789ABCDEFGHIJ");
        co_await coro::fs::write_all(*path, "short");
        *final_content = co_await coro::fs::read_all(*path);
    }

} // namespace

// ── 断言 (非协程) ──

TEST(FsTest, WriteReadRoundtrip) {
    std::string path = tmp_path("roundtrip");
    std::string content(10000, 'x');
    content.replace(123, 5, "HELLO"); // 掺一点非重复内容
    bool write_ok = false;
    std::string read_back;
    test_util::run_task([&] { return roundtrip_case(&path, &content, &write_ok, &read_back); });
    EXPECT_TRUE(write_ok);
    EXPECT_EQ(read_back, content);
    std::remove(path.c_str());
}

TEST(FsTest, ReadAtPositionalAndEof) {
    std::string path = tmp_path("readat");
    bool write_ok = false;
    std::string content = "0123456789abcdefghijklmnopqrstuvwxyz";
    std::string read_back;
    test_util::run_task([&] { return roundtrip_case(&path, &content, &write_ok, &read_back); });
    ASSERT_TRUE(write_ok);

    std::string chunk;
    int at5 = -99, eof = -99, past_eof = -99;
    test_util::run_task([&] { return read_at_case(&path, &chunk, &at5, &eof, &past_eof); });
    EXPECT_EQ(at5, 10);
    EXPECT_EQ(chunk, "56789abcde"); // offset 5 起 10 字节
    EXPECT_EQ(eof, 0);              // 精确 EOF → 0 字节
    EXPECT_EQ(past_eof, 0);         // 越过 EOF → 0 字节
    std::remove(path.c_str());
}

TEST(FsTest, ConcurrentChunkedRead) {
    std::string path = tmp_path("chunked");
    bool write_ok = false;
    std::string content(8192, '\0');
    for (size_t i = 0; i < content.size(); ++i)
        content[i] = (char)('A' + (i % 26));
    std::string read_back;
    test_util::run_task([&] { return roundtrip_case(&path, &content, &write_ok, &read_back); });
    ASSERT_TRUE(write_ok);

    bool all_match = false;
    test_util::run_task([&] { return concurrent_case(&path, &content, &all_match); });
    EXPECT_TRUE(all_match);
    std::remove(path.c_str());
}

TEST(FsTest, StatReportsSizeAndDir) {
    std::string path = tmp_path("statfile");
    bool write_ok = false;
    std::string content = "0123456789";
    std::string read_back;
    test_util::run_task([&] { return roundtrip_case(&path, &content, &write_ok, &read_back); });
    ASSERT_TRUE(write_ok);

    auto st = coro::fs::stat(path);
    coro::io::clear_error();
    EXPECT_TRUE(st.exists);
    EXPECT_FALSE(st.is_dir);
    EXPECT_EQ(st.size, 10u);
    EXPECT_GT(st.mtime_sec, 0);

    auto miss = coro::fs::stat(path + "_missing");
    coro::io::clear_error();
    EXPECT_FALSE(miss.exists);
    std::remove(path.c_str());
}

TEST(FsTest, AppendMode) {
    std::string path = tmp_path("append");
    std::string final_content;
    test_util::run_task([&] { return append_case(&path, &final_content); });
    EXPECT_EQ(final_content, "hello, world");
    std::remove(path.c_str());
}

TEST(FsTest, FsyncAndExclusiveCreate) {
    std::string path = tmp_path("excl");
    bool fsync_ok = false, excl_first = false, excl_second = true;
    test_util::run_task([&] { return sync_excl_case(&path, &fsync_ok, &excl_first, &excl_second); });
    coro::io::clear_error(); // excl_second 的失败会设置错误码, 属预期
    EXPECT_TRUE(fsync_ok);
    EXPECT_TRUE(excl_first);
    EXPECT_FALSE(excl_second);
    std::remove(path.c_str());
}

TEST(FsTest, OpenMissingFileSetsError) {
    std::string path = tmp_path("definitely_missing");
    bool invalid = false;
    int err = 0;
    test_util::run_task([&] { return missing_case(&path, &invalid, &err); });
    EXPECT_TRUE(invalid);
    EXPECT_NE(err, 0); // io::last_error() 有具体错误码 (ENOENT 等)
}

TEST(FsTest, WriteTruncatesExisting) {
    std::string path = tmp_path("trunc");
    std::string final_content;
    test_util::run_task([&] { return truncate_case(&path, &final_content); });
    EXPECT_EQ(final_content, "short"); // 第二次写 ("w") 截断了旧内容
    std::remove(path.c_str());
}
#endif // _WIN32 || __linux__
