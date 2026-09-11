// file_io.cpp — coro::fs 异步文件 IO 示例
//
// 演示: open / read_at / write_at / stat / read_all / write_all / fsync
// 以及多协程并发分块读同一文件 (定位读无游标竞争)。
//
// 构建运行: ./build/Release/example_file_io.exe

#include <coro/coro.hpp>
#include <coro/fs.hpp>

#include <cstdio>
#include <string>

using namespace std::chrono_literals;

// ── 1. 基本写读回环 ──
coro::Task<> basic_roundtrip(const std::string& path) {
    // write_all: 一次性写入 ("w" 语义 = 创建或截断)
    bool ok = co_await coro::fs::write_all(path, "Hello, coro::fs!\n0123456789\n");
    std::printf("[1] write_all: %s\n", ok ? "ok" : "failed");

    // stat: 查元信息 (同步, 元数据操作)
    auto st = coro::fs::stat(path);
    std::printf("[1] stat: size=%llu mtime=%lld\n", (unsigned long long)st.size, (long long)st.mtime_sec);

    // read_all: 一次性读回
    std::string back = co_await coro::fs::read_all(path);
    std::printf("[1] read_all: %zu bytes: %.20s...\n", back.size(), back.c_str());
}

// ── 2. 定位读写 (read_at / write_at): 文件游标无关, 可并发分块 ──
coro::Task<> positional_rw(const std::string& path) {
    auto f = co_await coro::fs::open(path, coro::fs::mode::write);
    if (!f.valid()) {
        std::printf("[2] open failed, error=%d\n", coro::io::last_error());
        co_return;
    }

    // 在不同 offset 写两段 (互不影响)
    int n1 = co_await f.write_at("AAAA", 4, 0);  // 字节 0~3
    int n2 = co_await f.write_at("BBBB", 4, 10); // 字节 10~13
    std::printf("[2] write_at: %d + %d bytes\n", n1, n2);

    bool synced = co_await f.fsync(); // 刷盘 (Windows: to_thread; Linux: io_uring)
    std::printf("[2] fsync: %s\n", synced ? "ok" : "failed");
    f.close();

    // 定位读验证
    auto r = co_await coro::fs::open(path, coro::fs::mode::read);
    char buf[16]{};
    int got = co_await r.read_at(buf, 4, 10); // 读 offset 10 的 4 字节
    std::printf("[2] read_at(10): %d bytes: %.4s\n", got, buf);

    // EOF 语义: 精确到文件尾 / 越过文件尾 → 0
    auto st = coro::fs::stat(path);
    int at_eof = co_await r.read_at(buf, 4, st.size);
    std::printf("[2] read_at(EOF): %d (0 = 到达文件尾)\n", at_eof);
}

// ── 3. 并发分块读: 4 个协程各读 1/4, gather 汇合 ──
coro::Task<std::string> read_slice(const std::string& path, uint64_t off, size_t len) {
    auto f = co_await coro::fs::open(path, coro::fs::mode::read);
    std::string out(len, '\0');
    uint64_t done = 0;
    while (done < len) {
        int n = co_await f.read_at(out.data() + done, len - (size_t)done, off + done);
        if (n <= 0)
            break;
        done += (uint64_t)n;
    }
    out.resize((size_t)done);
    co_return out;
}

coro::Task<> concurrent_chunks(const std::string& path) {
    auto st = coro::fs::stat(path);
    size_t q = st.size / 4;
    auto&& [a, b, c, d] = co_await coro::gather(read_slice(path, 0, q), read_slice(path, q, q),
                                                read_slice(path, 2 * q, q), read_slice(path, 3 * q, st.size - 3 * q));
    std::printf("[3] 4 个协程并发分块读: %zu + %zu + %zu + %zu = %zu 字节\n", a.size(), b.size(), c.size(), d.size(),
                a.size() + b.size() + c.size() + d.size());
}

// ── 主协程 ──
coro::Task<> main_task() {
    std::string path = "file_io_demo.tmp";

    co_await basic_roundtrip(path);
    co_await positional_rw(path);
    co_await concurrent_chunks(path);

    std::remove(path.c_str());
    std::printf("done.\n");
}

int main() {
    coro::run(main_task());
    return 0;
}
