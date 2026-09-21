// process_demo.cpp — coro::process 子进程示例
//
// 演示: 捕获 stdout / 退出码 / 并发多进程 / stdin 写入
// 构建运行: Windows build/Release/example_process.exe；Linux ./build/example_process

#include <coro/coro.hpp>
#include <coro/process.hpp>

#include <cstdio>
#include <string>

// ── 1. 捕获输出 ──
coro::Task<> run_and_capture() {
#ifdef _WIN32
    auto [code, out] = co_await coro::process::run_capture({"cmd", "/c", "echo hello from subprocess"});
#else
    auto [code, out] = co_await coro::process::run_capture({"sh", "-c", "echo hello from subprocess"});
#endif
    std::printf("[1] exit=%d stdout=%.*s", code, (int)out.size(), out.c_str());
}

// ── 2. 退出码 ──
// 注: 拆分为 helper + exit_code, 避免 GCC 13 协程变换中
// move-only 类型与 co_return <value> 共存时触发 ICE
static coro::Task<int> spawn_wait_exit(std::vector<std::string> args) {
    auto p = co_await coro::process::spawn(std::move(args), {.capture_stdout = true});
    if (!p.valid())
        co_return -1;
    int rc = co_await p.wait();
    co_return rc;
}

coro::Task<int> exit_code(int v) {
#ifdef _WIN32
    return spawn_wait_exit({"cmd", "/c", "exit " + std::to_string(v)});
#else
    return spawn_wait_exit({"sh", "-c", "exit " + std::to_string(v)});
#endif
}

// ── 3. stdin 交互: 管道写入 + 读取回显 ──
coro::Task<> stdin_roundtrip() {
#ifdef _WIN32
    auto p = co_await coro::process::spawn({"cmd", "/c", "findstr x"}, {.capture_stdin = true, .capture_stdout = true});
#else
    auto p = co_await coro::process::spawn({"grep", "x"}, {.capture_stdin = true, .capture_stdout = true});
#endif
    if (!p.valid()) {
        std::printf("[3] spawn failed: %d\n", coro::io::last_error());
        co_return;
    }
    // findstr x: 只回显包含 "x" 的行
    const char msg[] = "aaa\nbbb\nccc\nxxx\n";
    co_await p.stdin_pipe()->write(msg, sizeof(msg) - 1);
    p.stdin_pipe()->close(); // EOF → findstr 输出并退出

    std::string out;
    char buf[256];
    while (true) {
        int n = co_await p.stdout_pipe()->read(buf, sizeof(buf));
        if (n <= 0)
            break;
        out.append(buf, (size_t)n);
    }
    int code = co_await p.wait();
    std::printf("[3] exit=%d matched=%.*s", code, (int)out.size(), out.c_str());
}

coro::Task<> main_task() {
    co_await run_and_capture();

    auto [a, b] = co_await coro::gather(exit_code(3), exit_code(5));
    std::printf("[2] concurrent exits: %d %d\n", a, b);

    co_await stdin_roundtrip();
    std::printf("done.\n");
}

int main() {
    coro::run(main_task());
    return 0;
}
