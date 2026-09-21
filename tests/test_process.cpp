// test_process.cpp — 子进程: stdout 捕获 / 退出码 / 终止
#if defined(_WIN32) || (defined(__linux__) && (!defined(CORO_HAS_URING) || CORO_HAS_URING))
#include <gtest/gtest.h>

#include <coro/coro.hpp>
#include <coro/process.hpp>

#include "test_util.h"

#include <string>

using namespace std::chrono_literals;

namespace {

    // ── 命名协程函数 ──

    // stdout 捕获: cmd /c echo
    coro::Task<> capture_case(int* code, std::string* out) {
#ifdef _WIN32
        auto [c, o] = co_await coro::process::run_capture({"cmd", "/c", "echo hello-cor"});
#else
        auto [c, o] = co_await coro::process::run_capture({"sh", "-c", "echo hello-cor"});
#endif
        *code = c;
        *out = o;
    }

    // 退出码: cmd /c exit 42
    coro::Task<> exitcode_case(int* code) {
#ifdef _WIN32
        auto p = co_await coro::process::spawn({"cmd", "/c", "exit 42"}, {.capture_stdout = true});
#else
        auto p = co_await coro::process::spawn({"sh", "-c", "exit 42"}, {.capture_stdout = true});
#endif
        if (!p.valid()) {
            *code = -99;
            co_return;
        }
        *code = co_await p.wait();
    }

    // 终止: 起一个长任务, terminate 后 wait 返回 (不必是 0)
    coro::Task<> terminate_case(int* code, bool* wait_returned) {
#ifdef _WIN32
        auto p = co_await coro::process::spawn({"cmd", "/c", "timeout /t 30"}, {.capture_stdout = true});
#else
        auto p = co_await coro::process::spawn({"sleep", "30"}, {.capture_stdout = true});
#endif
        if (!p.valid()) {
            *wait_returned = false;
            co_return;
        }
        *wait_returned = false;
        *code = -1;

        // 100ms 后终止
        struct Killer {
            static coro::Task<> kill_later(coro::process::Process* pp) {
                co_await coro::sleep(100ms);
                pp->terminate();
            }
        };
        auto killer = coro::spawn(Killer::kill_later(&p));

        *code = co_await p.wait();
        *wait_returned = true; // wait 返回 = 终止已生效
        co_await std::move(killer);
    }

    // spawn 不存在的程序 → 无效 Process + 错误码
    coro::Task<> missing_case(bool* invalid, int* err) {
        auto p = co_await coro::process::spawn({"coro_definitely_missing_xyz"});
        *invalid = !p.valid();
        *err = coro::io::last_error();
        coro::io::clear_error();
    }

    coro::Task<> empty_args_case(bool* invalid, int* err) {
        auto p = co_await coro::process::spawn({});
        *invalid = !p.valid();
        *err = coro::io::last_error();
        coro::io::clear_error();
    }

    // 并发多进程
    // 注: 拆分为 helper + one_exit, 避免 GCC 13 协程变换中
    // move-only 类型与 co_return <value> 共存时触发 ICE
    static coro::Task<int> spawn_wait_exit(std::vector<std::string> args) {
        auto p = co_await coro::process::spawn(std::move(args), {.capture_stdout = true});
        if (!p.valid())
            co_return -1;
        int rc = co_await p.wait();
        co_return rc;  // p 在 co_return 前已离开作用域并析构
    }

    // 非协程包装: 直接返回 Task, 避免 GCC 13 协程变换 ICE
    coro::Task<int> one_exit(int v) {
#ifdef _WIN32
        return spawn_wait_exit({"cmd", "/c", "exit " + std::to_string(v)});
#else
        return spawn_wait_exit({"sh", "-c", "exit " + std::to_string(v)});
#endif
    }

    coro::Task<> concurrent_case(bool* all_match) {
        auto [a, b, c] = co_await coro::gather(one_exit(7), one_exit(8), one_exit(9));
        *all_match = (a == 7 && b == 8 && c == 9);
    }

} // namespace

TEST(ProcessTest, CaptureStdout) {
    int code = -1;
    std::string out;
    test_util::run_task([&] { return capture_case(&code, &out); });
    EXPECT_EQ(code, 0);
    // "echo hello-cor\r\n" (回显含换行)
    EXPECT_NE(out.find("hello-cor"), std::string::npos) << "out=" << out;
}

TEST(ProcessTest, ExitCode) {
    int code = -1;
    test_util::run_task([&] { return exitcode_case(&code); });
    EXPECT_EQ(code, 42);
}

TEST(ProcessTest, TerminateUnblocksWait) {
    int code = -1;
    bool wait_returned = false;
    test_util::run_task([&] { return terminate_case(&code, &wait_returned); });
    EXPECT_TRUE(wait_returned); // wait 被终止唤醒 (30s 的 timeout 没等满)
}

TEST(ProcessTest, SpawnMissingProgram) {
    bool invalid = false;
    int err = 0;
    test_util::run_task([&] { return missing_case(&invalid, &err); });
    EXPECT_TRUE(invalid);
    EXPECT_NE(err, 0);
}

TEST(ProcessTest, EmptyArgumentsAreRejected) {
    bool invalid = false;
    int err = 0;
    test_util::run_task([&] { return empty_args_case(&invalid, &err); });
    EXPECT_TRUE(invalid);
    EXPECT_NE(err, 0);
}

TEST(ProcessTest, ConcurrentProcesses) {
    bool all_match = false;
    test_util::run_task([&] { return concurrent_case(&all_match); });
    EXPECT_TRUE(all_match);
}
#endif // _WIN32 || CORO_URING_ENABLED
