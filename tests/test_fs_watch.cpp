// test_fs_watch.cpp — 目录监视: 创建/修改/重命名/删除 事件
#include <gtest/gtest.h>

#include <coro/coro.hpp>
#include <coro/fs.hpp>
#include <coro/fs_watch.hpp>

#include "test_util.h"

#include <chrono>
#include <filesystem>
#include <string>
#include <vector>

using namespace std::chrono_literals;

namespace {

    std::string make_watch_dir() {
        const char* tmp = std::getenv("TEMP");
        if (!tmp)
            tmp = std::getenv("TMPDIR");
        std::string dir = (tmp ? tmp : ".");
        dir += "/coro_watch_test";
        std::filesystem::remove_all(dir);
        std::filesystem::create_directories(dir);
        return dir;
    }

    // 收集事件, 直到看到「删除 b.txt」或超时
    coro::Task<> collect_until_done(coro::fs::DirectoryWatcher* w, std::vector<coro::fs::watch_event>* out) {
        while (true) {
            coro::fs::watch_event ev;
            try {
                ev = co_await coro::wait_for(w->next(), 3s);
            } catch (const coro::TimeoutError&) {
                co_return; // 安静了: 结束收集
            }
            out->push_back(std::move(ev));
            if (out->back().type == coro::fs::watch_event_type::removed &&
                out->back().path.find("b.txt") != std::string::npos)
                co_return; // 看到最终删除: 结束
        }
    }

    // 文件操作序列 (改名/删除经 to_thread, 同步文件系统调用不碰 loop)
    coro::Task<> do_ops(const std::string* dir) {
        co_await coro::sleep(100ms);                                  // 确保监视已挂起
        co_await coro::fs::write_all(*dir + "/a.txt", "hello watch"); // 创建+写入
        co_await coro::sleep(100ms);
        co_await coro::to_thread([dir] { std::filesystem::rename(*dir + "/a.txt", *dir + "/b.txt"); });
        co_await coro::sleep(100ms);
        co_await coro::to_thread([dir] { std::filesystem::remove(*dir + "/b.txt"); });
    }

    coro::Task<> watch_case(std::vector<coro::fs::watch_event>* out) {
        std::string dir = make_watch_dir();
        auto w = co_await coro::fs::watch(dir, /*recursive=*/false);
        if (!w.valid())
            co_return; // 监视打开失败: 测试由断言暴露

        auto collector = coro::spawn(collect_until_done(&w, out));
        co_await do_ops(&dir);
        co_await std::move(collector);
        w.close();
        std::filesystem::remove_all(dir);
    }

} // namespace

TEST(FsWatchTest, CreateModifyRenameRemoveEvents) {
    std::vector<coro::fs::watch_event> events;
    test_util::run_task([&] { return watch_case(&events); });

    ASSERT_FALSE(events.empty()) << "没有收到任何目录事件";

    auto has = [&](coro::fs::watch_event_type t, const char* substr) {
        for (const auto& e : events)
            if (e.type == t && e.path.find(substr) != std::string::npos)
                return true;
        return false;
    };

    // 创建 a.txt (创建/修改事件的粒度由内核决定, 两者都接受)
    EXPECT_TRUE(has(coro::fs::watch_event_type::created, "a.txt") || has(coro::fs::watch_event_type::modified, "a.txt"))
        << "未见 a.txt 的创建/修改事件";
    // 重命名 a.txt → b.txt
    bool saw_rename = false;
    for (const auto& e : events)
        if (e.type == coro::fs::watch_event_type::renamed && e.path.find("b.txt") != std::string::npos &&
            e.old_path.find("a.txt") != std::string::npos)
            saw_rename = true;
    EXPECT_TRUE(saw_rename) << "未见 a.txt→b.txt 的重命名事件";
    // 删除 b.txt
    EXPECT_TRUE(has(coro::fs::watch_event_type::removed, "b.txt")) << "未见 b.txt 的删除事件";
}
