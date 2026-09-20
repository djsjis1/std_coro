// test_fs_watch.cpp — 目录监视: 创建/修改/重命名/删除 事件
#if defined(_WIN32) || (defined(__linux__) && (!defined(CORO_HAS_URING) || CORO_HAS_URING))
#include <gtest/gtest.h>

#include <coro/coro.hpp>
#include <coro/fs.hpp>
#include <coro/fs_watch.hpp>

#include "test_util.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <string>
#include <vector>

#ifdef _WIN32
#include <process.h>
#else
#include <unistd.h>
#endif

using namespace std::chrono_literals;

namespace {

    std::string make_watch_dir() {
        static std::atomic<unsigned> sequence{0};
#ifdef _WIN32
        const auto pid = _getpid();
#else
        const auto pid = getpid();
#endif
        const auto dir = (std::filesystem::temp_directory_path() /
                          ("coro_watch_test_" + std::to_string(pid) + "_" + std::to_string(sequence.fetch_add(1))))
                             .string();
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

    coro::Task<> collect_recursive(coro::fs::DirectoryWatcher* w, std::vector<coro::fs::watch_event>* out) {
        bool saw_renamed_child = false;
        bool saw_dynamic_child = false;
        while (!saw_renamed_child || !saw_dynamic_child) {
            try {
                auto ev = co_await coro::wait_for(w->next(), 3s);
                if (ev.path == "renamed/after.txt")
                    saw_renamed_child = true;
                if (ev.path == "dynamic/new.txt")
                    saw_dynamic_child = true;
                out->push_back(std::move(ev));
            } catch (const coro::TimeoutError&) {
                co_return;
            }
        }
    }

    coro::Task<> recursive_watch_case(std::vector<coro::fs::watch_event>* out) {
        std::string dir = make_watch_dir();
        std::filesystem::create_directories(dir + "/existing");
        auto w = co_await coro::fs::watch(dir, /*recursive=*/true);
        if (!w.valid())
            co_return;

        auto collector = coro::spawn(collect_recursive(&w, out));
        co_await coro::sleep(100ms);
        co_await coro::fs::write_all(dir + "/existing/seed.txt", "seed");
        co_await coro::sleep(100ms);
        co_await coro::to_thread([&dir] { std::filesystem::rename(dir + "/existing", dir + "/renamed"); });
        co_await coro::sleep(100ms);
        co_await coro::fs::write_all(dir + "/renamed/after.txt", "after rename");
        co_await coro::sleep(100ms);
        co_await coro::to_thread([&dir] { std::filesystem::create_directories(dir + "/dynamic"); });
        co_await coro::sleep(150ms); // 让 Linux collector 先处理 IN_CREATE 并添加子 watch
        co_await coro::fs::write_all(dir + "/dynamic/new.txt", "dynamic");
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

TEST(FsWatchTest, RecursiveTracksExistingRenamedAndNewDirectories) {
    std::vector<coro::fs::watch_event> events;
    test_util::run_task([&] { return recursive_watch_case(&events); });

    auto has_path = [&](const char* path) {
        return std::any_of(events.begin(), events.end(), [&](const auto& event) { return event.path == path; });
    };
    EXPECT_TRUE(has_path("renamed/after.txt")) << "重命名子目录的 watch 映射未更新";
    EXPECT_TRUE(has_path("dynamic/new.txt")) << "新建子目录未动态加入递归监视";
}
#endif // _WIN32 || __linux__
