// dir_watch.cpp — coro::fs::watch 目录监视示例
//
// 监视当前目录 (非递归), 打印发生的文件事件。
// 构建运行: ./build/Release/example_dir_watch.exe
// 另开终端在当前目录 touch/删除文件即可看到事件输出。

#include <coro/coro.hpp>
#include <coro/fs.hpp>
#include <coro/fs_watch.hpp>

#include <cstdio>
#include <string>

using namespace std::chrono_literals;

static const char* type_name(coro::fs::watch_event_type t) {
    switch (t) {
        case coro::fs::watch_event_type::created:
            return "created ";
        case coro::fs::watch_event_type::removed:
            return "removed ";
        case coro::fs::watch_event_type::modified:
            return "modified";
        case coro::fs::watch_event_type::renamed:
            return "renamed ";
        case coro::fs::watch_event_type::overflow:
            return "OVERFLOW";
    }
    return "?";
}

coro::Task<> main_task() {
    std::string dir = ".";
    auto w = co_await coro::fs::watch(dir, /*recursive=*/false);
    if (!w.valid()) {
        std::printf("watch failed, error=%d\n", coro::io::last_error());
        co_return;
    }
    std::printf("watching \"%s\" ... (Ctrl+C 退出)\n", dir.c_str());

    // 自演示: 后台延迟生成事件 (ReadDirectoryChangesW 只报告挂起之后
    // 的变更, 所以写入必须在 next() 挂起之后发生)
    struct Demo {
        static coro::Task<> gen() {
            co_await coro::sleep(100ms);
            co_await coro::fs::write_all("dir_watch_demo.tmp", "demo"); // 创建+写入
            co_await coro::sleep(100ms);
            co_await coro::fs::write_all("dir_watch_demo.tmp", "demo2"); // 修改
        }
    };
    auto demo = coro::spawn(Demo::gen());

    // 监听 (用 wait_for + TimeoutError 做超时)
    int count = 0;
    while (count < 10) {
        coro::fs::watch_event ev;
        try {
            ev = co_await coro::wait_for(w.next(), 5s);
        } catch (const coro::TimeoutError&) {
            break; // 5 秒无事件: 退出
        }
        if (ev.type == coro::fs::watch_event_type::renamed)
            std::printf("  %-8s %s -> %s\n", type_name(ev.type), ev.old_path.c_str(), ev.path.c_str());
        else
            std::printf("  %-8s %s\n", type_name(ev.type), ev.path.c_str());
        count++;
    }

    co_await std::move(demo);
    std::remove("dir_watch_demo.tmp");
    w.close();
    std::printf("done (%d events)\n", count);
}

int main() {
    coro::run(main_task());
    return 0;
}
