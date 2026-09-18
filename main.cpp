// ============================================================================
// main.cpp — 文件监控练习场
// ============================================================================
//
// 监控指定目录的文件变化 (创建/修改/删除/重命名)。
// 构建并运行:
//   cmake --build build --config Debug --target coro_practice
//   ./build/Debug/coro_practice
//
// 另开一个终端, 在监控目录下 touch / 编辑 / 删除文件即可看到事件输出。
//
// 快速参考 (Python → 本库):
//   await asyncio.sleep(1)           →  co_await coro::sleep(1s)
//   await asyncio.gather(a(), b())   →  co_await coro::gather(a(), b())
//   asyncio.create_task(f())         →  auto t = coro::spawn(f());
//   await asyncio.wait_for(c, 5)     →  co_await coro::wait_for(c(), 5s)
//   async with asyncio.Lock():       →  co_await lock.acquire(); ... lock.release();
//   q = asyncio.Queue(); await q.put →  co_await q.put(x); co_await q.get()
//
// 网络 (Windows 默认 IOCP, Linux 默认 io_uring):
//   co_await listener.accept(); co_await stream.read/write
//   参考 examples/echo_server.cpp
// ============================================================================

#include <coro/coro.hpp>
#include <coro/fs.hpp>
#include <coro/fs_watch.hpp>

#include <chrono>
#include <cstdio>
#include <string>

using namespace coro;
using namespace std::chrono_literals;

// 事件类型 → 可读字符串
static const char* event_type_name(coro::fs::watch_event_type t) {
    switch (t) {
        case coro::fs::watch_event_type::created:
            return "创建  ";
        case coro::fs::watch_event_type::removed:
            return "删除  ";
        case coro::fs::watch_event_type::modified:
            return "修改  ";
        case coro::fs::watch_event_type::renamed:
            return "重命名";
        case coro::fs::watch_event_type::overflow:
            return "溢出  ";
    }
    return "未知";
}

// 打印一条事件 (带时间戳)
static void print_event(const coro::fs::watch_event& ev) {
    auto now = std::chrono::system_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count() % 100000;
    std::printf("[%05ldms] %s  %s", ms, event_type_name(ev.type), ev.path.c_str());
    if (ev.type == coro::fs::watch_event_type::renamed)
        std::printf("  (旧名: %s)", ev.old_path.c_str());
    if (ev.is_dir)
        std::printf("  [目录]");
    std::printf("\n");
}

// 后台演示: 自动产生一些文件事件, 方便观察
coro::Task<> demo_events() {
    co_await coro::sleep(500ms);

    std::printf("\n--- 开始自动演示事件 ---\n");

    // 1. 创建文件
    co_await coro::fs::write_all("watch_demo_tmp.txt", "hello coro\n");
    std::printf("  [demo] 创建 watch_demo_tmp.txt\n");
    co_await coro::sleep(300ms);

    // 2. 修改文件
    co_await coro::fs::write_all("watch_demo_tmp.txt", "hello coro v2\n");
    std::printf("  [demo] 修改 watch_demo_tmp.txt\n");
    co_await coro::sleep(300ms);

    // 3. 创建第二个文件
    co_await coro::fs::write_all("watch_demo_tmp2.txt", "another file\n");
    std::printf("  [demo] 创建 watch_demo_tmp2.txt\n");
    co_await coro::sleep(300ms);

    // 4. 删除文件
    std::remove("watch_demo_tmp.txt");
    std::printf("  [demo] 删除 watch_demo_tmp.txt\n");
    co_await coro::sleep(300ms);

    std::remove("watch_demo_tmp2.txt");
    std::printf("  [demo] 删除 watch_demo_tmp2.txt\n");
    std::printf("--- 演示事件结束 ---\n\n");
}

coro::Task<> main_task() {
    std::printf("=== coro 文件监控示例 ===\n\n");

    // 监控当前目录, 非递归
    std::string dir = ".";
    auto watcher = co_await coro::fs::watch(dir, /*recursive=*/false);
    if (!watcher.valid()) {
        std::printf("监控启动失败, error=%d\n", coro::io::last_error());
        co_return;
    }
    std::printf("正在监控目录: \"%s\"\n", dir.c_str());
    std::printf("另开终端在此目录下创建/修改/删除文件即可看到事件\n");
    std::printf("(超时 10 秒无事件自动退出)\n\n");

    // 启动后台演示, 自动产生一些事件
    auto demo = coro::spawn(demo_events());

    // 主循环: 持续读取事件, 10 秒超时退出
    int event_count = 0;
    while (true) {
        coro::fs::watch_event ev;
        try {
            ev = co_await coro::wait_for(watcher.next(), 10s);
        } catch (const coro::TimeoutError&) {
            std::printf("\n超时, 无新事件, 退出监控。\n");
            break;
        }

        // 跳过演示产生的临时文件, 只打印有意义的事件
        if (ev.path.find("watch_demo_") == std::string::npos) {
            print_event(ev);
            event_count++;
        } else {
            // 演示文件事件用灰色提示
            std::printf("  (demo)  %s  %s\n", event_type_name(ev.type), ev.path.c_str());
        }
    }

    co_await std::move(demo);

    // 清理演示残留文件
    std::remove("watch_demo_tmp.txt");
    std::remove("watch_demo_tmp2.txt");

    watcher.close();
    std::printf("\n=== 监控结束, 共收到 %d 个用户事件 ===\n", event_count);
}

int main() {
    coro::run(main_task());
    return 0;
}
