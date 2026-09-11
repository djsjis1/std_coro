// ============================================================================
// main.cpp — 协程练习场
// ============================================================================
//
// 在这里写你的协程练习代码。构建并运行:
//   cmake --build build --config Debug --target coro_practice
//   ./build/Debug/coro_practice.exe
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

#include <iostream>
#include <string>

using namespace coro;
using namespace std::chrono_literals;

static const char* kFilePath = "coro_demo_output.txt";

coro::Task<> writer(coro::Event& done) {
    std::cout << "[writer] 准备写入文件..." << std::endl;
    co_await coro::sleep(500ms);

    std::string data = "Hello from coro!\n"
                       "Line 2: async file IO\n"
                       "Line 3: Event + File combined\n";

    bool ok = co_await coro::fs::write_all(kFilePath, data);
    if (ok)
        std::cout << "[writer] 文件写入完成, 通知 reader" << std::endl;
    else
        std::cout << "[writer] 写入失败!" << std::endl;

    done.set();
}

coro::Task<> reader(coro::Event& done) {
    std::cout << "[reader] 等待 writer 完成..." << std::endl;
    co_await done.wait();

    std::cout << "[reader] 收到事件, 开始读取文件..." << std::endl;
    std::string content = co_await coro::fs::read_all(kFilePath);
    std::cout << "[reader] 文件内容:\n---\n" << content << "---" << std::endl;

    auto st = coro::fs::stat(kFilePath);
    std::cout << "[reader] 文件大小: " << st.size << " 字节" << std::endl;
}

coro::Task<> main_task() {
    std::cout << "=== 协程 Event + 文件 IO 示例 ===" << std::endl;

    coro::Event file_done;

    auto w = coro::spawn(writer(file_done));
    auto r = coro::spawn(reader(file_done));

    co_await std::move(w);
    co_await std::move(r);

    std::cout << "=== 全部完成 ===" << std::endl;
}

int main() {
    coro::run(main_task());
    return 0;
}
