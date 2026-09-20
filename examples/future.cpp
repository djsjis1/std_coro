// ============================================================================
// future.cpp - Promise/Future pattern: bridging callbacks to coroutines
// ============================================================================

#include <coro/coro.hpp>

#include <chrono>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

using namespace std::chrono_literals;

// ============================================================================
// Scenario: wrap a callback-based async API using Promise/Future
//
// Promise/Future 的核心用途:
//   把「回调式」的异步代码包装成「co_await 式」的协程代码。
//   这是桥接旧代码和协程世界的标准模式。
// ============================================================================

// ---- 模拟一个"老式"回调风格的异步下载器 ----
class AsyncDownloader {
  public:
    using Callback = std::function<void(const std::string&)>;

    void download(const std::string& url, Callback cb) {
        // 注册回调 (模拟异步 I/O 的完成通知)
        pending_.push_back({url, std::move(cb)});
    }

    // 由事件循环驱动: 处理所有待处理的回调
    void process() {
        for (auto& p : pending_) {
            p.cb("<<< content of " + p.url + " >>>");
        }
        pending_.clear();
    }

  private:
    struct Item {
        std::string url;
        Callback cb;
    };
    std::vector<Item> pending_;
};

// ---- 用 Promise/Future 包装为协程接口 ----
coro::Task<std::string> download_async(AsyncDownloader& dl, const std::string& url) {
    coro::Promise<std::string> promise;
    auto future = promise.get_future();

    // 在回调中完成 Promise
    dl.download(url, [&promise](const std::string& result) { promise.set_value(result); });

    // 模拟等待 I/O 完成, 然后触发回调
    co_await coro::sleep(100ms);
    dl.process();

    co_return co_await future;
}

// ---- 用 Promise/Future + spawn 实现延迟计算 ----
// Promise 被移入后台协程 (通过 shared_ptr), 在延迟后完成
coro::Task<> complete_later(std::shared_ptr<coro::Promise<int>> promise, int value, int delay_ms) {
    co_await coro::sleep(std::chrono::milliseconds(delay_ms));
    promise->set_value(value * 10);
}

coro::Task<int> delayed_compute(int value, int delay_ms) {
    auto promise = std::make_shared<coro::Promise<int>>();
    auto future = promise->get_future();

    // spawn 一个后台协程, 在 delay_ms 后完成 Promise
    // 注意: 必须保存 spawn 的返回值, 否则协程会被立即销毁
    auto bg = coro::spawn(complete_later(promise, value, delay_ms));

    co_return co_await future;
    // bg 在 delayed_compute 协程帧中保持存活, 直到此处析构
    // 此时后台协程已经完成 (因为 future 已被满足)
}

// ---- 已就绪的 Future: await_ready() 返回 true, 不挂起 ----
coro::Task<> test_ready_future() {
    std::cout << "4. Ready Future (no suspension):" << std::endl;

    coro::Promise<std::string> promise;
    auto future = promise.get_future();

    // 先设置值, 再 await — await_ready() 返回 true, 不会挂起
    promise.set_value("immediately ready!");
    std::cout << "   Promise set before await..." << std::endl;
    std::string result = co_await future;
    std::cout << "   result: " << result << std::endl;
}

// ---- 主协程 ----
coro::Task<> main_task() {
    std::cout << "=== Future/Promise Example ===" << std::endl << std::endl;

    // 1. 包装回调式 API
    std::cout << "1. Wrapping callback-style API:" << std::endl;
    AsyncDownloader downloader;
    auto content = co_await download_async(downloader, "https://example.com/data");
    std::cout << "   " << content << std::endl << std::endl;

    // 2. 延迟计算 (Promise 由后台协程在定时器触发后完成)
    std::cout << "2. Delayed computation (timer-based Promise):" << std::endl;
    int val = co_await delayed_compute(42, 200);
    std::cout << "   val(42) * 10 = " << val << std::endl << std::endl;

    // 3. 多个 Promise 并发 gather
    std::cout << "3. Multiple Promises with gather:" << std::endl;
    auto [v1, v2] = co_await coro::gather(delayed_compute(10, 300), delayed_compute(20, 200));
    std::cout << "   v1=" << v1 << ", v2=" << v2 << std::endl;
    std::cout << "   (total ~300ms)" << std::endl << std::endl;

    // 4. 已就绪 Future
    co_await test_ready_future();

    std::cout << std::endl << "=== Future/Promise example done ===" << std::endl;
}

int main() {
    coro::run(main_task());
    return 0;
}
