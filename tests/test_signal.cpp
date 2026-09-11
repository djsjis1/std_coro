// test_signal.cpp — 信号事件: wait / 多等待者 / handle / 超时取消
#include <gtest/gtest.h>

#include <coro/coro.hpp>
#include <coro/signal.hpp>

#include "test_util.h"

#include <atomic>
#include <csignal>
#include <mutex>

using namespace std::chrono_literals;

namespace {

    // 在 loop 线程直接 raise: Windows CRT 处理器是同线程同步调用,
    // deliver() 路由到等待者的 loop 后由事件循环恢复等待者 —— 路径
    // 与真实控制台事件 (专用线程投递) 完全一致, 且避免了 to_thread
    // 的临时 Task 生命周期问题。
    // Linux: 线程已阻塞该信号, raise 后进入 pending, 由 signalfd 消费。
    void raise_sig(int sig) {
        std::raise(sig);
    }

    // ── 命名协程函数 ──

    // 基本等待: 挂起 → (外部 raise) → 返回信号编号
    coro::Task<int> wait_one(int sig) {
        co_return co_await coro::signal::wait(sig);
    }

    coro::Task<> basic_case(int* received, bool* ok) {
        auto t = coro::spawn(wait_one(SIGBREAK));
        co_await coro::sleep(30ms); // 确保等待者已注册 (处理器已安装)
        raise_sig(SIGBREAK);
        *received = co_await std::move(t);
        *ok = (*received == SIGBREAK);
    }

    // 多个等待者: 一次投递唤醒全部
    coro::Task<int> waiter(int sig, int id, std::vector<int>* order, std::mutex* mtx) {
        int s = co_await coro::signal::wait(sig);
        {
            std::lock_guard lock(*mtx);
            order->push_back(id);
        }
        co_return s;
    }

    coro::Task<> multi_waiter_case(int* woken, std::vector<int>* order) {
        std::mutex mtx; // order 写入互斥 (等待者可能被路由并发恢复? 同 loop 单线程, 防御性)
        auto a = coro::spawn(waiter(SIGINT, 1, order, &mtx));
        auto b = coro::spawn(waiter(SIGINT, 2, order, &mtx));
        auto c = coro::spawn(waiter(SIGINT, 3, order, &mtx));
        co_await coro::sleep(30ms);
        raise_sig(SIGINT);
        co_await std::move(a);
        co_await std::move(b);
        co_await std::move(c);
        *woken = (int)order->size();
    }

    // handle: 持续处理, 两次信号触发两次 factory
    std::atomic<int> g_handle_fires{0};

    coro::Task<> handler_tick() {
        ++g_handle_fires;
        co_return;
    }

    coro::Task<> handle_case(int* fires, bool* saw_two) {
        g_handle_fires = 0;
        // 持有注册对象到协程结束 (RAII: co_return 后注销, 不阻塞 loop 退出)
        auto h = coro::signal::handle(SIGTERM, [] { return handler_tick(); });
        co_await coro::sleep(30ms); // 确保 handle 循环协程已挂起在 wait 上
        raise_sig(SIGTERM);
        co_await coro::sleep(50ms); // 处理协程执行
        raise_sig(SIGTERM);
        co_await coro::sleep(50ms);
        *fires = g_handle_fires.load();
        *saw_two = (*fires == 2);
        (void)h;
    }

    // 超时取消等待中的 signal::wait (等待者应被干净摘除)
    coro::Task<> cancel_case(bool* timed_out, bool* cancel_clean) {
        auto t = coro::spawn(wait_one(SIGBREAK));
        co_await coro::sleep(30ms);
        try {
            co_await coro::wait_for(std::move(t), 50ms);
            *timed_out = false;
        } catch (const coro::TimeoutError&) {
            *timed_out = true; // 超时: wait 已被取消, 等待者摘除
        }
        // 再发一次信号: 不应有崩溃/悬挂 (旧等待者已摘除)
        raise_sig(SIGBREAK);
        co_await coro::sleep(30ms);
        *cancel_clean = true;
    }

} // namespace

TEST(SignalTest, WaitReceivesSignal) {
    int received = 0;
    bool ok = false;
    test_util::run_task([&] { return basic_case(&received, &ok); });
    EXPECT_TRUE(ok);
    EXPECT_EQ(received, SIGBREAK);
}

TEST(SignalTest, MultipleWaitersAllWoken) {
    int woken = 0;
    std::vector<int> order;
    test_util::run_task([&] { return multi_waiter_case(&woken, &order); });
    EXPECT_EQ(woken, 3); // 一次投递 → 三个等待者全部唤醒
}

TEST(SignalTest, HandleFiresPerSignal) {
    int fires = 0;
    bool saw_two = false;
    test_util::run_task([&] { return handle_case(&fires, &saw_two); });
    EXPECT_TRUE(saw_two);
    EXPECT_EQ(fires, 2); // 两次 SIGTERM → handler 执行两次
}

TEST(SignalTest, CancelledWaitIsClean) {
    bool timed_out = false, cancel_clean = false;
    test_util::run_task([&] { return cancel_case(&timed_out, &cancel_clean); });
    EXPECT_TRUE(timed_out);
    EXPECT_TRUE(cancel_clean);
}
