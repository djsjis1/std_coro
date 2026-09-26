// timer_context.cpp — 可重置 Timer 与显式 Context 取消的最小示例
//
// 演示三件事:
//   1. Timer 的 wait() 既能到期返回 true, 也能被 cancel() 唤醒返回 false;
//   2. 看门狗用"到期→打点→reset"实现周期行为, 而不是让库偷偷起后台任务;
//   3. Context 显式传递给子协程: 长任务靠 throw_if_cancelled() 检查点跳出,
//      监督者靠 co_await ctx.wait() 区分"被取消"与"超时"两种终态。
#include <coro/coro.hpp>
#include <coro/context.hpp>
#include <coro/timer.hpp>

#include <chrono>
#include <iostream>

using namespace std::chrono_literals;

// 空闲看门狗: 每次到期打一行, 被 cancel 时结束。
// Timer 是一次性的, 所以周期行为靠用户自己 reset 表达 —— 库不隐式起后台任务。
coro::Task<> watchdog(coro::Timer* idle, int* ticks, std::chrono::milliseconds period) {
    while (true) {
        const bool expired = co_await idle->wait();
        if (!expired) {
            std::cout << "[watchdog] 被 cancel 唤醒, 退出" << std::endl;
            co_return;
        }
        ++*ticks;
        std::cout << "[watchdog] 空闲到期 #" << *ticks << std::endl;
        idle->reset(period); // 不重新登记下一轮就不会再到期
    }
}

// 长任务: 只在自己的检查点响应取消, 不依赖外部强杀
coro::Task<> worker(coro::Context ctx) {
    for (int step = 0; step < 100; ++step) {
        co_await coro::sleep(10ms);
        ctx.throw_if_cancelled(); // 取消→CancelledError, 超期→TimeoutError
        std::cout << "[worker] step " << step << std::endl;
    }
    co_return;
}

// 监督者: 等待上下文的终态, 并区分原因
coro::Task<> supervisor(coro::Context ctx) {
    try {
        co_await ctx.wait();
        std::cout << "[supervisor] 上下文正常结束" << std::endl;
    } catch (const coro::CancelledError&) {
        std::cout << "[supervisor] 上下文被取消" << std::endl;
    } catch (const coro::TimeoutError&) {
        std::cout << "[supervisor] 上下文超时" << std::endl;
    }
    co_return;
}

int main() {
    return coro::run([]() -> coro::Task<int> {
        // --- Timer ---
        coro::Timer idle{50ms};
        int ticks = 0;
        auto dog = coro::spawn(watchdog(&idle, &ticks, 50ms));
        co_await coro::sleep(120ms); // 大约跑到两三次到期
        idle.cancel();               // 唤醒等待者, wait() 返回 false
        co_await std::move(dog);
        std::cout << "--- Timer 共打点 " << ticks << " 次 ---" << std::endl;

        // --- Context ---
        coro::CancellationSource src;
        auto parent = coro::Context::from(src.token()).with_deadline(5s);
        auto child = parent.make_child(); // 继承取消关系与期限

        auto job = coro::spawn(worker(child));
        auto watch = coro::spawn(supervisor(child));
        co_await coro::sleep(80ms);
        src.cancel(); // 任意线程可调用; 这里同线程演示

        co_await std::move(watch);
        try {
            co_await std::move(job);
            std::cout << "[main] worker 自己跑完了" << std::endl;
        } catch (const coro::CancelledError&) {
            std::cout << "[main] worker 在检查点退出" << std::endl;
        }
        co_return 0;
    }());
}
