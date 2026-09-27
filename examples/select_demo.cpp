// select_demo.cpp — coro::select 多路等待示例
//
// 演示三个最常见的形态:
//   1. 两个通道谁先来消息就处理谁 (多路 recv);
//   2. 带超时兜底的等待: 限时等不到就走超时分支;
//   3. 非阻塞尝试: 有 default 分支时绝不挂起, 适合主循环轮询。
//
// 核心保证: 输家分支不会消费消息 —— 落选通道里的值原封不动留给后续接收。
#include <coro/channel.hpp>
#include <coro/coro.hpp>
#include <coro/select.hpp>

#include <iostream>

using namespace std::chrono_literals;

coro::Task<> fast_feeder(coro::channel<int>::sender tx) {
    co_await coro::sleep(20ms);
    co_await tx.send(1);
}

coro::Task<> slow_feeder(coro::channel<int>::sender tx) {
    co_await coro::sleep(80ms);
    co_await tx.send(2);
}

int main() {
    return coro::run([]() -> coro::Task<int> {
        // --- 形态 1: 多路 recv, 谁先到处理谁 ---
        auto fast = coro::channel<int>::bounded(1);
        auto slow = coro::channel<int>::bounded(1);
        auto f1 = coro::spawn(fast_feeder(fast.make_sender()));
        auto f2 = coro::spawn(slow_feeder(slow.make_sender()));
        auto rx_fast = fast.make_receiver();
        auto rx_slow = slow.make_receiver();

        int order_sum = 0;
        for (int round = 0; round < 2; ++round) {
            auto r = co_await coro::select(coro::recv_of(rx_fast), coro::recv_of(rx_slow));
            if (r.value)
                order_sum = order_sum * 10 + *r.value; // 期望 12: 快的先赢
        }
        co_await std::move(f1);
        co_await std::move(f2);
        std::cout << "order_sum=" << order_sum << std::endl;

        // --- 形态 2: 超时兜底 ---
        auto quiet = coro::channel<int>::bounded(1);
        auto r = co_await coro::select(coro::recv_of(quiet.make_receiver()), coro::after(30ms));
        std::cout << "timed_out=" << r.timed_out << std::endl;

        // --- 形态 3: default 分支, 绝不挂起 ---
        auto r2 = co_await coro::select(coro::recv_of(quiet.make_receiver()), coro::default_nowait());
        std::cout << "defaulted=" << r2.defaulted << std::endl;

        co_return (order_sum == 12 && r.timed_out && r2.defaulted) ? 0 : 1;
    }());
}
