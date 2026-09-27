// channel_pipeline.cpp — coro::channel<T> 的生产/消费与关闭排空示例
//
// 演示四件事:
//   1. 有界通道做背压: 缓冲满时 send 挂起, 而不是无界堆积;
//   2. 多生产者共享一个 sender 句柄 (拷贝分发), 最后一个释放即关闭发送侧;
//   3. 接收侧"先排空存量、再拿到 nullopt(EOF)"的正确收尾循环;
//   4. move-only 元素可以直接放进通道 (值只在成功交接时被移动)。
#include <coro/coro.hpp>
#include <coro/channel.hpp>

#include <iostream>
#include <memory>
#include <vector>

coro::Task<> producer(coro::channel<int>::sender tx, int from, int to) {
    for (int i = from; i < to; ++i)
        co_await tx.send(i); // 通道满时在这里挂起 (背压), 不丢也不无限排队
    co_return;               // tx 析构: 本生产者的发送端释放
}

coro::Task<> produce_packets(coro::channel<std::unique_ptr<std::string>>::sender tx) {
    co_await tx.send(std::make_unique<std::string>("hello"));
    co_await tx.send(std::make_unique<std::string>("channel"));
    co_return;
}

int main() {
    return coro::run([]() -> coro::Task<int> {
        // --- 有界通道 + 多生产者 + EOF 收尾 ---
        auto ch = coro::channel<int>::bounded(2);
        auto rx = ch.make_receiver();

        std::vector<coro::Task<>> workers;
        workers.push_back(coro::spawn(producer(ch.make_sender(), 0, 3)));
        workers.push_back(coro::spawn(producer(ch.make_sender(), 3, 6)));

        long sum = 0;
        int eof = 0;
        while (true) {
            auto value = co_await rx.recv();
            if (!value.has_value()) { // 所有生产者停手且存量排空 → EOF
                eof = 1;
                break;
            }
            sum += *value;
        }
        for (auto& w : workers)
            co_await std::move(w);
        std::cout << "sum=" << sum << " eof=" << eof << std::endl;

        // --- move-only 元素 ---
        auto texts = coro::channel<std::unique_ptr<std::string>>::rendezvous();
        auto tr = texts.make_receiver();
        auto tp = coro::spawn(produce_packets(texts.make_sender()));
        std::string joined;
        while (auto packet = co_await tr.recv())
            joined += **packet + " ";
        co_await std::move(tp);
        std::cout << "packets: " << joined << std::endl;
        co_return 0;
    }());
}
