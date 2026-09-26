// 纯核心消费者: 只使用协程/调度/计时设施 (coro::sleep 走 CVEventSource 即可),
// 不 include 任何 I/O 头, 用来验证 coro::core 独立可用。
#include <coro/coro.hpp>

#include <chrono>

coro::Task<int> delayed_answer() {
    co_await coro::sleep(std::chrono::milliseconds(1));
    co_return 42;
}

int main() {
    return coro::run(delayed_answer()) == 42 ? 0 : 1;
}
