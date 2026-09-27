// rate_limit_demo.cpp — coro::rate_limiter 令牌桶示例
//
// 演示四件事:
//   1. 桶初始满 → 允许等容量的一次性突发;
//   2. acquire() 不足时挂起到补充 (而不是失败), 因此总耗时能反映限流下界;
//   3. try_acquire() 用于降级路径 (拿不到就走备用逻辑), 批量申请是原子的;
//   4. retry_after() 供上层回 429 / Retry-After 之类的提示。
#include <coro/coro.hpp>
#include <coro/rate_limit.hpp>

#include <atomic>
#include <chrono>
#include <iostream>

using namespace std::chrono_literals;

coro::Task<> worker(coro::rate_limiter* rl, std::atomic<int>* passed, std::atomic<int>* degraded) {
    if (rl->try_acquire()) {
        passed->fetch_add(1);
        co_return;
    }
    // 降级路径: 不想等的请求直接走备用处理
    degraded->fetch_add(1);
    co_return;
}

int main() {
    return coro::run([]() -> coro::Task<int> {
        // --- 1) 突发容量与降级 ---
        coro::rate_limiter rl(3, 1s); // 容量 3, 每秒补 3
        std::atomic<int> passed{0};
        std::atomic<int> degraded{0};
        std::vector<coro::Task<>> batch;
        for (int i = 0; i < 8; ++i)
            batch.push_back(coro::spawn(worker(&rl, &passed, &degraded)));
        for (auto& w : batch)
            co_await std::move(w);
        std::cout << "burst: passed=" << passed.load() << " degraded=" << degraded.load() << std::endl;
        if (passed.load() != 3)
            co_return 1;

        // --- 2) acquire 会等, 而不是失败 ---
        coro::rate_limiter drip(2, 40ms); // 每 40ms 补 2
        (void)drip.try_acquire(2);        // 先清空
        auto start = std::chrono::steady_clock::now();
        co_await drip.acquire(); // 必须等到下一轮补充
        auto waited = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start);
        std::cout << "acquire waited " << waited.count() << "ms (>= 30ms 说明真在等)" << std::endl;
        if (waited < 30ms)
            co_return 2;

        // --- 3) 批量申请是原子的 ---
        coro::rate_limiter bulk(5, 1s);
        (void)bulk.try_acquire(4); // 只剩 1
        std::cout << "before batch: available=" << bulk.available() << std::endl;
        if (bulk.try_acquire(3))
            co_return 3; // 不该成功
        std::cout << "after failed batch: available=" << bulk.available() << " (不能被吃掉一半)" << std::endl;
        if (bulk.available() != 1)
            co_return 4;

        // --- 4) retry_after 供上层做 429 提示 ---
        std::cout << "retry_after=" << bulk.retry_after<std::chrono::milliseconds>(3).count() << "ms" << std::endl;
        std::cout << "ALL RATE LIMIT CASES PASSED" << std::endl;
        co_return 0;
    }());
}
