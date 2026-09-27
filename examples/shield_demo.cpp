// shield_demo.cpp — cancellation_shield: 取消被**延后**, 而不是被吞掉
//
// 场景: "扣款后落账"这类步骤中途被取消会留下半提交状态。正确用法不是忽略取消,
// 而是让它等这一步做完再浮现 —— 用户的取消意愿最终仍会生效。
#include <coro/context.hpp>
#include <coro/coro.hpp>

#include <atomic>
#include <iostream>

using namespace std::chrono_literals;

coro::Task<> critical_section(coro::CancellationSource* src, std::atomic<int>* steps) {
    {
        auto guard = src->make_shield(); // 作用域内本源的取消被延后
        src->cancel();                   // 模拟"用户在这会儿按了取消"
        std::cout << "inside shield: cancelled=" << src->token().cancelled() << std::endl;
        for (int i = 0; i < 3; ++i) {
            co_await coro::sleep(5ms); // 不可中断的提交步骤
            steps->fetch_add(1);
        }
    } // 作用域退出 → 延后的取消在这里补发
    std::cout << "after shield:  cancelled=" << src->token().cancelled() << std::endl;

    try {
        auto ctx = coro::Context::from(src->token());
        co_await ctx.wait(); // 取消已生效: 这里立刻浮现
    } catch (const coro::CancelledError&) {
        std::cout << "cancellation surfaced after the critical section" << std::endl;
    }
    co_return;
}

int main() {
    std::atomic<int> steps{0};
    coro::run([](std::atomic<int>* n) -> coro::Task<> {
        coro::CancellationSource src;
        auto job = coro::spawn(critical_section(&src, n));
        co_await std::move(job);
    }(&steps));

    // 临界区的 3 步必须全部完成, 且取消最终浮现 (见上面的输出)
    std::cout << "steps_done=" << steps.load() << std::endl;
    return steps.load() == 3 ? 0 : 1;
}
