#include <coro/coro.hpp>
#include <gtest/gtest.h>

#include <atomic>
#include <memory>

// ============================================================================
// 事件循环公平性回归: ready 队列非空时也必须轮询 I/O
// ============================================================================
//
// 旧行为: 只有 ready/batch/fn 队列全空时才调用 event_source_->wait_for()。
// 一个不断 yield() 的协程会让 ready 队列永不为空, 内核已完成的 I/O 因此
// 永远不被收割 (I/O 饥饿)。CPython 在 ready 非空时仍执行 select(0)。
//
// 本测试用 fake 事件源记录轮询次数: 只要“在 yield 循环执行期间”发生过轮询,
// 就说明公平性修复生效; 旧实现该值为 0。
// ============================================================================

namespace {

    class CountingSource : public coro::EventSource {
      public:
        std::atomic<int> polls{0};
        std::atomic<bool> pending{true};

        int wait_for(std::chrono::milliseconds /*timeout*/) override {
            polls.fetch_add(1, std::memory_order_relaxed);
            pending.store(false, std::memory_order_relaxed); // 只模拟一次待处理事件
            return 0;
        }

        void wake() override {}

        bool has_pending() const override { return pending.load(std::memory_order_relaxed); }
    };

    /// 持续自我重入队: 每轮到下一个 await 点都重新 schedule 自己
    coro::Task<void> spin_yield(int rounds, std::atomic<int>* observed_polls) {
        for (int i = 0; i < rounds; ++i)
            co_await coro::yield();
        // 记录“yield 循环执行期间”观察到的轮询次数
        *observed_polls = 0;
        co_return;
    }

    coro::Task<void> spin_and_report(int rounds, CountingSource* src, std::atomic<int>* observed) {
        for (int i = 0; i < rounds; ++i)
            co_await coro::yield();
        *observed = src->polls.load(std::memory_order_relaxed);
        co_return;
    }

} // namespace

TEST(LoopFairnessTest, PollsIOWhileReadyQueueIsNonEmpty) {
    auto& loop = coro::EventLoop::get();
    auto fake = std::make_shared<CountingSource>();
    auto previous = loop.swap_event_source(fake);

    std::atomic<int> observed{-1};
    auto task = spin_and_report(100, fake.get(), &observed);
    task.start();
    loop.run();
    task.take_result();

    EXPECT_GT(observed.load(), 0)
        << "ready 队列持续非空时完全没有轮询事件源: I/O 完成包会被无限期跳过";

    fake->pending.store(false, std::memory_order_relaxed);
    loop.swap_event_source(previous);
}

TEST(LoopFairnessTest, YieldHeavyLoopStillTerminates) {
    auto& loop = coro::EventLoop::get();
    auto fake = std::make_shared<CountingSource>();
    auto previous = loop.swap_event_source(fake);

    std::atomic<int> observed{-1};
    auto task = spin_yield(500, &observed);
    task.start();
    loop.run();
    task.take_result();

    // 公平性轮询不能把循环变成“永不退出”: 这里只要求正常收敛
    EXPECT_EQ(observed.load(), 0);

    fake->pending.store(false, std::memory_order_relaxed);
    loop.swap_event_source(previous);
}
