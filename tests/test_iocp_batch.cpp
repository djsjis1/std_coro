#include <coro/coro.hpp>
#include <gtest/gtest.h>

// ============================================================================
// IOCP 完成包收割回归 (仅 Windows)
// ============================================================================
//
// 回归目标: wait_for() 的批量排空不得“多取一个包却不处理”。
//   旧实现在处理完第 DRAIN_BATCH(64) 个完成后仍先调用一次
//   GetQueuedCompletionStatus, 再把取到的第 65 个包直接丢弃 ——
//   对应协程永远收不到完成, pending 计数也无法归零。
//
// 本测试直接向完成端口注入 64/65/128/129 个完成包, 断言:
//   1) 每个包恰好被消费一次 (计数 == 注入数);
//   2) has_pending() 最终回到 false (挂起计数归零)。
// ============================================================================

#ifdef _WIN32

namespace {

    struct BatchCounter {
        int completions = 0;
    };

    void count_completion(void* ctx, std::coroutine_handle<> /*h*/) {
        static_cast<BatchCounter*>(ctx)->completions++;
    }

    /// 注入 n 个完成包并驱动事件源排空; 返回总共消费的完成数。
    int drain_completions(coro::net::IocpEventSource& src, int n) {
        BatchCounter counter;
        src.set_completion_handler(&counter, &count_completion);

        // 操作状态必须稳定存活到完成包被消费之后 (与真实 I/O 相同约束)
        std::vector<std::unique_ptr<coro::detail::iocp_op>> ops;
        ops.reserve(static_cast<size_t>(n));
        for (int i = 0; i < n; ++i) {
            auto op = std::make_unique<coro::detail::iocp_op>();
            op->continuation = std::noop_coroutine();
            if (src.native_handle() == nullptr || !PostQueuedCompletionStatus(src.native_handle(), 0, 0, &op->ov)) {
                // 非 void 函数不能用 ASSERT_*: 记录失败并停止注入
                ADD_FAILURE() << "PostQueuedCompletionStatus failed";
                return counter.completions;
            }
            src.op_start();
            ops.push_back(std::move(op));
        }

        // 有界排空: 一旦丢失完成包, pending 永远不会归零, 这里会失败而不是挂死
        for (int guard = 0; src.has_pending() && guard < n * 4 + 64; ++guard)
            src.wait_for(std::chrono::milliseconds(0));

        return counter.completions;
    }

} // namespace

TEST(IocpBatchTest, SixtyFourCompletionsAllConsumed) {
    coro::net::IocpEventSource src;
    ASSERT_TRUE(src.valid());
    EXPECT_EQ(drain_completions(src, 64), 64);
    EXPECT_FALSE(src.has_pending());
}

TEST(IocpBatchTest, SixtyFiveCompletionsAllConsumed) {
    coro::net::IocpEventSource src;
    ASSERT_TRUE(src.valid());
    // 边界: 第 65 个包正是旧实现丢掉的那一个
    EXPECT_EQ(drain_completions(src, 65), 65);
    EXPECT_FALSE(src.has_pending());
}

TEST(IocpBatchTest, OneHundredTwentyNineCompletionsAllConsumed) {
    coro::net::IocpEventSource src;
    ASSERT_TRUE(src.valid());
    EXPECT_EQ(drain_completions(src, 129), 129);
    EXPECT_FALSE(src.has_pending());
}

TEST(IocpBatchTest, WakePacketDoesNotConsumeIOBudget) {
    coro::net::IocpEventSource src;
    ASSERT_TRUE(src.valid());
    BatchCounter counter;
    src.set_completion_handler(&counter, &count_completion);

    // 混合 65 个真实完成与若干唤醒包: 唤醒包不携带 OVERLAPPED,
    // 不应被当成操作完成, 也不能打断其余完成包的收割。
    std::vector<std::unique_ptr<coro::detail::iocp_op>> ops;
    for (int i = 0; i < 65; ++i) {
        auto op = std::make_unique<coro::detail::iocp_op>();
        op->continuation = std::noop_coroutine();
        ASSERT_TRUE(PostQueuedCompletionStatus(src.native_handle(), 0, 0, &op->ov));
        src.op_start();
        ops.push_back(std::move(op));
    }
    src.wake();
    src.wake();

    for (int guard = 0; src.has_pending() && guard < 512; ++guard)
        src.wait_for(std::chrono::milliseconds(0));

    EXPECT_EQ(counter.completions, 65);
    EXPECT_FALSE(src.has_pending());
}

#endif // _WIN32
