#pragma once

#include <coro/coro.hpp>

// ============================================================================
// 测试辅助工具
// ============================================================================
//
// 约束: gtest 的 EXPECT_*/ASSERT_* 宏会在 void 函数里生成 `return;`,
//       而协程体内不允许 return —— 所以断言一律放在 TEST 函数里(非协程),
//       协程只负责执行逻辑并通过指针参数把结果带回。
//
// 捕获型协程 lambda 的闭包必须活到协程结束；临时闭包启动后立即销毁会让
// 协程帧中的 this 悬空。长期存活的闭包和非协程 factory lambda 均可使用。
// ============================================================================

namespace test_util {

    // 启动一个惰性 Task 并驱动事件循环直到完成。
    // factory 返回 Task (通常是非协程 lambda, 内部调用命名协程函数)。
    template <typename F> void run_task(F&& factory) {
        using task_t = std::invoke_result_t<F>;
        task_t t = factory();
        t.start();
        coro::EventLoop::get().run();
        // 不要让测试只验证“事件循环停止”: 根任务的未处理异常也必须
        // 重新抛出, 否则失败路径会被静默吞掉。
        t.take_result();
    }

} // namespace test_util
