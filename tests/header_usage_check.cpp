// tests/header_usage_check.cpp — 公共头的“代表性实例化”检查 (G02)
//
// 目的: 只 include 头文件不足以证明模板在能力开启的配置下真的可实例化。
// 这里用 decltype / static_assert 把关键模板签名钉住:
//   - 返回类型（Task<tuple<...>> / Task<vector<...>> / Task<bool> ...）
//   - move-only 结果、void 任务路径
//   - 各能力宏开启时才实例化的组件
// 本文件只在编译期生效, 不产生运行开销, 也不进入 coro_tests。

#include <coro/coro.hpp>
#include <coro/future.hpp>
#include <coro/gather.hpp>
#include <coro/pool.hpp>
#include <coro/queue.hpp>
#include <coro/sync.hpp>
#include <coro/task_group.hpp>
#include <coro/thread.hpp>
#include <coro/wait.hpp>

#if defined(CORO_HAS_CONCURRENCY_EXT) && CORO_HAS_CONCURRENCY_EXT
#include <coro/channel.hpp>
#include <coro/context.hpp>
#include <coro/rate_limit.hpp>
#include <coro/timer.hpp>
#endif

#if defined(CORO_HAS_HTTP_CLIENT) && CORO_HAS_HTTP_CLIENT
#include <coro/http_client.hpp>
#endif

#if defined(CORO_HAS_NET) && CORO_HAS_NET
#include <coro/process.hpp>
#include <coro/scheduler.hpp>
#endif

#include <chrono>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

namespace header_usage {

    using int_task = coro::Task<int>;
    using void_task = coro::Task<>;

    // ---- 组合等待: 返回类型就是契约 ----
    // gather 返回按值的 gather_awaiter (不是 Task); 结果类型由 await_resume 钉住,
    // void 位置在 tuple 中是 std::monostate。
    static_assert(std::is_same_v<decltype(coro::gather(std::declval<int_task>(), std::declval<int_task>())),
                                 coro::detail::gather_awaiter<int, int>>);
    static_assert(
        std::is_same_v<decltype(coro::gather(std::declval<int_task>(), std::declval<int_task>()).await_resume()),
                       std::tuple<int, int>>);
    static_assert(std::is_same_v<decltype(coro::wait_tasks(std::declval<std::vector<int_task>>())),
                                 coro::Task<std::vector<int>>>);
    static_assert(
        std::is_same_v<decltype(coro::wait_for(std::declval<coro::Task<void>>(), std::chrono::milliseconds(1))),
                       coro::Task<void>>);
    static_assert(std::is_same_v<decltype(coro::wait_until(std::declval<int_task>(),
                                                           std::declval<std::chrono::steady_clock::time_point>())),
                                 coro::Task<int>>);

    // ---- move-only / void 路径 ----
    static_assert(std::is_same_v<decltype(coro::gather(std::declval<coro::Task<std::unique_ptr<int>>>())),
                                 coro::detail::gather_awaiter<std::unique_ptr<int>>>);
    static_assert(
        std::is_same_v<decltype(coro::gather(std::declval<coro::Task<std::unique_ptr<int>>>()).await_resume()),
                       std::tuple<std::unique_ptr<int>>>);
    static_assert(
        std::is_same_v<decltype(coro::gather(std::declval<void_task>()).await_resume()), std::tuple<std::monostate>>);
    // wait_tasks 的 vector 版本只对非 void T 可实例化 (内部结果容器是
    // vector<optional<T>>, optional<void> 非法); void 任务集应使用 gather
    // (tuple 中以 monostate 占位) —— 此约束由实现保证, 无需在此断言。

    // ---- 同步原语与线程/执行器 ----
    static_assert(std::is_same_v<decltype(std::declval<coro::Lock&>().acquire()), coro::Lock::acquire_awaiter>);
    static_assert(
        std::is_same_v<decltype(std::declval<coro::Semaphore&>().acquire()), coro::Semaphore::acquire_awaiter>);
    static_assert(std::is_same_v<decltype(std::declval<coro::Queue<int>&>().get()), coro::Queue<int>::get_awaiter>);
    static_assert(std::is_same_v<decltype(coro::to_thread_lazy([] { return 1; })), coro::Task<int>>);
    static_assert(std::is_same_v<decltype(coro::to_thread([] { return 1; })), coro::Task<int>>);

    // ---- 资源池: lease 类型与移动语义 ----
    static_assert(std::is_move_constructible_v<coro::Pool<int>::lease_type>);
    static_assert(!std::is_copy_constructible_v<coro::Pool<int>::lease_type>);

#if defined(CORO_HAS_CONCURRENCY_EXT) && CORO_HAS_CONCURRENCY_EXT
    // ---- 并发扩展: 每个入口都被真正实例化一次 ----
    static_assert(std::is_same_v<decltype(std::declval<coro::Timer&>().wait()), coro::Task<bool>>);
    static_assert(std::is_same_v<decltype(std::declval<coro::Context&>().wait()), coro::Task<void>>);
    static_assert(std::is_same_v<decltype(coro::Context::with_timeout(std::chrono::milliseconds(1))), coro::Context>);
    static_assert(std::is_move_constructible_v<coro::channel<int>::receiver>);
    // receiver 是共享句柄语义: 可拷贝 (内部引用计数 +1), 与 Task 的独占所有权不同
    static_assert(std::is_copy_constructible_v<coro::channel<int>::receiver>);

    // 真正取一次地址, 强制成员模板实例化 (不执行)
    inline void instantiate_concurrency_entries() {
        auto timer = coro::Timer{std::chrono::milliseconds(1)};
        (void)&coro::Timer::wait;
        (void)&coro::Context::wait;
        (void)&coro::rate_limiter::acquire;
        (void)timer;
    }
#endif

#if defined(CORO_HAS_HTTP_CLIENT) && CORO_HAS_HTTP_CLIENT
    // 流式响应必须是 move-only, 且 read_chunk 返回 optional<string>
    static_assert(std::is_move_constructible_v<coro::HttpResponseStream>);
    static_assert(!std::is_copy_constructible_v<coro::HttpResponseStream>);
    static_assert(std::is_same_v<decltype(std::declval<coro::HttpResponseStream&>().read_chunk()),
                                 coro::Task<std::optional<std::string>>>);
    static_assert(std::is_same_v<decltype(std::declval<coro::HttpClient&>().stream(std::declval<coro::HttpRequest>())),
                                 coro::Task<coro::HttpResponseStream>>);
#endif

#if defined(CORO_HAS_NET) && CORO_HAS_NET
    // 进程 communicate 与 Scheduler::join 的签名
    static_assert(std::is_same_v<decltype(coro::process::communicate(std::declval<coro::process::Process&>())),
                                 coro::Task<coro::process::CommunicateResult>>);
    static_assert(std::is_same_v<decltype(std::declval<coro::Scheduler&>().join()), coro::Task<void>>);
#endif

} // namespace header_usage
