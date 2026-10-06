#pragma once

#include <exception>
#include <stdexcept>
#include <string>
#include <vector>

// ============================================================================
// coro 异常体系 — 库内所有异常的唯一定义点 (公共根 coro::Error)
// ============================================================================
//
// 收口约定:
//   - 本头是库异常的唯一事实源, 不依赖任何其他 coro 头 (分层第 0 层);
//     CancelledError/TimeoutError (原 task.hpp) 与 ClosedChannelError
//     (原 channel.hpp) 已迁入此处。
//   - 所有库异常继承公共根 coro::Error : std::runtime_error, 调用方可用
//     catch (const coro::Error&) 兜底库内错误, 不必逐个列举、也不会误捕
//     业务代码抛出的裸 std::runtime_error 之外的类型。
//   - 取消 (CancelledError) 是控制流信号而非故障: TaskGroup 不聚合它,
//     detached 任务的处理回调也忽略它 —— 业务代码不应把它当失败处理。
//
// 结构化并发 (TaskGroup) 中, 多个子任务可能同时失败。ExceptionGroup
// 把一组异常打包成单个异常抛出, 调用方可以用 exceptions() 逐个检查:
//
//   try {
//       co_await group.wait();
//   } catch (const coro::ExceptionGroup& eg) {
//       for (auto& e : eg.exceptions()) { /* 逐个处理 */ }
//   }
//
// 语义约定 (与 Python 一致):
//   - 即使只有一个异常也包装成 ExceptionGroup (调用方统一处理路径)
//   - CancelledError (组内取消引起) 不聚合进来
// ============================================================================

namespace coro {

    /// 库内所有异常的公共根。继承 std::runtime_error 保持既有
    /// catch (const std::runtime_error&) 代码的兼容性。
    class Error : public std::runtime_error {
      public:
        explicit Error(const std::string& message) : std::runtime_error(message) {}
        explicit Error(const char* message) : std::runtime_error(message) {}
    };

    // ============================================================================
    // CancelledError — 协程取消异常（类似 Python asyncio.CancelledError）
    // ============================================================================
    //
    //   当 Task::cancel() 被调用后，被取消的协程会在下一个 await 点
    //   收到此异常（注入协程体内，可 catch 做清理），任何 co_await 该
    //   Task 的协程也会在 await_resume 时收到此异常。
    //
    //   用法:
    //     try { co_await task; }
    //     catch (const CancelledError&) { /* 清理工作 */ }
    // ============================================================================
    struct CancelledError : Error {
        CancelledError() : Error("coroutine cancelled") {}
    };

    // ============================================================================
    // TimeoutError — 超时异常（wait_for 超时时抛出）
    // ============================================================================
    struct TimeoutError : Error {
        TimeoutError() : Error("operation timed out") {}
    };

    /// 通道已被关闭 (发送侧或接收侧), 与"取消""超时"区分开
    class ClosedChannelError : public Error {
      public:
        explicit ClosedChannelError(const std::string& what) : Error(what) {}
    };

    /// Promise producer disappeared before publishing a value/exception.
    /// Waiting Futures receive this instead of remaining suspended forever.
    class BrokenPromiseError : public Error {
      public:
        BrokenPromiseError() : Error("promise destroyed before completion") {}
    };

    /// 结构化并发容器被按非法顺序使用: wait() 之后又 spawn、或重复 wait()。
    /// 这属于调用顺序错误而非运行时故障, 单独成类型便于调用方精确断言。
    class StructuredConcurrencyError : public Error {
      public:
        explicit StructuredConcurrencyError(const std::string& message) : Error(message) {}
    };

    /// 流上"要求读满 N 字节, 但对端在凑够之前正常关闭"。
    /// 它必须与三种情况各自可区分: 对端正常 EOF(返回 nullopt/false)、IO 错误(-1 + 错误码)、
    /// 被取消(CancelledError)。把它们混成一个异常会让调用方的重试/丢弃决策失去依据。
    class IncompleteStreamError : public Error {
      public:
        IncompleteStreamError(size_t wanted, size_t got)
            : Error("stream ended after " + std::to_string(got) + " of " + std::to_string(wanted) + " byte(s)"),
              requested(wanted), received(got) {}

        size_t requested; ///< 本来要读多少
        size_t received;  ///< 实际拿到多少 (可能是 0)
    };

    /// 在缓冲上限内始终找不到分隔符。没它的话"按行/按分隔符读"在遇到一行超长的
    /// 恶意或损坏数据时会无界吃内存 —— 有界背压要求上限必须存在且可预期。
    class StreamOverflowError : public Error {
      public:
        explicit StreamOverflowError(std::size_t limit)
            : Error("delimiter not found within " + std::to_string(limit) + " buffered bytes"), limit(limit) {}

        std::size_t limit; ///< 当时生效的缓冲上限
    };

    class ExceptionGroup : public Error {
      public:
        explicit ExceptionGroup(std::vector<std::exception_ptr> exceptions)
            : Error(std::to_string(exceptions.size()) + " exception(s) in group"), exceptions_(std::move(exceptions)) {}

        /// 组内聚合的所有异常 (按发生先后排序)
        const std::vector<std::exception_ptr>& exceptions() const noexcept { return exceptions_; }

      private:
        std::vector<std::exception_ptr> exceptions_;
    };

} // namespace coro
