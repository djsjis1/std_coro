#pragma once

#include <exception>
#include <stdexcept>
#include <string>
#include <vector>

// ============================================================================
// coro::ExceptionGroup — 聚合异常 (对标 Python 3.11 的 ExceptionGroup)
// ============================================================================
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

    /// Promise producer disappeared before publishing a value/exception.
    /// Waiting Futures receive this instead of remaining suspended forever.
    class BrokenPromiseError : public std::runtime_error {
      public:
        BrokenPromiseError() : std::runtime_error("promise destroyed before completion") {}
    };

    /// 结构化并发容器被按非法顺序使用: wait() 之后又 spawn、或重复 wait()。
    /// 这属于调用顺序错误而非运行时故障, 单独成类型便于调用方精确断言。
    class StructuredConcurrencyError : public std::runtime_error {
      public:
        explicit StructuredConcurrencyError(const std::string& message) : std::runtime_error(message) {}
    };

    /// 流上"要求读满 N 字节, 但对端在凑够之前正常关闭"。
    /// 它必须与三种情况各自可区分: 对端正常 EOF(返回 nullopt/false)、IO 错误(-1 + 错误码)、
    /// 被取消(CancelledError)。把它们混成一个异常会让调用方的重试/丢弃决策失去依据。
    class IncompleteStreamError : public std::runtime_error {
      public:
        IncompleteStreamError(size_t wanted, size_t got)
            : std::runtime_error("stream ended after " + std::to_string(got) + " of " + std::to_string(wanted) +
                                 " byte(s)"),
              requested(wanted), received(got) {}

        size_t requested; ///< 本来要读多少
        size_t received;  ///< 实际拿到多少 (可能是 0)
    };

    /// 在缓冲上限内始终找不到分隔符。没它的话"按行/按分隔符读"在遇到一行超长的
    /// 恶意或损坏数据时会无界吃内存 —— 有界背压要求上限必须存在且可预期。
    class StreamOverflowError : public std::runtime_error {
      public:
        explicit StreamOverflowError(std::size_t limit)
            : std::runtime_error("delimiter not found within " + std::to_string(limit) + " buffered bytes"),
              limit(limit) {}

        std::size_t limit; ///< 当时生效的缓冲上限
    };

    class ExceptionGroup : public std::runtime_error {
      public:
        explicit ExceptionGroup(std::vector<std::exception_ptr> exceptions)
            : std::runtime_error(std::to_string(exceptions.size()) + " exception(s) in group"),
              exceptions_(std::move(exceptions)) {}

        /// 组内聚合的所有异常 (按发生先后排序)
        const std::vector<std::exception_ptr>& exceptions() const noexcept { return exceptions_; }

      private:
        std::vector<std::exception_ptr> exceptions_;
    };

} // namespace coro
