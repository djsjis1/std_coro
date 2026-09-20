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
