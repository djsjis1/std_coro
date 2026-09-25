#pragma once

#include <coro/coro.hpp>

#include <functional>
#include <cstddef>
#include <stdexcept>
#include <utility>
#include <vector>

#include "http_types.h"

// ============================================================================
// middleware.h — 中间件执行机制: next_fn / middleware_chain
// ============================================================================
//
// 职责分工 (洋葱模型, 参考 Koa / aiohttp 的 await next()):
//   - 本文件只管「如何依次执行」: 索引递归驱动 + move-only next_fn
//   - router.h        管「匹配哪个 handler、挂了哪些中间件」(全局 use / 路由级)
//   - web_server.cpp  管「最终异常兜底」(500 / CancelledError 语义)
//
// 中间件签名:
//   前半段 = 请求侧处理 (可短路: 不调 next() 直接 co_return);
//   co_await next() 进入内层; 拿到响应后可再改写 (追加响应头等)。
//
// 生命周期约定 (违反属未定义行为, 本层不做运行期防逃逸):
//   1. middleware_chain 必须在消费它的协程内创建为「局部对象」, 并 co_await
//      到执行结束 —— Task 延迟执行, 禁止返回临时链的 run() 结果 (悬空)。
//   2. next_fn 只允许在「当前请求的中间件任务内」调用并等待完成。
//      move-only 只防复制; 把它移动进脱离请求生命周期的任务中延后调用时,
//      chain_/req_ 可能已悬空, 检查本身无法安全发现, 不承诺抛错。
// ============================================================================

namespace middleware {

    /// 链尾终端: 路由 handler 或 route_core 兜底 (由 router.h 适配)
    using terminal_fn = std::function<coro::Task<http_response>(http_request&)>;

    class middleware_chain; // 前置声明: next_fn 只以指针引用它

    // ------------------------------------------------------------------------
    // next_fn — 每次请求在栈上构造的轻量视图 (move-only)
    //
    // 「最多一次」语义: 短路时调用 0 次合法; 重复调用或调用 moved-from 对象
    // 抛 std::logic_error。operator() 是普通函数 (非协程): 返回任务前立刻
    // 检查并占用调用权 —— 若写成协程, 函数体延迟到任务启动才执行,
    // 两次调用会先各自拿到任务, 检查就失效了。
    // ------------------------------------------------------------------------
    class next_fn {
      public:
        next_fn(const next_fn&) = delete;
        next_fn& operator=(const next_fn&) = delete;

        /// 按值接管语义: 移动使源对象失效
        next_fn(next_fn&& other) noexcept
            : chain_(std::exchange(other.chain_, nullptr)), req_(std::exchange(other.req_, nullptr)),
              index_(other.index_), called_(std::exchange(other.called_, true)) {}

        /// 定义在 middleware_chain 之后 (类内无法调用不完整类型的成员)
        coro::Task<http_response> operator()();

      private:
        friend class middleware_chain;
        next_fn(middleware_chain& chain, http_request& req, size_t index) : chain_(&chain), req_(&req), index_(index) {}

        middleware_chain* chain_; // moved-from 置空 → 再调用立即报错
        http_request* req_;
        size_t index_;
        bool called_ = false;
    };

    /// 中间件: 接收请求 + move-only next。
    /// next_fn 作为「参数」按值传递不影响运行时注册 —— std::function
    /// 要求可复制的是它保存的中间件对象, 不是参数。
    using middleware_fn = std::function<coro::Task<http_response>(http_request&, next_fn)>;

    // ------------------------------------------------------------------------
    // middleware_chain — 一段中间件列表 (全局链或路由级链) 的索引递归驱动。
    // 不预组装 next 闭包: 每层调用时在调用方栈上构造 next_fn。
    // ------------------------------------------------------------------------
    class middleware_chain {
      public:
        middleware_chain(const std::vector<middleware_fn>& mws, terminal_fn terminal)
            : mws_(mws), terminal_(std::move(terminal)) {}

        /// 非协程: 直接把任务交回调用方 (第 index 层中间件或终端)
        coro::Task<http_response> run(size_t index, http_request& req) {
            if (index < mws_.size())
                return mws_[index](req, next_fn(*this, req, index + 1));
            return terminal_(req);
        }

      private:
        friend class next_fn;
        const std::vector<middleware_fn>& mws_;
        terminal_fn terminal_;
    };

    // middleware_chain 已完整, 此处定义 next_fn::operator()
    inline coro::Task<http_response> next_fn::operator()() {
        if (chain_ == nullptr || called_)
            throw std::logic_error("next() already consumed");
        called_ = true;
        return chain_->run(index_, *req_);
    }

} // namespace middleware
