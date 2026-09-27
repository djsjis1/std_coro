#pragma once

#include "event_loop.hpp"
#include "exceptions.hpp"
#include "task.hpp"

#include <coroutine>
#include <cstddef>
#include <deque>
#include <functional>
#include <memory>
#include <utility>

// ============================================================================
// coro::Pool<T> / coro::PoolLease<T> — 通用异步资源池 (计划 M4d)
// ============================================================================
//
// 面向"建立成本高、可复用、会坏"的资源: TCP/Unix 连接、TLS 会话、后端客户端等。
// 工厂本身是协程 (Task<T>), 所以建连可以是真异步的:
//
//   coro::Pool<coro::net::TcpStream> pool(
//       []() -> coro::Task<coro::net::TcpStream> {
//           co_return co_await coro::net::TcpStream::connect("127.0.0.1", 8080);
//       },
//       coro::Pool<coro::net::TcpStream>::options{});
//
//   auto lease = co_await pool.acquire();     // 复用空闲或新建; 到上限则等待
//   if (!lease.valid()) throw ...;            // 池已关闭 → 无效句柄
//   use(*lease);
//   // 离开作用域自动归还; 连接已坏时 lease.discard() 让池少一个槽
//
// 四条合同 (逐条有测试):
//   1. **归还靠 RAII**: PoolLease 析构即归还, 不需要用户记得调用 release;
//      所以异常路径与提前 return 都不会漏还 (这是 detach 式手写归还的主要 bug 源)。
//   2. **池可以先于借出的句柄析构**: 状态放在 shared_ptr 的 pool_state 里, lease 与
//      等待者都持有它的引用 (与 task_registry / TcpServer 同一范式)。
//   3. **取消安全**: acquire 挂起时被取消, 等待节点由 awaiter **析构**摘除 —— 只写在
//      await_resume 里会因框架的取消检查包装器绕过 await_resume 而失效 (本项目反复
//      踩过的同一课)。
//   4. **不超发**: 同时存在的 lease 数不超过 max_agents; 达到上限时新请求排队, 而不是
//      偷偷多建一个连接。
//
// 范围 (有意划小): loop-local (与 channel/rate_limiter 一致, 不跨线程投递);
// 不做空闲 TTL 淘汰与健康探测 —— 那需要资源侧的"还能用吗"回调, 由使用者在拿到
// lease 后自行校验并 discard() 更诚实。
// ============================================================================

namespace coro {
    namespace detail {

        /// 池的共享状态。只在创建它的 EventLoop 线程上访问, 因此不加锁 (与 channel 同理)。
        template <typename T> struct pool_state {
            std::function<Task<T>()> factory;
            std::deque<T> idle;
            /// 等待队列存的是 shared_ptr 凭证而不是裸句柄: 取消/归还时按身份精确摘除,
            /// 不依赖 deque 元素地址 (地址在增删后会变, 拿它当身份是隐藏错误)。
            std::deque<std::shared_ptr<std::coroutine_handle<>>> waiters;
            std::size_t max_agents = 0; // 0 = 不限制
            std::size_t max_idle = 1;   // 空闲上限, 超出直接丢弃资源
            std::size_t in_use = 0;     // 已借出 + 正在建立的数
            bool closed = false;

            /// 有空位或不限并发时可以新建
            bool can_create() const noexcept { return !closed && (max_agents == 0 || in_use < max_agents); }

            /// 唤醒一个等待者 (由它自己重试; 这里不直接塞资源, 避免出现"给了又没拿"的歧义)
            void wake_one() {
                while (!waiters.empty()) {
                    auto node = waiters.front();
                    waiters.pop_front();
                    if (!node || !*node)
                        continue; // 已被摘除的空凭证
                    std::coroutine_handle<> h = *node;
                    *node = std::coroutine_handle<>{};
                    EventLoop::get().schedule(h);
                    return;
                }
            }
        };

        /// acquire 的 awaiter。node 是堆上的 shared_ptr: 它同时是"等待队列里的身份凭证",
        /// 因为 deque 元素地址在增删后不保证稳定, 拿地址当身份会是隐藏错误 (channel 踩过)。
        template <typename T> struct pool_acquire_awaiter {
            std::shared_ptr<pool_state<T>> st;
            std::shared_ptr<std::coroutine_handle<>> node; // 挂在 waiters 里的凭证
            bool ready = false;                            // 无需挂起即可推进

            pool_acquire_awaiter(std::shared_ptr<pool_state<T>> s) : st(std::move(s)) {}

            ~pool_acquire_awaiter() {
                // 取消路径绕过 await_resume, 所以摘链必须在这里兜底
                if (node && *node)
                    drop();
            }

            bool await_ready() noexcept {
                // 有现成的空闲资源, 或还能新建 → 不需要挂起
                if (!st->idle.empty()) {
                    ready = true;
                    return true;
                }
                if (st->can_create()) {
                    ready = true;
                    return true;
                }
                return false; // 池关闭且无空闲 → 也走这条, 由 await_resume 给出"无效"结果
            }

            /// 被唤醒后不做任何事: 回到 acquire_impl 的循环顶部重新竞争资源
            /// (可能已被别的等待者抢走, 所以必须重判而不是假定轮到自己)。
            void await_resume() const noexcept {}

            void await_suspend(std::coroutine_handle<> h) {
                if (st->closed && st->idle.empty()) {
                    EventLoop::get().schedule(h); // 已关闭: 立即恢复, await_resume 给无效句柄
                    return;
                }
                node = std::make_shared<std::coroutine_handle<>>(h);
                st->waiters.push_back(node);
            }

            void drop() {
                if (!node)
                    return;
                auto& q = st->waiters;
                for (auto it = q.begin(); it != q.end(); ++it) {
                    if (*it == node) { // 比 shared_ptr 本身: 与 deque 元素位置无关
                        q.erase(it);
                        break;
                    }
                }
                *node = std::coroutine_handle<>{};
            }
        };

    } // namespace detail

    template <typename T> class Pool;

    /// 借出句柄: 移动专属, 析构自动归还
    template <typename T> class PoolLease {
      public:
        PoolLease() = default;
        PoolLease(const PoolLease&) = delete;
        PoolLease& operator=(const PoolLease&) = delete;

        PoolLease(PoolLease&& other) noexcept
            : res_(std::move(other.res_)), st_(std::move(other.st_)), valid_(other.valid_) {
            other.valid_ = false;
        }

        PoolLease& operator=(PoolLease&& other) noexcept {
            if (this != &other) {
                release(); // 自我赋值前先把旧资源还回去
                res_ = std::move(other.res_);
                st_ = std::move(other.st_);
                valid_ = other.valid_;
                other.valid_ = false;
            }
            return *this;
        }

        /// 析构即归还 (RAII): 异常路径与提前 return 都不会漏还
        ~PoolLease() { release(); }

        bool valid() const noexcept { return valid_ && static_cast<bool>(st_); }
        explicit operator bool() const noexcept { return valid(); }

        /// 可变与只读两套访问器: 归还/丢弃只需要 const 读, 而使用资源可能要改状态
        /// (例如给连接发请求)。写成 const 返回 T& 在 optional 上编译不过。
        T& operator*() {
            ensure_valid();
            return *res_;
        }
        const T& operator*() const {
            ensure_valid();
            return *res_;
        }
        T* operator->() {
            ensure_valid();
            return &*res_;
        }
        const T* operator->() const {
            ensure_valid();
            return &*res_;
        }

        /// 资源已不可信 (连接被对端关掉、协议状态脏了): 不归还, 让池少一个成员。
        /// 忘记调用会让坏连接被下一个借用者拿到 —— 这是连接池最典型的故障模式。
        void discard() noexcept {
            if (!valid_ || !st_)
                return;
            auto st = st_;
            valid_ = false;
            res_.reset();
            st->in_use -= st->in_use > 0 ? 1 : 0;
            st->wake_one(); // 腾出的名额给排队者
            st_ = nullptr;
        }

      private:
        friend class Pool<T>;

        PoolLease(T&& resource, std::shared_ptr<detail::pool_state<T>> st)
            : res_(std::move(resource)), st_(std::move(st)), valid_(true) {}

        void ensure_valid() const {
            if (!valid() || !res_)
                throw StructuredConcurrencyError("PoolLease used after release/invalid lease");
        }

        void release() {
            if (!valid_ || !st_)
                return;
            auto st = st_;
            valid_ = false;
            if (res_) {
                if (st->closed || st->idle.size() >= st->max_idle) {
                    res_.reset(); // 池已关或空闲够多: 直接丢弃资源
                    st->in_use -= st->in_use > 0 ? 1 : 0;
                } else {
                    st->idle.push_back(std::move(*res_));
                    res_.reset();
                    st->in_use -= st->in_use > 0 ? 1 : 0;
                    st->wake_one(); // 有新空闲资源: 放行一个等待者
                }
            } else {
                st->in_use -= st->in_use > 0 ? 1 : 0;
                st->wake_one();
            }
            st_ = nullptr;
        }

        std::optional<T> res_{}; // T 可能不可默认构造, 所以用 optional 而不是 T + 哨兵
        std::shared_ptr<detail::pool_state<T>> st_{};
        bool valid_ = false;
    };

    /// 资源池
    template <typename T> class Pool {
      public:
        /// 工厂必须返回 Task<T>: 建连本身可以是异步的。
        using factory_fn = std::function<Task<T>()>;
        using lease_type = PoolLease<T>; ///< 便于测试/调用方书写, 也避免"模板当类型用"的冗长

        struct options {
            std::size_t max_agents = 8; ///< 同时存在的资源上限 (0 = 不限制)
            std::size_t max_idle = 4;   ///< 空闲保留数, 超出的归还即丢弃
        };

        Pool(factory_fn factory, options opts = {}) : st_(std::make_shared<detail::pool_state<T>>()) {
            if (!factory)
                throw StructuredConcurrencyError("Pool requires a non-null factory");
            st_->factory = std::move(factory);
            st_->max_agents = opts.max_agents;
            st_->max_idle = opts.max_idle == 0 ? 1 : opts.max_idle;
        }

        Pool(const Pool&) = delete;
        Pool& operator=(const Pool&) = delete;
        Pool(Pool&&) noexcept = default;
        Pool& operator=(Pool&&) noexcept = default;

        /// 析构: 标记关闭并丢弃空闲资源。借出中的 lease 仍安全 (状态是 shared_ptr)。
        ~Pool() {
            if (!st_)
                return;
            st_->closed = true;
            st_->idle.clear();
            while (!st_->waiters.empty())
                st_->wake_one(); // 让等待者自己看到"已关闭"并拿到无效句柄
        }

        /// 借一个资源: 优先复用空闲, 其次在额度内新建, 否则排队等待。
        Task<PoolLease<T>> acquire() { return acquire_impl(st_); }

        std::size_t idle_count() const noexcept { return st_->idle.size(); }
        std::size_t in_use_count() const noexcept { return st_->in_use; }
        std::size_t waiting_count() const noexcept { return st_->waiters.size(); }
        bool closed() const noexcept { return st_->closed; }

        /// 主动关闭: 拒绝新的 acquire, 丢弃空闲资源, 唤醒等待者
        void close() {
            st_->closed = true;
            st_->idle.clear();
            while (!st_->waiters.empty())
                st_->wake_one();
        }

      private:
        /// 自由协程 (不是成员协程): 帧只捕获 shared_ptr, 不捕获 Pool 的 this
        static Task<PoolLease<T>> acquire_impl(std::shared_ptr<detail::pool_state<T>> st) {
            for (;;) {
                if (!st->idle.empty()) {
                    T resource = std::move(st->idle.front());
                    st->idle.pop_front();
                    ++st->in_use; // 复用不增加总数: 它本来就在池里
                    co_return PoolLease<T>{std::move(resource), st};
                }
                if (st->closed)
                    co_return PoolLease<T>{}; // 池已关: 无效句柄, 由调用方判 valid()
                if (!st->can_create()) {
                    // 到上限: 排队。唤醒后回到循环顶部重新竞争 (可能已被别人抢走)
                    detail::pool_acquire_awaiter<T> aw{st};
                    co_await aw;
                    continue;
                }
                ++st->in_use; // 先占名额: 否则并发 acquire 会同时建出超额资源
                try {
                    T created = co_await st->factory();
                    co_return PoolLease<T>{std::move(created), st};
                } catch (...) {
                    --st->in_use; // 建连失败必须归还名额
                    st->wake_one();
                    throw; // 原样上抛: 是工厂的错, 不该被池改写成语义不明
                }
            }
        }

        std::shared_ptr<detail::pool_state<T>> st_;
    };

} // namespace coro
