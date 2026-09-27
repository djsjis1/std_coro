#pragma once

#include "event_loop.hpp"
#include "exceptions.hpp"
#include "task.hpp"

#include <coroutine>
#include <cstddef>
#include <deque>
#include <memory>
#include <optional>
#include <utility>

// ============================================================================
// coro::channel<T> — loop-local 的有界通道 (对标 Go channel / asyncio 队列语义)
// ============================================================================
//
// 与既有 coro::Queue 的区别是刻意的, 两者合同不同, 因此不合并:
//   - Queue 无容量 0 的 rendezvous, 也没有发送/接收端句柄与"关闭后排空"的 EOF 语义;
//   - Queue 的 get/put 面向"任务完成计数"(task_done/join), Channel 面向"生命周期结束"。
//
// 用法:
//   auto ch = coro::channel<int>::bounded(8);
//   auto tx = ch.sender();      // 端点句柄, 可拷贝分发给多个生产者
//   auto rx = ch.receiver();
//   co_await tx.send(42);                     // 满则挂起
//   if (auto v = co_await rx.recv()) use(*v); // 关闭且排空后得到 nullopt
//
// 合同:
//   - 有界容量必须显式给出; 无界用 unbounded(), 容量 0 用 rendezvous() (只直接交接)。
//   - send 在通道关闭后失败 (抛 ClosedChannelError); recv 先排空已有缓冲再报 EOF。
//   - 支持 move-only 元素: 值只在"入队/交接"两处移动, 拷贝不会发生。
//   - 最后一个 sender 释放等价于关闭发送侧; 最后一个 receiver 释放让 send 失败并唤醒
//     挂起的发送者 —— 端点句柄不是可有可无的糖, 它们是关闭条件的依据。
//   - 取消安全: 等待者被取消或提前销毁时必须摘链, 已交接的值不得丢, 预留名额必须归还。
//   - loop-local: 只在创建它的 EventLoop 内使用, 不提供跨线程投递。
// ============================================================================

namespace coro {

    /// 通道已被关闭 (发送侧或接收侧), 与"取消""超时"区分开
    class ClosedChannelError : public std::runtime_error {
      public:
        explicit ClosedChannelError(const std::string& what) : std::runtime_error(what) {}
    };

    namespace detail {

        template <typename T> struct channel_state;

        /// 发送侧等待节点。放堆上并用 shared_ptr 当身份: 容器在 push/erase 后
        /// 不保证元素地址不变, 拿容器内地址当"我是谁"会是隐藏错误。
        template <typename T> struct channel_sender_waiter {
            T value;
            std::coroutine_handle<> handle{};
            bool taken = false; // 值已被搬走 = 交接成功
            explicit channel_sender_waiter(T v) : value(std::move(v)) {}
        };

        /// 接收侧等待节点。值必须存放在这个堆节点里, 不能放 awaiter 成员再传指针:
        /// awaiter 是 co_await 的临时量, 会被 Task 的 await_transform 以右值引用
        /// 持有甚至移动, 把它的成员地址交给发送方写就是悬空写入 (实测 move-only
        /// 元素在第一次 recv 处 SIGSEGV)。
        template <typename T> struct channel_receiver_waiter {
            std::optional<T> value;
            std::coroutine_handle<> handle{};
            bool delivered = false; // 区分"值已交到我手上"与"只是被唤醒去看 EOF"
        };

        template <typename T> struct channel_state {
            std::deque<T> buffer;
            std::deque<std::shared_ptr<channel_sender_waiter<T>>> senders;
            std::deque<std::shared_ptr<channel_receiver_waiter<T>>> receivers;
            size_t capacity = 0;      // 0 = rendezvous
            bool send_closed = false; // 显式 close 或最后一个 sender 释放
            bool all_receivers_gone = false;
            size_t live_senders = 0;
            size_t live_receivers = 0;
            EventLoop* owner = nullptr;
        };

    } // namespace detail

    namespace detail {

        template <typename T> inline bool channel_has_room(const channel_state<T>& st) {
            return st.capacity == SIZE_MAX || st.buffer.size() < st.capacity;
        }

        template <typename T> inline void channel_wake(const channel_state<T>& st, std::coroutine_handle<> h) {
            if (st.owner != nullptr && h)
                st.owner->schedule(h);
        }

        /// 直接交给已经挂起的接收者 (rendezvous 与"有等待者即交接"都走这里)。
        /// 参数必须是左值引用而**不能按值接收**: 按值会把值先 move 进来, 一旦没找到
        /// 接收者, 值就随参数析构消失, 调用方随后放进缓冲的是空对象 (move-only
        /// 元素实测崩在解引用空 unique_ptr)。只有确定要交接时才移动。
        template <typename T> inline bool channel_deliver(channel_state<T>& st, T& value) {
            if (st.receivers.empty())
                return false;
            std::shared_ptr<channel_receiver_waiter<T>> node = st.receivers.front();
            st.receivers.pop_front();
            node->value = std::move(value);
            node->delivered = true;
            channel_wake(st, node->handle);
            return true;
        }

        /// 接收侧直接从挂起的发送者手里取值: rendezvous (容量 0, 缓冲永远是空的)
        /// 以及"缓冲空但已有人正等着发"都靠这条路径成交。少了它, 先 send 后 recv
        /// 的顺序会让双方永远等对方 —— 交接必须是双向可达的。
        template <typename T> inline bool channel_take_pending_sender(channel_state<T>& st, std::optional<T>& into) {
            if (st.senders.empty())
                return false;
            std::shared_ptr<channel_sender_waiter<T>> node = st.senders.front();
            st.senders.pop_front();
            into = std::move(node->value);
            node->taken = true;
            channel_wake(st, node->handle);
            return true;
        }

        /// 缓冲腾出位置后, 按 FIFO 把挂着的发送者搬进缓冲 (不插队, 保证公平)
        template <typename T> inline void channel_promote_senders(channel_state<T>& st) {
            while (!st.senders.empty() && channel_has_room(st)) {
                std::shared_ptr<channel_sender_waiter<T>> node = st.senders.front();
                st.senders.pop_front();
                st.buffer.push_back(std::move(node->value));
                node->taken = true;
                channel_wake(st, node->handle);
            }
        }

        /// 关闭: 唤醒所有挂着的发送者 (它们会以 ClosedChannelError 结束),
        /// 缓冲里已有的值保留, 让接收侧继续排空后再 EOF。
        template <typename T> inline void channel_close(channel_state<T>& st) {
            st.send_closed = true;
            while (!st.senders.empty()) {
                std::shared_ptr<channel_sender_waiter<T>> node = st.senders.front();
                st.senders.pop_front();
                channel_wake(st, node->handle);
            }
            // 没有更多值会进来了: 空缓冲时的等待接收者直接拿到 EOF
            if (st.buffer.empty()) {
                while (!st.receivers.empty()) {
                    std::shared_ptr<channel_receiver_waiter<T>> node = st.receivers.front();
                    st.receivers.pop_front();
                    channel_wake(st, node->handle);
                }
            }
        }

        /// 接收侧全灭: 挂着的发送者必须失败退出, 不能永远等一个不会来的接收者
        template <typename T> inline void channel_note_no_receivers(channel_state<T>& st) {
            st.all_receivers_gone = true;
            channel_close(st);
        }

        /// 摘掉一个已取消/已销毁的发送节点 (值随节点析构而释放, 不会丢给半路读者)
        template <typename T>
        inline void channel_drop_sender(channel_state<T>& st, const std::shared_ptr<channel_sender_waiter<T>>& node) {
            for (auto it = st.senders.begin(); it != st.senders.end(); ++it) {
                if (*it == node) {
                    st.senders.erase(it);
                    break;
                }
            }
            // 走掉一个发送者可能腾出一个位置 (缓冲满时挂着的人被摘走)
            channel_promote_senders(st);
        }

        template <typename T>
        inline void channel_drop_receiver(channel_state<T>& st,
                                          const std::shared_ptr<channel_receiver_waiter<T>>& node) {
            for (auto it = st.receivers.begin(); it != st.receivers.end(); ++it) {
                if (*it == node) {
                    st.receivers.erase(it);
                    break;
                }
            }
        }

    } // namespace detail

    template <typename T> class channel {
      public:
        class sender;
        class receiver;

        /// 在创建它的 EventLoop 线程内使用 (loop-local, 不跨线程投递)
        explicit channel(size_t capacity) : state_(std::make_shared<detail::channel_state<T>>()) {
            state_->capacity = capacity;
            state_->owner = &EventLoop::get();
        }

        static channel bounded(size_t n) { return channel{n}; }
        static channel rendezvous() { return channel{0}; } // 只直接交接, 不缓存
        static channel unbounded() { return channel{SIZE_MAX}; }

        /// 发送端句柄: 可拷贝分发给多个生产者, 最后一个释放即关闭发送侧
        class sender {
          public:
            sender() = default;
            sender(const sender& other) : state_(other.state_) { bump(); }
            sender(sender&& other) noexcept : state_(std::move(other.state_)) {}
            sender& operator=(const sender& other) {
                if (this != &other) {
                    release();
                    state_ = other.state_;
                    bump();
                }
                return *this;
            }
            sender& operator=(sender&& other) noexcept {
                if (this != &other) {
                    release();
                    state_ = std::move(other.state_);
                }
                return *this;
            }
            ~sender() { release(); }

            /// 非阻塞发送: true = 已受理 (交接或入缓冲), false = 需要等待。
            /// 通道已关闭时抛 ClosedChannelError。
            bool try_send(T value) const {
                auto& st = *state_;
                if (st.send_closed || st.all_receivers_gone)
                    throw ClosedChannelError("send on closed channel");
                if (detail::channel_deliver(st, value)) // 未命中接收者时值仍归调用方
                    return true;
                if (detail::channel_has_room(st)) {
                    st.buffer.push_back(std::move(value));
                    return true;
                }
                return false;
            }

            /// 阻塞式发送 (协程): 满则挂起, 关闭则抛 ClosedChannelError
            Task<void> send(T value) const;

            /// 主动关闭发送侧: 已挂起的发送者以 ClosedChannelError 退出,
            /// 缓冲里的存量仍可被接收方排空 (先排空再 EOF)。
            void close() const { detail::channel_close(*state_); }

            bool valid() const noexcept { return static_cast<bool>(state_); }

          private:
            friend class channel;
            explicit sender(std::shared_ptr<detail::channel_state<T>> st) : state_(std::move(st)) { bump(); }
            void bump() {
                if (state_)
                    ++state_->live_senders;
            }
            void release() {
                if (!state_)
                    return;
                if (--state_->live_senders == 0)
                    detail::channel_close(*state_); // 最后发送端释放 = 发送侧关闭
                state_.reset();
            }
            std::shared_ptr<detail::channel_state<T>> state_;
        };

        /// 接收端句柄: 最后一个接收端释放会让发送侧失败并唤醒挂起的发送者
        class receiver {
          public:
            receiver() = default;
            receiver(const receiver& other) : state_(other.state_) { bump(); }
            receiver(receiver&& other) noexcept : state_(std::move(other.state_)) {}
            receiver& operator=(const receiver& other) {
                if (this != &other) {
                    release();
                    state_ = other.state_;
                    bump();
                }
                return *this;
            }
            receiver& operator=(receiver&& other) noexcept {
                if (this != &other) {
                    release();
                    state_ = std::move(other.state_);
                }
                return *this;
            }
            ~receiver() { release(); }

            /// 非阻塞接收: 拿到值返回 true 并填 out; 缓冲空 (无论是否关闭) 返回 false。
            /// 是否已 EOF 用 closed() 判断, 不要用"没拿到值"推断。
            bool try_recv(T& out) const {
                auto& st = *state_;
                if (st.buffer.empty()) {
                    std::optional<T> pending;
                    if (!detail::channel_take_pending_sender(st, pending))
                        return false;
                    out = std::move(*pending);
                    return true;
                }
                out = std::move(st.buffer.front());
                st.buffer.pop_front();
                detail::channel_promote_senders(st); // 腾出位置后放行挂起的发送者
                return true;
            }

            /// 阻塞式接收 (协程): 空则挂起; 先排空存量再返回 nullopt 表示 EOF
            Task<std::optional<T>> recv() const;

            bool closed() const noexcept { return state_->send_closed && state_->buffer.empty(); }
            size_t size() const noexcept { return state_->buffer.size(); }
            bool valid() const noexcept { return static_cast<bool>(state_); }

          private:
            friend class channel;
            explicit receiver(std::shared_ptr<detail::channel_state<T>> st) : state_(std::move(st)) { bump(); }
            void bump() {
                if (state_)
                    ++state_->live_receivers;
            }
            void release() {
                if (!state_)
                    return;
                if (--state_->live_receivers == 0)
                    detail::channel_note_no_receivers(*state_); // 不会再有人取: 让发送方失败退出
                state_.reset();
            }
            std::shared_ptr<detail::channel_state<T>> state_;
        };

        sender make_sender() { return sender{state_}; }
        receiver make_receiver() { return receiver{state_}; }

        /// 关闭发送侧 (等价于所有生产者停手): 存量仍可被排空
        void close() { detail::channel_close(*state_); }

        bool closed() const noexcept { return state_->send_closed; }
        size_t size() const noexcept { return state_->buffer.size(); }
        bool empty() const noexcept { return state_->buffer.empty(); }
        size_t capacity() const noexcept { return state_->capacity; }

      private:
        std::shared_ptr<detail::channel_state<T>> state_;
    };

    namespace detail {

        /// 发送侧 awaiter。持有 state 与自己的节点 (都是 shared_ptr), 因此等待期间
        /// 端点句柄被销毁也不会悬空; 等待协程本身是自由协程, 同理不捕获句柄的 this。
        template <typename T> struct channel_send_awaiter {
            std::shared_ptr<channel_state<T>> st;
            std::shared_ptr<channel_sender_waiter<T>> node;

            bool await_ready() {
                if (st->send_closed || st->all_receivers_gone)
                    return false; // 仍走挂起路径, 由 await_suspend 里的自检立刻醒回来
                if (channel_deliver(*st, node->value)) {
                    node->taken = true;
                    return true;
                }
                if (channel_has_room(*st)) {
                    st->buffer.push_back(std::move(node->value));
                    node->taken = true;
                    return true;
                }
                return false;
            }

            void await_suspend(std::coroutine_handle<> h) {
                auto& loop = EventLoop::get();
                node->handle = h;
                st->senders.push_back(node);
                if (st->send_closed || st->all_receivers_gone) {
                    // 竞态窗口: 关闭正好发生在挂链之前, 对方已看不到我们 -> 自己唤醒自己
                    channel_drop_sender(*st, node);
                    loop.schedule(h);
                }
            }

            void await_resume() {
                if (node->taken)
                    return;
                channel_drop_sender(*st, node); // 关闭路径: 值随节点析构释放
                throw ClosedChannelError("send on closed channel");
            }

            /// 摘链必须在这里兜底, 不能只写在 await_resume: 任务被取消时
            /// CancelledError 由 Task 的取消检查包装器抛出, **绕过本 awaiter 的
            /// await_resume**, 于是节点滞留在发送队列里 —— 随后别的接收者一到,
            /// 它的值会被 promote 进缓冲, 交给一个根本没等它的人。
            ~channel_send_awaiter() {
                if (st && node && !node->taken)
                    channel_drop_sender(*st, node);
            }
        };

        /// 接收侧 awaiter。**所有值一律经堆节点传递**: awaiter 是 co_await 的临时量,
        /// 会被 Task 的 await_transform 以右值引用持有甚至移动, 任何写它成员的做法
        /// (包括 await_suspend 里自己写) 都可能落在另一份副本上看不见。
        template <typename T> struct channel_recv_awaiter {
            std::shared_ptr<channel_state<T>> st;
            std::shared_ptr<channel_receiver_waiter<T>> node;

            /// 快速路径与挂起路径共用同一个出口: 落到堆节点上
            void land(std::optional<T> value) {
                node->value = std::move(value);
                node->delivered = true;
            }

            bool await_ready() {
                if (!st->buffer.empty()) {
                    std::optional<T> got = std::move(st->buffer.front());
                    st->buffer.pop_front();
                    land(std::move(got));
                    channel_promote_senders(*st); // 腾出的位置按 FIFO 放行挂起的发送者
                    return true;
                }
                std::optional<T> got;
                if (channel_take_pending_sender(*st, got)) { // 含 rendezvous 的关键路径
                    land(std::move(got));
                    return true;
                }
                return st->send_closed; // 已关闭且无存量: 直接 EOF
            }

            void await_suspend(std::coroutine_handle<> h) {
                auto& loop = EventLoop::get();
                node->handle = h;
                st->receivers.push_back(node);
                // 挂链后自检竞态: 期间被关闭, 或缓冲里刚好有了值
                if ((st->send_closed && st->buffer.empty()) || (!st->buffer.empty() && st->capacity > 0)) {
                    channel_drop_receiver(*st, node);
                    loop.schedule(h);
                    return;
                }
                if (st->buffer.empty()) {
                    std::optional<T> got;
                    if (channel_take_pending_sender(*st, got)) {
                        land(std::move(got));
                        loop.schedule(h);
                    }
                }
            }

            std::optional<T> await_resume() {
                if (node->delivered)
                    return std::move(node->value);
                return std::nullopt; // 被关闭/无人接收唤醒: EOF
            }

            /// 与发送侧同理: 取消绕过 await_resume, 必须由析构摘链, 否则发送方
            /// 会往一个已经不存在的接收节点上交接。
            ~channel_recv_awaiter() {
                if (st && node && !node->delivered)
                    channel_drop_receiver(*st, node);
            }
        };

        template <typename T> inline Task<void> channel_send(std::shared_ptr<channel_state<T>> st, T value) {
            co_await channel_send_awaiter<T>{std::move(st),
                                             std::make_shared<channel_sender_waiter<T>>(std::move(value))};
            co_return;
        }

        template <typename T> inline Task<std::optional<T>> channel_recv(std::shared_ptr<channel_state<T>> st) {
            co_return co_await channel_recv_awaiter<T>{std::move(st), std::make_shared<channel_receiver_waiter<T>>()};
        }

    } // namespace detail

    template <typename T> Task<void> channel<T>::sender::send(T value) const {
        return detail::channel_send(state_, std::move(value));
    }

    template <typename T> Task<std::optional<T>> channel<T>::receiver::recv() const {
        return detail::channel_recv(state_);
    }

} // namespace coro
