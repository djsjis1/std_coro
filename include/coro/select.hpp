#pragma once

#include "channel.hpp"
#include "event_loop.hpp"
#include "task.hpp"

#include <atomic>
#include <chrono>
#include <coroutine>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

// ============================================================================
// coro::select — 多路等待: 只有一个分支能提交副作用 (计划 M3b)
// ============================================================================
//
// 为什么不能拿现成的 wait_any 拼: wait_any 的落选任务不会被取消, 会继续跑完,
// 于是"落选的 recv"其实已经从通道里取走了一个消息 —— 那不是 select, 是丢数据。
// 真正的 select 要求: 登记 → 选中 → 提交 → 撤销, 只有赢家消费消息或提交发送,
// 输家必须完整摘链并归还预留名额。
//
// 实现要点 (每一条都被 Channel 的教训逼出来的):
//   1. 仲裁先于副作用: probe → admit → commit。反过来 (先提交再回滚) 在"值直接
//      交给已挂起对侧"时收不回来, 会造成两个分支都完成副作用。
//   2. 等待节点带 commit_gate: channel 在真正交付前问闸门, 首个放行者赢; 输家
//      的节点不会被交付, 撤销即可, 无需回滚。
//   3. 状态全部放在堆上的 select_body 里 (shared_ptr): awaiter 是 co_await 的
//      临时量, 会被 await_transform 移动, 不能作为任何指针的落点。
//   4. 撤销放在 awaiter 析构兜底: 取消的 CancelledError 由框架的取消检查包装器
//      抛出, 绕过 await_resume。
//
// 用法:
//   auto r = co_await coro::select(
//       coro::recv_of(rx1),            // 分支 0: 结果在 r.value (nullopt = 该通道 EOF)
//       coro::recv_of(rx2),            // 分支 1
//       coro::send_of(tx, 42),         // 分支 2 (与 recv 共存, 元素类型必须一致)
//       coro::after(100ms),            // 分支 3: r.timed_out == true
//       coro::default_nowait());       // 分支 4: r.defaulted == true, 绝不挂起
//
// 第一版范围 (刻意划小): 分支接受 channel 的 send/recv、定时器与 default;
// 所有分支元素类型一致; 多个分支同时就绪时按轮转起点挑选, 不固定偏向第一个;
// 不支持嵌套 select 与任意 Task 竞速。
// ============================================================================

namespace coro {
    namespace detail {

        /// 一次 select 的仲裁与唤醒状态。继承 commit_gate: channel 在真正交付
        /// 元素前必须先问它, 保证"只有一个分支完成了副作用"。
        struct select_state : commit_gate {
            std::atomic<int> winner{-1};
            std::atomic<bool> settled{false};
            std::coroutine_handle<> handle{};
            EventLoop* owner = nullptr;

            /// 纯观察 (probe 只许用它): 本 select 还没成交就算有资格
            bool can_admit(int) const noexcept override { return winner.load() == -1; }

            /// 原子申领: 首个成交者获胜, 之后任何分支想成交都会被拒
            bool claim(int tag) noexcept override {
                int expected = -1;
                return winner.compare_exchange_strong(expected, tag);
            }

            int claimed() const noexcept { return winner.load(); }

            void wake_once() {
                bool expected = false;
                if (settled.compare_exchange_strong(expected, true) && owner != nullptr && handle)
                    owner->schedule(handle);
            }
        };

    } // namespace detail

    /// select 的结果: index 是赢家分支号 (按传参顺序从 0 起)
    template <typename T> struct select_result {
        int index = -1;
        std::optional<T> value; // recv 分支的载荷; nullopt 表示该通道已关闭 (EOF)
        bool timed_out = false; // 赢的是 after() 分支
        bool defaulted = false; // 赢的是 default_nowait()
    };

    /// 分支描述符 (工厂函数的返回值, 轻量, 只在构造 select 时使用)
    template <typename T> struct recv_spec {
        using value_type = T;
        typename channel<T>::receiver rx;
    };
    template <typename T> struct send_spec {
        using value_type = T;
        typename channel<T>::sender tx;
        T value;
    };
    struct timer_spec {
        using value_type = void; // 无元素载荷的分支 (定时器/default) 用 void 标记
        std::chrono::steady_clock::time_point deadline;
    };
    struct default_spec {
        using value_type = void;
    };

    /// 分支工厂 (必须按端点类型推导: channel<T>::receiver 是非推导上下文)
    template <typename Receiver> recv_spec<typename Receiver::value_type> recv_of(Receiver rx) {
        return recv_spec<typename Receiver::value_type>{std::move(rx)};
    }
    template <typename Sender, typename V> send_spec<typename Sender::value_type> send_of(Sender tx, V&& value) {
        using T = typename Sender::value_type;
        return send_spec<T>{std::move(tx), T(std::forward<V>(value))};
    }
    template <typename R, typename P> timer_spec after(std::chrono::duration<R, P> d) {
        return timer_spec{std::chrono::steady_clock::now() + d};
    }
    inline default_spec default_nowait() {
        return default_spec{};
    }

    namespace detail {

        /// 归一后的运行期分支。recv/send 合成一种 (kind 区分), 避免异构容器。
        template <typename T> struct select_branch {
            bool is_send = false;
            int index = -1;
            typename channel<T>::receiver rx;
            typename channel<T>::sender tx;
            std::optional<T> outbound; // send 分支待提交的值
            std::shared_ptr<channel_receiver_waiter<T>> rnode;
            std::shared_ptr<channel_sender_waiter<T>> snode;

            /// 快速路径: probe → admit → commit (顺序不可颠倒)
            bool try_now(select_state& sel) {
                if (is_send) {
                    if (!tx.probe_can_accept())
                        return false;
                    if (!sel.claim(index))
                        return false;
                    if (!tx.commit_accept(*outbound)) {
                        sel.winner.store(-1); // 竞态窗口内状态变了: 归还仲裁权
                        return false;
                    }
                    return true;
                }
                std::optional<T> got;
                if (!rx.probe_has_message())
                    return false;
                if (!sel.claim(index))
                    return false;
                if (!rx.commit_take(got)) {
                    sel.winner.store(-1);
                    return false;
                }
                inbound = std::move(got);
                return true;
            }

            /// 挂起路径: 值/位置挂在带闸门的等待节点上
            void arm(select_state& sel) {
                if (is_send) {
                    snode = tx.arm_for_select(std::move(*outbound), &sel, index, sel.handle);
                } else {
                    rnode = rx.arm_for_select(&sel, index, sel.handle);
                }
            }

            /// 撤销: 未成交的分支摘链; 已成交的调用是空操作
            void disarm() {
                if (is_send) {
                    if (snode && !snode->taken)
                        outbound = std::move(snode->value); // 值随分支归还
                    if (snode)
                        tx.disarm_for_select(snode);
                } else if (rnode) {
                    rx.disarm_for_select(rnode);
                }
            }

            /// 是否已成交 (赢家或被闸门放行后交付)
            bool settled_now() const { return is_send ? (snode && snode->taken) : (rnode && rnode->delivered); }

            std::optional<T> inbound; // recv 快速路径取到的值
        };

        /// select 的全部可变状态, 放堆上: awaiter 只是临时量会被移动, 不能当落点
        template <typename T> struct select_body {
            select_state state;
            std::vector<select_branch<T>> branches;
            bool has_timer = false;
            int timer_index = -1;
            std::chrono::steady_clock::time_point deadline{};
            std::shared_ptr<std::atomic<bool>> timer_token;
            bool has_default = false;
            int default_index = -1;
            int rotation = 0; // 轮转起点: 多分支同时就绪时不总偏向第 0 个

            bool timer_expired() const { return has_timer && std::chrono::steady_clock::now() >= deadline; }
        };

        /// select 的挂起 awaiter: 只做登记与复检, 结果在 body 里
        template <typename T> struct select_awaiter {
            std::shared_ptr<select_body<T>> body;

            bool await_ready() { return false; } // 快速路径已在协程体内处理

            bool await_suspend(std::coroutine_handle<> h) {
                auto& b = *body;
                b.state.owner = &EventLoop::get();
                b.state.handle = h;
                for (auto& branch : b.branches)
                    branch.arm(b.state);
                if (b.has_timer) {
                    b.timer_token = std::make_shared<std::atomic<bool>>(false);
                    b.state.owner->schedule_timer(h, b.deadline, b.timer_token);
                }
                // 登记完成后复检: 期间可能有分支已被交付或定时器已到点
                if (b.state.claimed() >= 0 || b.timer_expired())
                    return false; // 立即恢复, 不真正挂起
                for (auto& branch : b.branches) {
                    if (branch.settled_now())
                        return false;
                }
                return true;
            }

            void await_resume() {}
        };

        template <typename T> Task<select_result<T>> run_select(std::shared_ptr<select_body<T>> b) {
            auto& state = b->state;

            /// 统一收尾, 覆盖所有 co_return 路径: 撤销未获胜分支的登记; 作废定时器
            /// token —— 不作废的话 loop 会一直等到 deadline 才能判定无事可做,
            /// 表现为"结果正确但 select 白挂了整个 deadline" (实测用例耗时 500ms)。
            struct settle_guard {
                std::shared_ptr<select_body<T>> body;
                ~settle_guard() {
                    for (auto& branch : body->branches)
                        branch.disarm();
                    if (body->timer_token)
                        *body->timer_token = true; // 已到期/未到期都无害
                }
            } guard{b};
            // ---- 快速路径: 轮转扫描, probe → admit → commit ----
            const std::size_t n = b->branches.size();
            for (std::size_t step = 0; step < n; ++step) {
                auto& branch = b->branches[static_cast<std::size_t>(b->rotation) % n];
                b->rotation = (b->rotation + 1) % static_cast<int>(n ? n : 1);
                if (branch.try_now(state)) {
                    select_result<T> r;
                    r.index = branch.index;
                    if (!branch.is_send)
                        r.value = std::move(branch.inbound);
                    co_return r;
                }
            }
            if (b->has_timer && b->timer_expired()) { // 0 时限的 after() 直接命中
                select_result<T> r;
                r.index = b->timer_index;
                r.timed_out = true;
                co_return r;
            }
            if (b->has_default) {
                select_result<T> r;
                r.index = b->default_index;
                r.defaulted = true;
                co_return r;
            }

            // ---- 慢速路径: 全部登记, 挂起等赢家 ----
            co_await select_awaiter<T>{b};

            select_result<T> r;
            const int winner = state.claimed();
            if (winner >= 0) {
                for (auto& branch : b->branches) {
                    if (branch.index == winner) {
                        r.index = winner;
                        if (branch.is_send) {
                            // 发送已提交 (值已离开本分支)
                        } else if (branch.rnode && branch.rnode->delivered) {
                            r.value = std::move(branch.rnode->value);
                        } else {
                            r.value = std::move(branch.inbound);
                        }
                        co_return r;
                    }
                }
                if (b->has_timer && winner == b->timer_index) {
                    r.index = winner;
                    r.timed_out = true;
                    co_return r;
                }
            }
            if (b->timer_expired()) {
                r.index = b->timer_index;
                r.timed_out = true;
                co_return r;
            }
            // 被关闭唤醒 (无赢家): 找一个已关闭的 recv 分支如实报告 EOF
            for (auto& branch : b->branches) {
                if (!branch.is_send && branch.rx.closed()) {
                    r.index = branch.index;
                    r.value = std::nullopt;
                    co_return r;
                }
            }
            co_return r; // 兜底: 不该走到 (index = -1)
        }

    } // namespace detail

    /// 多路等待入口。所有分支元素类型必须一致 (编译期检查)。
    template <typename First, typename... Rest>
    auto select(First first, Rest... rest) -> Task<select_result<typename First::value_type>> {
        using T = typename First::value_type;
        static_assert(!std::is_void_v<T>, "select 的第一个参数必须是 recv_of/send_of —— 由它决定元素类型");
        static_assert(
            ((std::is_void_v<typename Rest::value_type> || std::is_same_v<typename Rest::value_type, T>) && ...),
            "recv_of/send_of 分支的通道元素类型必须一致");

        auto body = std::make_shared<detail::select_body<T>>();
        int index = 0;
        auto add = [&](auto&& spec) {
            using S = std::decay_t<decltype(spec)>;
            if constexpr (std::is_same_v<S, recv_spec<T>>) {
                detail::select_branch<T> b;
                b.is_send = false;
                b.index = index++;
                b.rx = std::move(spec.rx);
                body->branches.push_back(std::move(b));
            } else if constexpr (std::is_same_v<S, send_spec<T>>) {
                detail::select_branch<T> b;
                b.is_send = true;
                b.index = index++;
                b.tx = std::move(spec.tx);
                b.outbound = std::move(spec.value);
                body->branches.push_back(std::move(b));
            } else if constexpr (std::is_same_v<S, timer_spec>) {
                body->has_timer = true;
                body->timer_index = index++;
                body->deadline = spec.deadline;
            } else {
                body->has_default = true;
                body->default_index = index++;
            }
        };
        add(std::move(first));
        (add(std::move(rest)), ...);
        return detail::run_select(std::move(body));
    }

} // namespace coro
