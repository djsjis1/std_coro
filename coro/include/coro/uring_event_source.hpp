#pragma once

#include "event_source.hpp"

#ifdef __linux__
#include <liburing.h>
#endif

#include <atomic>
#include <chrono>
#include <coroutine>
#include <cstring>
#include <mutex>
#include <unordered_set>

// ============================================================================
// coro::net::UringEventSource — Linux io_uring 等待原语 (平台默认事件源)
// ============================================================================
//
// 这是 EventLoop 在 Linux 上的默认等待原语 (等价 Python ProactorEventLoop):
//   - wait_for()  →  io_uring_wait_cqe_timeout: 定时器超时 / I/O 完成 / 外部唤醒
//   - wake()      →  提交一个 NOP SQE: 产生一个假 CQE 唤醒 (self-pipe 等价物)
//   - has_pending()→ 还有挂起的 I/O 操作, 事件循环不应退出
//
// 依赖 liburing (io_uring 官方用户态库, github.com/axboe/liburing):
//   Ubuntu/Debian:  sudo apt install liburing-dev
//   Fedora:         sudo dnf install liburing-devel
//
// 与 IOCP 的差异 (io_uring 更简单):
//   - 不需要把 fd 预先"关联"到某个端口, 每个操作直接提交 SQE
//   - SQE 的 user_data 字段存 uring_op* 指针 (对应 IOCP 的 OVERLAPPED 反查)
//   - 无论同步/异步完成, 一定有 CQE (没有 IOCP 的"同步完成不投递"问题)
//
// 注意: 本文件在 Windows 上不可编译, 需要 Linux 环境验证。
// ============================================================================

namespace coro {
    namespace detail {

#ifdef __linux__

        // ---- 每个异步 I/O 操作的状态 ----
        // user_data 字段保存此指针, CQE 到达时通过它找回协程。
        // 所有 io_uring 异步操作 (socket / 文件 / 管道 / inotify / poll) 共享。
        struct uring_op {
            std::coroutine_handle<> continuation{}; // 完成时要恢复的协程
            int result = 0;                         // 完成结果 (字节数, 负值=错误)
            int error = 0;                          // 错误码 (0=成功)
        };

#endif

    } // namespace detail

    namespace net {

#ifdef __linux__

        class UringEventSource : public EventSource {
          public:
            // 完成队列深度 256: 同时挂起的异步操作上限 (可按需调大)
            UringEventSource() { io_uring_queue_init(256, &ring_, 0); }

            ~UringEventSource() override { io_uring_queue_exit(&ring_); }

            /// 获取底层 ring (网络层提交 SQE 用)
            io_uring* handle() { return &ring_; }

            /// 标记一个异步操作开始 (提交 SQE 后调用)
            void op_start() { ++pending_ops_; }

            /// 注册 op 为存活 (awaiter 挂起时调用)
            void track_op(detail::uring_op* op) {
                std::lock_guard lock(tracked_mutex_);
                tracked_ops_.insert(op);
            }

            /// 注销 op (awaiter 析构时调用, 帧销毁后防止延迟 CQE 写已释放内存)
            void untrack_op(detail::uring_op* op) {
                std::lock_guard lock(tracked_mutex_);
                tracked_ops_.erase(op);
            }

            /// 是否还有挂起的 I/O 操作 (事件循环据此决定是否退出)
            bool has_pending() const override { return pending_ops_ > 0; }

            int wait_for(std::chrono::milliseconds timeout) override {
                // 先非阻塞地消费所有已就绪的 CQE
                int completed = 0;
                io_uring_cqe* cqe = nullptr;
                unsigned head = 0;
                while (io_uring_peek_batch_cqe(&ring_, &cqe, 1) > 0) {
                    process_cqe(cqe);
                    io_uring_cqe_seen(&ring_, cqe);
                    ++completed;
                    cqe = nullptr;
                }
                if (completed)
                    return 1;

                // 没有就绪的 CQE, 带超时等待 (定时器到点 / 新 CQE / 被唤醒)
                struct __kernel_timespec ts {};
                ts.tv_sec = timeout.count() / 1000;
                ts.tv_nsec = (timeout.count() % 1000) * 1000000L;

                int ret = io_uring_wait_cqe_timeout(&ring_, &cqe, &ts);
                if (ret == -ETIME)
                    return 0; // 超时: 由 EventLoop 处理定时器
                if (ret < 0)
                    return 0;

                process_cqe(cqe);
                io_uring_cqe_seen(&ring_, cqe);
                return 1;
            }

            void wake() override {
                // 提交一个 NOP 操作: 产生一个假 CQE 唤醒等待者
                // (等价 IOCP 的 PostQueuedCompletionStatus / Python 的 self-pipe)
                io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
                if (sqe) {
                    io_uring_prep_nop(sqe);
                    io_uring_sqe_set_data(sqe, nullptr); // nullptr = 唤醒标记
                    io_uring_submit(&ring_);
                }
            }

          private:
            void process_cqe(io_uring_cqe* cqe) {
                auto* op = static_cast<detail::uring_op*>(io_uring_cqe_get_data(cqe));
                if (!op)
                    return; // 唤醒包, 无事可做

                --pending_ops_; // 挂起计数 -1
                // 帧已销毁 (awaiter 析构 → untrack_op): 跳过写入, 防止 use-after-free
                {
                    std::lock_guard lock(tracked_mutex_);
                    if (tracked_ops_.find(op) == tracked_ops_.end())
                        return;
                }
                op->result = cqe->res;
                op->error = cqe->res < 0 ? -cqe->res : 0;
                if (op->continuation)
                    on_complete(op->continuation); // 交还给事件循环
            }

            io_uring ring_;
            std::atomic<int> pending_ops_{0}; // 挂起的异步操作数
            std::mutex tracked_mutex_;
            std::unordered_set<detail::uring_op*> tracked_ops_; // 存活的 op (帧未销毁)
        };

#endif // __linux__

    } // namespace net
} // namespace coro
