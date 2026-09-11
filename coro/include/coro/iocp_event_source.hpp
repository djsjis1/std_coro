#pragma once

#include "event_source.hpp"

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#define NOMINMAX // 防止 windows.h 的 max/min 宏破坏 std::chrono::milliseconds::max()
#include <winsock2.h>
#include <windows.h>
#include <ws2tcpip.h>
#endif

#include <atomic>
#include <chrono>
#include <coroutine>

// ============================================================================
// coro::net::IocpEventSource — Windows IOCP 等待原语 (平台默认事件源)
// ============================================================================
//
// 这是 EventLoop 在 Windows 上的默认等待原语 (等价 Python ProactorEventLoop):
//   - wait_for()  →  GetQueuedCompletionStatus: 定时器超时 / I/O 完成 / 外部唤醒
//   - wake()      →  PostQueuedCompletionStatus: 投递假完成包 (self-pipe 等价物)
//   - has_pending()→ 还有挂起的 I/O 操作, 事件循环不应退出
//
// 完成的协程通过 EventSource::on_complete() 交还事件循环 (成员回调,
// 不再依赖全局函数指针 —— 多线程构造 loop 时无数据竞争)。
//
// detail::iocp_op 是所有 IOCP 异步操作 (socket / 文件 / 管道 / 目录监视)
// 共享的操作状态结构, 放在 coro::detail 供各 IO 模块复用。
// ============================================================================

namespace coro {
    namespace detail {

#ifdef _WIN32

        // ---- 每个异步 I/O 操作的状态 ----
        // OVERLAPPED 必须是第一个成员: IOCP 完成时通过 ov 指针反查整个结构
        struct iocp_op {
            OVERLAPPED ov{};                        // 提交给系统的 OVERLAPPED
            std::coroutine_handle<> continuation{}; // 完成时要恢复的协程
            int error = 0;                          // 0=成功, 否则 Windows 错误码 (WSA/Win32 原生码)
            DWORD transferred = 0;                  // 实际传输字节数
        };

#endif

    } // namespace detail

    namespace net {

#ifdef _WIN32

        class IocpEventSource : public EventSource {
          public:
            static constexpr ULONG_PTR WAKE_KEY = 1; // 特殊完成键: 外部唤醒

            /// wait_for 单次等待后追加排空的每批最大完成包数
            static constexpr DWORD DRAIN_BATCH = 64;

            IocpEventSource() : iocp_(CreateIoCompletionPort(INVALID_HANDLE_VALUE, nullptr, 0, 0)) {}

            ~IocpEventSource() override {
                if (iocp_)
                    CloseHandle(iocp_);
            }

            /// 将 socket 关联到完成端口 (之后其异步操作都投递到此端口)
            void associate(SOCKET s) { CreateIoCompletionPort(reinterpret_cast<HANDLE>(s), iocp_, 0, 0); }

            /// 将任意句柄关联到完成端口: 文件 (FILE_FLAG_OVERLAPPED)、
            /// 命名管道、目录句柄 (FILE_FLAG_BACKUP_SEMANTICS) 等。
            /// fs / pipe / fs_watch 模块共用本入口。
            void associate(HANDLE h) { CreateIoCompletionPort(h, iocp_, 0, 0); }

            /// 标记一个异步操作开始 (提交 WSA_IO_PENDING 后调用)
            void op_start() { ++pending_ops_; }

            /// 是否还有挂起的 I/O 操作 (事件循环据此决定是否退出)
            bool has_pending() const override { return pending_ops_ > 0; }

            int wait_for(std::chrono::milliseconds timeout) override {
                DWORD bytes = 0;
                ULONG_PTR key = 0;
                OVERLAPPED* ov = nullptr;
                // GetQueuedCompletionStatus 的 timeout 是 DWORD 毫秒
                DWORD ms =
                    (timeout.count() > (long long)(INFINITE - 1)) ? INFINITE - 1 : static_cast<DWORD>(timeout.count());
                BOOL ok = GetQueuedCompletionStatus(iocp_, &bytes, &key, &ov, ms);

                if (ov) {
                    // 一个 I/O 操作完成了: 反查操作状态, 恢复协程。
                    // 随后用 0 超时继续排空已就绪的完成包 (批量收割):
                    // 一次 loop 迭代处理 N 个完成, 摊薄每迭代的锁开销。
                    // 不用 GetQueuedCompletionStatusEx: 它对失败完成只给
                    // OVERLAPPED.Internal 的 NTSTATUS, 需自行转换错误码,
                    // 而 GQCS 的 FALSE+GetLastError 语义已是正确的 WSA 码。
                    int completed = 0;
                    do {
                        complete_one(ov, ok, bytes);
                        ++completed;
                        ok = GetQueuedCompletionStatus(iocp_, &bytes, &key, &ov, 0);
                    } while (ov && completed < (int)DRAIN_BATCH);
                    return 1;
                }
                if (ok && key == WAKE_KEY)
                    return 1; // 外部唤醒 (wake() 投递的假完成包)
                return 0;     // 超时: 由 EventLoop 处理定时器
            }

            void wake() override { PostQueuedCompletionStatus(iocp_, 0, WAKE_KEY, nullptr); }

          private:
            /// 消费一个完成包: 填充 op 状态并交还事件循环
            void complete_one(OVERLAPPED* ov, BOOL ok, DWORD bytes) {
                auto* op = reinterpret_cast<detail::iocp_op*>(ov);
                --pending_ops_; // 挂起计数 -1
                op->error = ok ? 0 : GetLastError();
                op->transferred = bytes;
                if (op->continuation)
                    on_complete(op->continuation); // 交还给事件循环
            }

            HANDLE iocp_;
            std::atomic<int> pending_ops_{0}; // 挂起的异步操作数
        };

#endif // _WIN32

    } // namespace net
} // namespace coro
