#pragma once

#include "event_source.hpp"

#ifdef _WIN32
// WIN32_LEAN_AND_MEAN / NOMINMAX 已由 event_source.hpp 单点定义
#include <winsock2.h>
#include <windows.h>
#include <ws2tcpip.h>
// timeBeginPeriod / timeEndPeriod: 把系统时钟分辨率抬到 1ms (需 winmm)
#include <timeapi.h>
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

        /// GQCSEx 对失败完成只给 OVERLAPPED.Internal (NTSTATUS), 而各 I/O 模块
        /// 消费的是 Win32/WSA 错误码。RtlNtStatusToDosError 正是 GQCS 内部使
        /// 用的那个转换函数; 动态取指针, 不为它增加链接依赖。
        inline int ntstatus_to_win32(long status) noexcept {
            using fn_t = ULONG(WINAPI*)(LONG);
            static fn_t fn = []() -> fn_t {
                HMODULE ntdll = GetModuleHandleW(L"ntdll.dll");
                if (ntdll == nullptr)
                    return nullptr;
                return reinterpret_cast<fn_t>(reinterpret_cast<void*>(GetProcAddress(ntdll, "RtlNtStatusToDosError")));
            }();
            if (fn == nullptr)
                return static_cast<int>(ERROR_OPERATION_ABORTED);
            return static_cast<int>(fn(static_cast<LONG>(status)));
        }

#endif

    } // namespace detail

    namespace net {

#ifdef _WIN32

        class IocpEventSource : public EventSource {
          public:
            static constexpr ULONG_PTR WAKE_KEY = 1; // 特殊完成键: 外部唤醒

            /// 单次 wait_for 最多取出的完成包数。批次越大, 相同负载下需要的
            /// 系统调用越少 (实测饱和 echo 场景每包由 1 次 GQCS 降为 1/128 次)。
            static constexpr ULONG DRAIN_BATCH = 128;

            IocpEventSource() : iocp_(CreateIoCompletionPort(INVALID_HANDLE_VALUE, nullptr, 0, 0)) {
                // GetQueuedCompletionStatus 的超时粒度跟随系统时钟 (缺省 15.6ms): 不抬高分辨率时
                // sleep(1ms) 实测平均 ~10ms、抖动最差 ~16ms, 整个定时器体系凭空慢一个数量级。
                // 与 libuv 在 Windows 上的做法一致: 事件源存活期间持住 1ms 分辨率。
                if (iocp_) {
                    timeBeginPeriod(1);
                    high_resolution_ = true;
                }
            }

            ~IocpEventSource() override {
                // 与构造配对: 只有真正抬高过分辨率才归还, 避免多 loop 时提前降回 15.6ms
                if (high_resolution_)
                    timeEndPeriod(1);
                if (iocp_)
                    CloseHandle(iocp_);
            }

            /// IOCP creation can fail under resource pressure or a restricted
            /// process token. EventLoop uses this to fall back to CVEventSource
            /// instead of later passing a null handle to GQCS.
            bool valid() const noexcept { return iocp_ != nullptr; }

            /// 原生完成端口句柄 (诊断与回归测试用: 可注入完成包验证收割逻辑)。
            /// 生产代码不需要它 —— 关联/提交均由各 I/O 模块封装。
            HANDLE native_handle() const noexcept { return iocp_; }

            /// 将 socket 关联到完成端口 (之后其异步操作都投递到此端口)
            bool associate(SOCKET s) {
                return iocp_ && CreateIoCompletionPort(reinterpret_cast<HANDLE>(s), iocp_, 0, 0) != nullptr;
            }

            /// 将任意句柄关联到完成端口: 文件 (FILE_FLAG_OVERLAPPED)、
            /// 命名管道、目录句柄 (FILE_FLAG_BACKUP_SEMANTICS) 等。
            /// fs / pipe / fs_watch 模块共用本入口。
            bool associate(HANDLE h) { return iocp_ && CreateIoCompletionPort(h, iocp_, 0, 0) != nullptr; }

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

                // 混合策略: 阻塞等待用 GQCS 取第一个包 (小批次时比 GQCSEx 开销低,
                // 且失败时直接给 WSA/Win32 错误码), 随后的排空用一次 GQCSEx 批量取 ——
                // 旧实现是"一包一次 GQCS", 饱和时每个完成都多一次系统调用。
                const BOOL ok = GetQueuedCompletionStatus(iocp_, &bytes, &key, &ov, ms);
                if (ov != nullptr) {
                    deliver(reinterpret_cast<detail::iocp_op*>(ov), ok ? 0 : static_cast<int>(GetLastError()), bytes);
                    drain_ready();
                    return 1;
                }
                if (ok && key == WAKE_KEY)
                    return 1; // 外部唤醒 (wake() 投递的假完成包)
                return 0;     // 超时: 由 EventLoop 处理定时器
            }

            void wake() override { PostQueuedCompletionStatus(iocp_, 0, WAKE_KEY, nullptr); }

          private:
            /// 交付一个已完成的 I/O 操作 (错误码已在调用点快照)
            void deliver(detail::iocp_op* op, int error, DWORD bytes) noexcept {
                --pending_ops_; // 挂起计数 -1
                op->error = error;
                op->transferred = bytes;
                if (op->continuation)
                    on_complete(op->continuation); // 交还给事件循环
            }

            /// 非阻塞排空已就绪的完成包: 一次系统调用最多取 DRAIN_BATCH 个。
            /// 剩余留到下一轮, 避免持续完成流饿死定时器与跨线程回调。
            void drain_ready() {
                OVERLAPPED_ENTRY entries[DRAIN_BATCH];
                ULONG count = 0;
                if (!GetQueuedCompletionStatusEx(iocp_, entries, DRAIN_BATCH, &count, 0, FALSE))
                    return;
                for (ULONG i = 0; i < count; ++i) {
                    const auto& entry = entries[i];
                    if (entry.lpOverlapped == nullptr) // 唤醒包: 本批已非空, 无需额外处理
                        continue;
                    // GQCSEx 失败时只给 NTSTATUS, 用与 GQCS 内部相同的转换还原错误码
                    const int error =
                        entry.Internal == 0 ? 0 : detail::ntstatus_to_win32(static_cast<long>(entry.Internal));
                    deliver(reinterpret_cast<detail::iocp_op*>(entry.lpOverlapped), error,
                            entry.dwNumberOfBytesTransferred);
                }
            }

            HANDLE iocp_;
            bool high_resolution_ = false;    // 构造期间是否成功抬高过时钟分辨率
            std::atomic<int> pending_ops_{0}; // 挂起的异步操作数
        };

#endif // _WIN32

    } // namespace net
} // namespace coro
