#pragma once

#include "event_loop.hpp"

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#define NOMINMAX
#include <winsock2.h>
#include <windows.h>
#elif defined(__linux__)
#include <cerrno>
#endif

#include <cerrno>

// ============================================================================
// coro::io — IO 公共底座: 错误模型 + 各 IO 模块 (net/fs/pipe/process) 共享的约定
// ============================================================================
//
// 错误约定 (所有 IO awaiter 统一):
//   - await_resume 返回 int: >= 0 传输字节数; -1 失败; 特殊语义 (如 read 的
//     0 = 对端关闭) 由各模块文档说明
//   - 失败时错误码通过两条通道暴露:
//       errno            = 平台无关的 errno 值 (strerror/perror 可用)
//       io::last_error() = 平台原生码 (Windows: WSA/Win32; Linux: errno),
//                          精确区分 WSAECONNRESET(10054) 与 ECONNRESET(105) 等
//   - last_error 是 thread_local: 与 errno 同样的每线程语义, 在 co_await
//     返回后立即读取有效 (不要跨 await 点保存)
//
// 历史问题: 旧版 net.hpp 直接 `errno = op.error` 赋 WSA 错误码, 而 WSA 码
// (10035/10054...) 与 CRT errno 值是完全不同的两套编码, strerror 得到乱码,
// `errno == EWOULDBLOCK` 等判断全部失效。本头文件提供 wsa_to_errno() 修复。
// ============================================================================

namespace coro {
#ifdef CORO_URING_ENABLED
    namespace detail {
        /// SQE 提交 + 挂起计数 (所有 io_uring IO 模块共用: net/fs/pipe/process)
        inline void uring_submit(net::UringEventSource* u, io_uring_sqe* sqe, detail::uring_op* op) {
            io_uring_sqe_set_data(sqe, op);
            int ret = io_uring_submit(u->handle());
            if (ret < 0) {
                // 提交失败: 不增加挂起计数, 立即报错并恢复协程
                op->result = ret;
                op->error = -ret;
                EventLoop::get().schedule(op->continuation);
                return;
            }
            u->op_start();
        }
    } // namespace detail
#endif

    namespace io {

        /// 每线程最近的 IO 错误码 (平台原生码, 0 = 无错误)
        inline thread_local int t_last_error = 0;

        /// 读取最近一次 IO 操作的原生错误码
        /// (Windows: WSA/Win32 码; Linux: errno 值)
        inline int last_error() noexcept {
            return t_last_error;
        }

        /// 清除错误码
        inline void clear_error() noexcept {
            t_last_error = 0;
        }

#ifdef _WIN32

        /// WSA / Win32 错误码 → CRT errno 值
        /// 覆盖网络与文件 IO 常见错误; 未映射的码返回 EIO
        inline int wsa_to_errno(int code) noexcept {
            switch (code) {
                case 0:
                    return 0;
                // Win32 / WSA 公共子集
                case ERROR_ACCESS_DENIED: // 5
                case WSAEACCES:           // 10013
                    return EACCES;
                case WSAEFAULT: // 10014
                    return EFAULT;
                case ERROR_INVALID_PARAMETER: // 87
                case WSAEINVAL:               // 10022
                    return EINVAL;
                case WSAEMFILE: // 10024
                    return EMFILE;
                case WSAEWOULDBLOCK: // 10035
                    return EAGAIN;
                case WSAEINPROGRESS: // 10036
                    return EBUSY;
                case WSAENOTSOCK: // 10038
                    return ENOTSOCK;
                case WSAEADDRINUSE: // 10048
                    return EADDRINUSE;
                case WSAEADDRNOTAVAIL: // 10049
                    return EADDRNOTAVAIL;
                case WSAEAFNOSUPPORT: // 10047
                    return EAFNOSUPPORT;
                case WSAENETDOWN: // 10050
                    return ENETDOWN;
                case WSAENETUNREACH: // 10051
                    return ENETUNREACH;
                case WSAENETRESET: // 10052
                    return ENETRESET;
                case WSAECONNABORTED: // 10053
                    return ECONNABORTED;
                case WSAECONNRESET: // 10054
                    return ECONNRESET;
                case WSAENOBUFS: // 10055
                    return ENOBUFS;
                case WSAESHUTDOWN: // 10058
                case WSAENOTCONN:  // 10057
                    return ENOTCONN;
                case WSAETIMEDOUT: // 10060
                    return ETIMEDOUT;
                case WSAECONNREFUSED: // 10061
                    return ECONNREFUSED;
                case WSAEHOSTDOWN:    // 10064
                case WSAEHOSTUNREACH: // 10065
                    return EHOSTUNREACH;
                case ERROR_OPERATION_ABORTED: // 995 (CancelIoEx 取消完成包)
                    return EINTR;             // MSVC errno 无 ECANCELED, 取消近似为被打断
                case ERROR_HANDLE_EOF:        // 38 (ReadFile 到达文件尾)
                    return 0;
                case ERROR_FILE_NOT_FOUND: // 2
                case ERROR_PATH_NOT_FOUND: // 3
                    return ENOENT;
                case ERROR_FILE_EXISTS:    // 80
                case ERROR_ALREADY_EXISTS: // 183
                    return EEXIST;
                case ERROR_DIR_NOT_EMPTY: // 145
                    return ENOTEMPTY;
                case ERROR_NOT_ENOUGH_MEMORY: // 8
                case ERROR_OUTOFMEMORY:       // 14
                    return ENOMEM;
                case ERROR_NO_MORE_FILES: // 18 (FindFirstFile/目录枚举结束)
                    return ENOENT;
                case ERROR_BROKEN_PIPE: // 109 (管道对端关闭)
                    return EPIPE;
                default:
                    return EIO;
            }
        }

#endif // _WIN32

        /// awaiter 内部统一的错误记录入口:
        /// 同时写 last_error (原生码) 与 errno (转换后的 errno 码)
        inline void set_error(int native_code) noexcept {
            t_last_error = native_code;
#ifdef _WIN32
            errno = wsa_to_errno(native_code);
#else
            errno = native_code;
#endif
        }

    } // namespace io
} // namespace coro
