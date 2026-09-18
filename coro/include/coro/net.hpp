#pragma once

#include "event_loop.hpp"
#include "io.hpp"

#ifdef _WIN32
#include "iocp_event_source.hpp"
#elif defined(__linux__)
#include "uring_event_source.hpp"
#endif

#include <coroutine>

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#define NOMINMAX
#include <mswsock.h>
#include <winsock2.h>
#include <windows.h>
#include <ws2tcpip.h>
#elif defined(__linux__)
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

#include <atomic>
#include <cstring>
#include <string>
#include <utility>

// ============================================================================
// coro::net — IOCP 异步网络层 (Windows)
// ============================================================================
//
// 架构 (模仿 Python asyncio ProactorEventLoop):
//
//   ┌────────────────────────────────────────────────────┐
//   │ TcpListener / TcpStream (协程 API)                 │
//   │   co_await listener.accept()                       │
//   │   co_await stream.read(buf, n)                     │
//   │   co_await stream.write(buf, n)                    │
//   └───────────────────────┬────────────────────────────┘
//                           │ 每个操作 = iocp_op (OVERLAPPED)
//   ┌───────────────────────▼────────────────────────────┐
//   │ IOCP 完成端口 (EventLoop 的默认 EventSource)        │
//   │   wait_for  → GetQueuedCompletionStatus(timeout)   │
//   │   wake      → PostQueuedCompletionStatus (唤醒包)  │
//   └───────────────────────┬────────────────────────────┘
//                           │ 完成 → 恢复对应协程
//   ┌───────────────────────▼────────────────────────────┐
//   │ EventLoop (schedule + resume, 与定时器统一调度)     │
//   └────────────────────────────────────────────────────┘
//
// 零配置: EventLoop 在 Windows 上默认使用 IOCP, 网络 API
// 自动关联 socket 到完成端口, 用户不需要任何初始化代码。
//
// 生命周期约定 (同 Task): 操作挂起期间, awaiter 必须保持存活
// (它在协程帧中, 协程挂起即存活; 不要在操作进行中销毁协程)。
// ============================================================================

namespace coro {
    namespace net {

#ifdef _WIN32

        // ==================================================================
        // Winsock 初始化 (幂等, 自动 WSAStartup)
        // ==================================================================
        inline void ensure_winsock() {
            static bool done = []() {
                WSADATA data;
                WSAStartup(MAKEWORD(2, 2), &data);
                return true;
            }();
            (void)done;
        }

        /// 获取当前事件循环的 IOCP 事件源 (供 awaiter 统计挂起操作)。
        /// EventLoop 构造时已缓存类型化指针 —— 这里只是一次指针读取,
        /// 替代旧的 dynamic_cast (每操作一次 RTTI 层级行走)。
        inline IocpEventSource* current_iocp() {
            return EventLoop::get().iocp();
        }

        // ==================================================================
        // TcpStream — 已连接的 TCP 套接字 (异步 read/write)
        // ==================================================================
        class TcpStream {
          public:
            TcpStream() = default;

            /// 从原生 socket 构造并自动关联 IOCP (由 accept/connect 内部调用)。
            /// attach_iocp=false: 延迟关联, 供多线程服务器把连接投递到
            /// worker 线程后由 reattach() 在目标线程首次关联
            /// (CreateIoCompletionPort 不支持对已关联句柄转移关联)。
            explicit TcpStream(SOCKET s, bool attach_iocp = true) : sock_(s) {
                if (sock_ != INVALID_SOCKET && attach_iocp)
                    if (auto* iocp = current_iocp())
                        iocp->associate(sock_);
            }

            ~TcpStream() {
                if (sock_ != INVALID_SOCKET)
                    closesocket(sock_);
            }

            TcpStream(TcpStream&& other) noexcept : sock_(std::exchange(other.sock_, INVALID_SOCKET)) {}

            TcpStream& operator=(TcpStream&& other) noexcept {
                if (this != &other) {
                    if (sock_ != INVALID_SOCKET)
                        closesocket(sock_);
                    sock_ = std::exchange(other.sock_, INVALID_SOCKET);
                }
                return *this;
            }

            TcpStream(const TcpStream&) = delete;
            TcpStream& operator=(const TcpStream&) = delete;

            // ---- 异步读 ----

            /// co_await stream.read(buf, size) → 返回实际读取字节数 (0=对端关闭)
            struct read_awaiter {
                TcpStream* stream;
                char* buf;
                size_t len;
                detail::iocp_op op;
                WSABUF wsa_buf;

                bool await_ready() noexcept { return false; }

                /// 取消挂起的读 (Task::cancel 的取消钩子):
                /// CancelIoEx 产生 ERROR_OPERATION_ABORTED 完成包唤醒协程,
                /// 保证 OVERLAPPED 在协程帧销毁前被完成包消费 (安全)。
                static void cancel_op(void* self) {
                    auto* aw = static_cast<read_awaiter*>(self);
                    if (aw->stream && aw->stream->valid())
                        CancelIoEx(reinterpret_cast<HANDLE>(aw->stream->sock_), &aw->op.ov);
                }

                void await_suspend(std::coroutine_handle<> h) {
                    op.continuation = h;
                    wsa_buf.len = (ULONG)len;
                    wsa_buf.buf = buf;
                    DWORD flags = 0;
                    DWORD received = 0;
                    int rc = WSARecv(stream->sock_, &wsa_buf, 1, &received, &flags, &op.ov, nullptr);
                    if (rc == SOCKET_ERROR && WSAGetLastError() != WSA_IO_PENDING) {
                        // 立即失败: 不会有 IOCP 通知, 手动恢复
                        op.error = WSAGetLastError();
                        EventLoop::get().schedule(h);
                    } else {
                        // 异步挂起 (WSA_IO_PENDING) 或同步完成 (rc==0):
                        // 两种情况 IOCP 都会投递完成通知, 统计挂起数
                        if (auto* iocp = current_iocp())
                            iocp->op_start();
                    }
                    // WSA_IO_PENDING → 等 IOCP 通知
                    // 同步成功 (rc==0) → IOCP 仍会投递完成包, 统一等通知
                }

                int await_resume() {
                    if (op.error) {
                        io::set_error(op.error); // errno=转换值, io::last_error()=原生码
                        return -1;
                    }
                    return (int)op.transferred;
                }
            };

            auto read(char* buf, size_t len) { return read_awaiter{this, buf, len, {}, {}}; }

            // ---- 异步连接 (客户端) ----

            /// 进程级 ConnectEx 函数指针 (WSAIoctl 获取一次, 全进程有效)。
            /// 旧实现每次 connect() 都发一次 WSAIoctl 系统调用。
            inline static LPFN_CONNECTEX get_connect_ex() {
                static LPFN_CONNECTEX fn = []() -> LPFN_CONNECTEX {
                    SOCKET s = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
                    if (s == INVALID_SOCKET)
                        return nullptr;
                    GUID guid = WSAID_CONNECTEX;
                    DWORD bytes = 0;
                    LPFN_CONNECTEX f = nullptr;
                    if (WSAIoctl(s, SIO_GET_EXTENSION_FUNCTION_POINTER, &guid, sizeof(guid), &f, sizeof(f), &bytes,
                                 nullptr, nullptr) != 0)
                        f = nullptr; // 获取失败: connect 立即报 WSAEINVAL
                    closesocket(s);
                    return f;
                }();
                return fn;
            }

            /// co_await TcpStream::connect(ip, port) → 连接成功返回 TcpStream
            struct connect_awaiter {
                SOCKET sock = INVALID_SOCKET;
                sockaddr_in addr{};
                detail::iocp_op op;
                LPFN_CONNECTEX connect_ex_ = nullptr; // 进程级缓存指针 (get_connect_ex)

                bool await_ready() noexcept { return false; }

                /// 取消挂起的连接 (同 read_awaiter::cancel_op)
                static void cancel_op(void* self) {
                    auto* aw = static_cast<connect_awaiter*>(self);
                    if (aw->sock != INVALID_SOCKET)
                        CancelIoEx(reinterpret_cast<HANDLE>(aw->sock), &aw->op.ov);
                }

                void await_suspend(std::coroutine_handle<> h) {
                    op.continuation = h;
                    if (sock == INVALID_SOCKET || !connect_ex_) {
                        // socket 创建/bind 失败 或 ConnectEx 指针获取失败:
                        // 不会投递完成包, 立即报错 (connect() 已记录具体错误码)
                        if (!op.error)
                            op.error = WSAEINVAL;
                        EventLoop::get().schedule(h);
                        return;
                    }
                    BOOL rc = connect_ex_(sock, (sockaddr*)&addr, sizeof(addr), nullptr, 0, nullptr, &op.ov);
                    if (rc) {
                        // 同步成功: ConnectEx 不会投递 IOCP 通知, 立即完成
                        EventLoop::get().schedule(h);
                    } else if (WSAGetLastError() != WSA_IO_PENDING) {
                        // 立即失败: 不会有 IOCP 通知, 手动恢复
                        op.error = WSAGetLastError();
                        EventLoop::get().schedule(h);
                    } else {
                        // WSA_IO_PENDING → 异步进行, 等 IOCP 通知
                        if (auto* iocp = current_iocp())
                            iocp->op_start();
                    }
                }

                TcpStream await_resume() {
                    if (op.error) {
                        closesocket(sock);
                        io::set_error(op.error); // 原生码见 io::last_error()
                        return TcpStream{};      // 连接失败, valid()==false
                    }
                    return TcpStream(sock); // 构造时自动关联 IOCP
                }
            };

            /// 创建 socket 并异步连接到 ip:port (零配置: 自动关联 IOCP)
            static connect_awaiter connect(const char* ip, unsigned short port) {
                ensure_winsock(); // 必须先初始化 Winsock

                connect_awaiter aw;
                aw.sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
                aw.addr.sin_family = AF_INET;
                aw.addr.sin_port = htons(port);
                aw.addr.sin_addr.s_addr = inet_addr(ip);
                if (aw.sock != INVALID_SOCKET) {
                    // ConnectEx 要求 socket 先绑定本地地址 (否则报 WSAEINVAL)
                    sockaddr_in local{};
                    local.sin_family = AF_INET;
                    local.sin_addr.s_addr = INADDR_ANY;
                    local.sin_port = 0;
                    if (bind(aw.sock, (sockaddr*)&local, sizeof(local)) == SOCKET_ERROR) {
                        // 绑定失败 (几乎不可能, 除非套接字耗尽): 关闭并走错误路径
                        closesocket(aw.sock);
                        aw.sock = INVALID_SOCKET;
                        aw.op.error = WSAGetLastError();
                    } else {
                        if (auto* iocp = current_iocp())
                            iocp->associate(aw.sock);
                        // ConnectEx 指针: 进程级缓存, 不再每次 WSAIoctl
                        aw.connect_ex_ = get_connect_ex();
                    }
                } else {
                    aw.op.error = WSAGetLastError();
                }
                return aw;
            }

            // ---- 异步写 ----

            struct write_awaiter {
                TcpStream* stream;
                const char* buf;
                size_t len;
                detail::iocp_op op;
                WSABUF wsa_buf;

                bool await_ready() noexcept { return false; }

                /// 取消挂起的写 (同 read_awaiter::cancel_op)
                static void cancel_op(void* self) {
                    auto* aw = static_cast<write_awaiter*>(self);
                    if (aw->stream && aw->stream->valid())
                        CancelIoEx(reinterpret_cast<HANDLE>(aw->stream->sock_), &aw->op.ov);
                }

                void await_suspend(std::coroutine_handle<> h) {
                    op.continuation = h;
                    wsa_buf.len = (ULONG)len;
                    wsa_buf.buf = const_cast<char*>(buf);
                    DWORD sent = 0;
                    int rc = WSASend(stream->sock_, &wsa_buf, 1, &sent, 0, &op.ov, nullptr);
                    if (rc == SOCKET_ERROR && WSAGetLastError() != WSA_IO_PENDING) {
                        // 立即失败: 不会有 IOCP 通知, 手动恢复
                        op.error = WSAGetLastError();
                        EventLoop::get().schedule(h);
                    } else {
                        if (auto* iocp = current_iocp())
                            iocp->op_start();
                    }
                }

                int await_resume() {
                    if (op.error) {
                        io::set_error(op.error);
                        return -1;
                    }
                    return (int)op.transferred;
                }
            };

            auto write(const char* buf, size_t len) { return write_awaiter{this, buf, len, {}, {}}; }

            // ---- 同步关闭 ----

            /// 将 socket 关联到「当前线程」事件循环的 IOCP。
            /// 多线程服务器用: accept_noattach() 出的连接未关联任何 IOCP,
            /// 迁移到 worker 线程后在工厂内调用本方法完成首次关联,
            /// I/O 完成包才会投递到 worker 的完成端口。
            void reattach() {
                if (sock_ != INVALID_SOCKET)
                    if (auto* iocp = current_iocp())
                        iocp->associate(sock_);
            }

            void close() {
                if (sock_ != INVALID_SOCKET) {
                    closesocket(sock_);
                    sock_ = INVALID_SOCKET;
                }
            }

            bool valid() const { return sock_ != INVALID_SOCKET; }

          private:
            SOCKET sock_ = INVALID_SOCKET;
        };

        // ==================================================================
        // TcpListener — 监听套接字 (异步 accept)
        // ==================================================================
        class TcpListener {
          public:
            TcpListener() = default;

            ~TcpListener() {
                if (sock_ != INVALID_SOCKET)
                    closesocket(sock_);
            }

            TcpListener(TcpListener&& other) noexcept : sock_(std::exchange(other.sock_, INVALID_SOCKET)) {}

            TcpListener& operator=(TcpListener&& other) noexcept {
                if (this != &other) {
                    if (sock_ != INVALID_SOCKET)
                        closesocket(sock_);
                    sock_ = std::exchange(other.sock_, INVALID_SOCKET);
                }
                return *this;
            }

            TcpListener(const TcpListener&) = delete;
            TcpListener& operator=(const TcpListener&) = delete;

            /// 绑定并监听 (同步, 一次性; 零配置: 自动关联 IOCP)
            bool bind_listen(const char* ip, unsigned short port) {
                ensure_winsock(); // 必须先初始化 Winsock

                sock_ = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
                if (sock_ == INVALID_SOCKET)
                    return false;

                sockaddr_in addr{};
                addr.sin_family = AF_INET;
                addr.sin_port = htons(port);
                addr.sin_addr.s_addr = inet_addr(ip);

                if (bind(sock_, (sockaddr*)&addr, sizeof(addr)) == SOCKET_ERROR)
                    return false;
                if (listen(sock_, SOMAXCONN) == SOCKET_ERROR)
                    return false;

                // 获取 AcceptEx 函数指针 (需要 WSAIoctl)
                GUID guid = WSAID_ACCEPTEX;
                DWORD bytes = 0;
                WSAIoctl(sock_, SIO_GET_EXTENSION_FUNCTION_POINTER, &guid, sizeof(guid), &accept_ex_,
                         sizeof(accept_ex_), &bytes, nullptr, nullptr);

                if (auto* iocp = current_iocp())
                    iocp->associate(sock_);
                return true;
            }

            /// co_await listener.accept() → 返回已连接的 TcpStream
            struct accept_awaiter {
                TcpListener* listener;
                detail::iocp_op op;
                SOCKET accepted = INVALID_SOCKET;
                /// false: 不自动关联 IOCP (多线程服务器延迟到 worker 线程
                /// reattach 首次关联; CreateIoCompletionPort 不能转移关联)
                bool attach = true;
                // AcceptEx 要求提供本地/远端地址缓冲区 (必须存活到完成)
                // SOCKADDR_STORAGE + 16 字节, 每个方向一份
                char addr_buf[2 * (sizeof(SOCKADDR_STORAGE) + 16)]{};
                static constexpr size_t ADDR_LEN = sizeof(SOCKADDR_STORAGE) + 16;

                bool await_ready() noexcept { return false; }

                /// 取消挂起的 accept (同 read_awaiter::cancel_op)
                static void cancel_op(void* self) {
                    auto* aw = static_cast<accept_awaiter*>(self);
                    if (aw->listener->sock_ != INVALID_SOCKET)
                        CancelIoEx(reinterpret_cast<HANDLE>(aw->listener->sock_), &aw->op.ov);
                }

                void await_suspend(std::coroutine_handle<> h) {
                    op.continuation = h;
                    accepted = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
                    if (!listener->accept_ex_) {
                        // WSAIoctl 获取 AcceptEx 失败: 不会投递完成包, 立即报错
                        op.error = WSAEINVAL;
                        closesocket(accepted);
                        accepted = INVALID_SOCKET;
                        EventLoop::get().schedule(h);
                        return;
                    }
                    DWORD received = 0;
                    BOOL rc = listener->accept_ex_(listener->sock_, accepted, addr_buf, 0, ADDR_LEN, ADDR_LEN,
                                                   &received, &op.ov);
                    if (rc) {
                        // 同步成功: AcceptEx 不会投递 IOCP 通知, 立即完成
                        EventLoop::get().schedule(h);
                    } else if (WSAGetLastError() != WSA_IO_PENDING) {
                        // 立即失败: 不会有 IOCP 通知, 手动恢复
                        op.error = WSAGetLastError();
                        closesocket(accepted);
                        accepted = INVALID_SOCKET;
                        EventLoop::get().schedule(h);
                    } else {
                        // WSA_IO_PENDING → 异步进行, 等 IOCP 通知
                        if (auto* iocp = current_iocp())
                            iocp->op_start();
                    }
                }

                TcpStream await_resume() {
                    if (op.error || accepted == INVALID_SOCKET) {
                        if (accepted != INVALID_SOCKET)
                            closesocket(accepted);
                        return TcpStream{}; // 无效流, valid()==false
                    }
                    // accept 出的 socket 按 attach 标志决定是否关联 IOCP
                    return TcpStream(accepted, attach);
                }
            };

            auto accept() { return accept_awaiter{this, {}, INVALID_SOCKET}; }

            /// 同 accept(), 但返回的连接不关联 IOCP:
            /// 多线程服务器用它把连接投递到 worker 线程, 在目标线程
            /// 调用 TcpStream::reattach() 完成关联 (首次关联)。
            auto accept_noattach() { return accept_awaiter{this, {}, INVALID_SOCKET, false}; }

            /// 同步关闭监听 (服务器停止用)。
            /// 挂起的 AcceptEx 会以 ERROR_OPERATION_ABORTED 完成包返回,
            /// accept() 立即返回无效流, 不会泄漏 OVERLAPPED。
            void close() {
                if (sock_ != INVALID_SOCKET) {
                    closesocket(sock_);
                    sock_ = INVALID_SOCKET;
                }
            }

          private:
            SOCKET sock_ = INVALID_SOCKET;
            LPFN_ACCEPTEX accept_ex_ = nullptr; // AcceptEx 函数指针
        };

#endif // _WIN32

#ifdef __linux__

        // ==================================================================
        // Linux io_uring 网络层 (结构对称于 Windows IOCP 层)
        // ==================================================================
        //
        // 与 IOCP 层的区别:
        //   - 每个操作直接提交 SQE (无需预先关联 fd)
        //   - cqe->res 即操作结果 (字节数 / 新 fd / 负 errno)
        //   - 无论同步/异步完成, 一定有 CQE
        //
        // 需要 liburing: sudo apt install liburing-dev
        // 注意: 需要 Linux 环境编译验证, 本仓库在 Windows 上开发。
        // ==================================================================

        /// 获取当前事件循环的 io_uring 事件源 (供 awaiter 提交操作)。
        /// EventLoop 构造时已缓存类型化指针 (同 current_iocp)。
        inline UringEventSource* current_uring() {
            return EventLoop::get().uring();
        }

        // 提交 SQE 并统计挂起数 (转发到 detail::uring_submit, 与 fs/pipe/process 共用)
        inline void uring_submit_op(UringEventSource* u, io_uring* ring, detail::uring_op* op, io_uring_sqe* sqe) {
            (void)ring;
            detail::uring_submit(u, sqe, op);
        }

        // ==================================================================
        // TcpStream — 已连接的 TCP 套接字 (异步 read/write)
        // ==================================================================
        class TcpStream {
          public:
            TcpStream() = default;
            explicit TcpStream(int fd) : fd_(fd) {}

            ~TcpStream() {
                if (fd_ >= 0)
                    ::close(fd_);
            }

            TcpStream(TcpStream&& other) noexcept : fd_(std::exchange(other.fd_, -1)) {}

            TcpStream& operator=(TcpStream&& other) noexcept {
                if (this != &other) {
                    if (fd_ >= 0)
                        ::close(fd_);
                    fd_ = std::exchange(other.fd_, -1);
                }
                return *this;
            }

            TcpStream(const TcpStream&) = delete;
            TcpStream& operator=(const TcpStream&) = delete;

            // ---- 异步读 ----

            /// co_await stream.read(buf, size) → 返回实际读取字节数 (0=对端关闭)
            struct read_awaiter {
                TcpStream* stream;
                char* buf;
                size_t len;
                detail::uring_op op;

                ~read_awaiter() { op.alive = false; }

                bool await_ready() noexcept { return false; }

                /// 取消挂起的读 (Task::cancel 的取消钩子):
                /// 提交 ASYNC_CANCEL, 原操作会收到 -ECANCELED 的 CQE 唤醒协程,
                /// 保证 op 在协程帧销毁前被 CQE 消费 (安全)。
                static void cancel_op(void* self) {
                    auto* aw = static_cast<read_awaiter*>(self);
                    if (auto* u = current_uring()) {
                        io_uring_sqe* sqe = io_uring_get_sqe(u->handle());
                        if (sqe) {
                            io_uring_prep_cancel(sqe, &aw->op, 0);
                            io_uring_submit(u->handle());
                        }
                        // sqe 获取失败 (ring 满): 原 op 最终仍会产生 CQE, 等它自然完成
                    }
                }

                void await_suspend(std::coroutine_handle<> h) {
                    op.continuation = h;
                    if (auto* u = current_uring()) {
                        io_uring_sqe* sqe = io_uring_get_sqe(u->handle());
                        if (!sqe) {
                            // 提交队列满: 无法提交, 立即报错
                            op.result = -ENOBUFS;
                            EventLoop::get().schedule(h);
                            return;
                        }
                        io_uring_prep_recv(sqe, stream->fd_, buf, len, 0);
                        uring_submit_op(u, u->handle(), &op, sqe);
                    } else {
                        // 没有 io_uring 事件源: 立即失败
                        op.result = -ENOTSUP;
                        EventLoop::get().schedule(h);
                    }
                }

                int await_resume() {
                    if (op.error) {
                        io::set_error(op.error);
                        return -1;
                    }
                    return op.result;
                }
            };

            auto read(char* buf, size_t len) { return read_awaiter{this, buf, len, {}}; }

            // ---- 异步写 ----

            struct write_awaiter {
                TcpStream* stream;
                const char* buf;
                size_t len;
                detail::uring_op op;

                ~write_awaiter() { op.alive = false; }

                bool await_ready() noexcept { return false; }

                /// 取消挂起的写 (同 read_awaiter::cancel_op)
                static void cancel_op(void* self) {
                    auto* aw = static_cast<write_awaiter*>(self);
                    if (auto* u = current_uring()) {
                        io_uring_sqe* sqe = io_uring_get_sqe(u->handle());
                        if (sqe) {
                            io_uring_prep_cancel(sqe, &aw->op, 0);
                            io_uring_submit(u->handle());
                        }
                    }
                }

                void await_suspend(std::coroutine_handle<> h) {
                    op.continuation = h;
                    if (auto* u = current_uring()) {
                        io_uring_sqe* sqe = io_uring_get_sqe(u->handle());
                        if (!sqe) {
                            op.result = -ENOBUFS;
                            EventLoop::get().schedule(h);
                            return;
                        }
                        io_uring_prep_send(sqe, stream->fd_, buf, len, 0);
                        uring_submit_op(u, u->handle(), &op, sqe);
                    } else {
                        op.result = -ENOTSUP;
                        EventLoop::get().schedule(h);
                    }
                }

                int await_resume() {
                    if (op.error) {
                        io::set_error(op.error);
                        return -1;
                    }
                    return op.result;
                }
            };

            auto write(const char* buf, size_t len) { return write_awaiter{this, buf, len, {}}; }

            // ---- 异步连接 (客户端) ----

            /// co_await TcpStream::connect(ip, port) → 连接成功返回 TcpStream
            struct connect_awaiter {
                int fd = -1;
                sockaddr_in addr{};
                detail::uring_op op;

                ~connect_awaiter() { op.alive = false; }

                bool await_ready() noexcept { return false; }

                /// 取消挂起的连接 (同 read_awaiter::cancel_op)
                static void cancel_op(void* self) {
                    auto* aw = static_cast<connect_awaiter*>(self);
                    if (auto* u = current_uring()) {
                        io_uring_sqe* sqe = io_uring_get_sqe(u->handle());
                        if (sqe) {
                            io_uring_prep_cancel(sqe, &aw->op, 0);
                            io_uring_submit(u->handle());
                        }
                    }
                }

                void await_suspend(std::coroutine_handle<> h) {
                    op.continuation = h;
                    if (auto* u = current_uring()) {
                        io_uring_sqe* sqe = io_uring_get_sqe(u->handle());
                        if (!sqe) {
                            op.result = -ENOBUFS;
                            EventLoop::get().schedule(h);
                            return;
                        }
                        io_uring_prep_connect(sqe, fd, (sockaddr*)&addr, sizeof(addr));
                        uring_submit_op(u, u->handle(), &op, sqe);
                    } else {
                        op.result = -ENOTSUP;
                        EventLoop::get().schedule(h);
                    }
                }

                TcpStream await_resume() {
                    if (op.error) {
                        ::close(fd);
                        io::set_error(op.error);
                        return TcpStream{}; // 连接失败, valid()==false
                    }
                    return TcpStream(fd);
                }
            };

            /// 创建 socket 并异步连接到 ip:port (零配置)
            static connect_awaiter connect(const char* ip, unsigned short port) {
                connect_awaiter aw;
                aw.fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
                aw.addr.sin_family = AF_INET;
                aw.addr.sin_port = htons(port);
                aw.addr.sin_addr.s_addr = inet_addr(ip);
                return aw;
            }

            // ---- 同步关闭 ----

            /// io_uring 无 socket 关联概念 (每次操作直接提交 SQE),
            /// 跨线程传递 fd 即可使用, 无需迁移。
            void reattach() {}

            void close() {
                if (fd_ >= 0) {
                    ::close(fd_);
                    fd_ = -1;
                }
            }

            bool valid() const { return fd_ >= 0; }

          private:
            int fd_ = -1;
        };

        // ==================================================================
        // TcpListener — 监听套接字 (异步 accept)
        // ==================================================================
        class TcpListener {
          public:
            TcpListener() = default;

            ~TcpListener() {
                if (fd_ >= 0)
                    ::close(fd_);
            }

            TcpListener(TcpListener&& other) noexcept : fd_(std::exchange(other.fd_, -1)) {}

            TcpListener& operator=(TcpListener&& other) noexcept {
                if (this != &other) {
                    if (fd_ >= 0)
                        ::close(fd_);
                    fd_ = std::exchange(other.fd_, -1);
                }
                return *this;
            }

            TcpListener(const TcpListener&) = delete;
            TcpListener& operator=(const TcpListener&) = delete;

            /// 绑定并监听 (同步, 一次性; 零配置)
            bool bind_listen(const char* ip, unsigned short port) {
                fd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
                if (fd_ < 0)
                    return false;

                sockaddr_in addr{};
                addr.sin_family = AF_INET;
                addr.sin_port = htons(port);
                addr.sin_addr.s_addr = inet_addr(ip);

                if (::bind(fd_, (sockaddr*)&addr, sizeof(addr)) < 0)
                    return false;
                if (::listen(fd_, SOMAXCONN) < 0)
                    return false;
                return true;
            }

            /// co_await listener.accept() → 返回已连接的 TcpStream
            struct accept_awaiter {
                TcpListener* listener;
                detail::uring_op op;

                ~accept_awaiter() { op.alive = false; }

                bool await_ready() noexcept { return false; }

                /// 取消挂起的 accept (同 read_awaiter::cancel_op)
                static void cancel_op(void* self) {
                    auto* aw = static_cast<accept_awaiter*>(self);
                    if (auto* u = current_uring()) {
                        io_uring_sqe* sqe = io_uring_get_sqe(u->handle());
                        if (sqe) {
                            io_uring_prep_cancel(sqe, &aw->op, 0);
                            io_uring_submit(u->handle());
                        }
                    }
                }

                void await_suspend(std::coroutine_handle<> h) {
                    op.continuation = h;
                    if (auto* u = current_uring()) {
                        io_uring_sqe* sqe = io_uring_get_sqe(u->handle());
                        if (!sqe) {
                            op.result = -ENOBUFS;
                            EventLoop::get().schedule(h);
                            return;
                        }
                        io_uring_prep_accept(sqe, listener->fd_, nullptr, nullptr, 0);
                        uring_submit_op(u, u->handle(), &op, sqe);
                    } else {
                        op.result = -ENOTSUP;
                        EventLoop::get().schedule(h);
                    }
                }

                TcpStream await_resume() {
                    if (op.error) {
                        io::set_error(op.error);
                        return TcpStream{}; // 无效流, valid()==false
                    }
                    return TcpStream(op.result); // cqe->res 即新连接的 fd
                }
            };

            auto accept() { return accept_awaiter{this, {}}; }

            /// io_uring 无 socket 关联概念, 与 accept() 等价 (接口对称,
            /// 方便多线程服务器按平台无差异地调用)。
            auto accept_noattach() { return accept_awaiter{this, {}}; }

            /// 同步关闭监听 (服务器停止用)。
            /// 挂起的 accept 会以 -ECANCELED 完成, accept() 立即返回无效流。
            void close() {
                if (fd_ >= 0) {
                    ::close(fd_);
                    fd_ = -1;
                }
            }

          private:
            int fd_ = -1;
        };

#endif // __linux__

    } // namespace net
} // namespace coro
