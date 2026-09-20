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

        inline bool associate_current_iocp(SOCKET socket) {
            auto* iocp = current_iocp();
            if (iocp && iocp->associate(socket))
                return true;
            io::set_error(iocp ? (int)GetLastError() : (int)WSAEOPNOTSUPP);
            return false;
        }

        inline IocpEventSource* require_current_iocp(detail::iocp_op& op, std::coroutine_handle<> h) {
            if (auto* iocp = current_iocp())
                return iocp;
            op.error = WSAEOPNOTSUPP;
            EventLoop::get().schedule(h);
            return nullptr;
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
                if (sock_ != INVALID_SOCKET && attach_iocp && !associate_current_iocp(sock_))
                    close();
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
                    auto* iocp = require_current_iocp(op, h);
                    if (!iocp)
                        return;
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

                connect_awaiter() = default;
                ~connect_awaiter() {
                    if (sock != INVALID_SOCKET)
                        closesocket(sock);
                }
                connect_awaiter(connect_awaiter&& other) noexcept
                    : sock(std::exchange(other.sock, INVALID_SOCKET)), addr(other.addr), op(other.op),
                      connect_ex_(other.connect_ex_) {}
                connect_awaiter(const connect_awaiter&) = delete;
                connect_awaiter& operator=(const connect_awaiter&) = delete;
                connect_awaiter& operator=(connect_awaiter&&) = delete;

                bool await_ready() noexcept { return false; }

                /// 取消挂起的连接 (同 read_awaiter::cancel_op)
                static void cancel_op(void* self) {
                    auto* aw = static_cast<connect_awaiter*>(self);
                    if (aw->sock != INVALID_SOCKET)
                        CancelIoEx(reinterpret_cast<HANDLE>(aw->sock), &aw->op.ov);
                }

                void await_suspend(std::coroutine_handle<> h) {
                    op.continuation = h;
                    auto* iocp = require_current_iocp(op, h);
                    if (!iocp)
                        return;
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
                        // IOCP 没有设置 FILE_SKIP_COMPLETION_PORT_ON_SUCCESS。
                        // 因而同步成功与 WSA_IO_PENDING 一样都会产生完成包;
                        // 统一交给 IOCP 收割, 避免手动 schedule 与完成包双重恢复。
                        iocp->op_start();
                    } else if (WSAGetLastError() != WSA_IO_PENDING) {
                        // 立即失败: 不会有 IOCP 通知, 手动恢复
                        op.error = WSAGetLastError();
                        EventLoop::get().schedule(h);
                    } else {
                        // WSA_IO_PENDING → 异步进行, 等 IOCP 通知
                        iocp->op_start();
                    }
                }

                TcpStream await_resume() {
                    if (op.error) {
                        closesocket(sock);
                        sock = INVALID_SOCKET;
                        io::set_error(op.error); // 原生码见 io::last_error()
                        return TcpStream{};      // 连接失败, valid()==false
                    }
                    // ConnectEx 完成后必须设置 SO_UPDATE_CONNECT_CONTEXT
                    // 否则后续某些 socket 操作可能失败
                    setsockopt(sock, SOL_SOCKET, SO_UPDATE_CONNECT_CONTEXT, nullptr, 0);
                    // connect() 创建时已经关联当前 IOCP；不要重复关联同一句柄。
                    return TcpStream(std::exchange(sock, INVALID_SOCKET), false);
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
                        const int error = WSAGetLastError();
                        closesocket(aw.sock);
                        aw.sock = INVALID_SOCKET;
                        aw.op.error = error;
                    } else {
                        if (!associate_current_iocp(aw.sock)) {
                            closesocket(aw.sock);
                            aw.sock = INVALID_SOCKET;
                            aw.op.error = WSAEOPNOTSUPP;
                        } else {
                            // ConnectEx 指针: 进程级缓存, 不再每次 WSAIoctl
                            aw.connect_ex_ = get_connect_ex();
                        }
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
                    auto* iocp = require_current_iocp(op, h);
                    if (!iocp)
                        return;
                    wsa_buf.len = (ULONG)len;
                    wsa_buf.buf = const_cast<char*>(buf);
                    DWORD sent = 0;
                    int rc = WSASend(stream->sock_, &wsa_buf, 1, &sent, 0, &op.ov, nullptr);
                    if (rc == SOCKET_ERROR && WSAGetLastError() != WSA_IO_PENDING) {
                        // 立即失败: 不会有 IOCP 通知, 手动恢复
                        op.error = WSAGetLastError();
                        EventLoop::get().schedule(h);
                    } else {
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
            bool reattach() {
                if (sock_ == INVALID_SOCKET)
                    return false;
                if (associate_current_iocp(sock_))
                    return true;
                close();
                return false;
            }

            void close() {
                if (sock_ != INVALID_SOCKET) {
                    closesocket(sock_);
                    sock_ = INVALID_SOCKET;
                }
            }

            /// 中止收发但保留句柄所有权。可由其他线程调用，用于唤醒挂起的
            /// read/write；真正 closesocket 仍由拥有 TcpStream 的线程执行。
            void shutdown() noexcept {
                if (sock_ != INVALID_SOCKET) {
                    // shutdown() 本身不保证唤醒已提交的重叠 WSARecv/WSASend；
                    // CancelIoEx(nullptr) 会让该句柄的所有挂起操作通过 IOCP
                    // 以 ERROR_OPERATION_ABORTED 完成，awaiter 因而能安全收尾。
                    CancelIoEx(reinterpret_cast<HANDLE>(sock_), nullptr);
                    ::shutdown(sock_, SD_BOTH);
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
                close();
                accept_ex_ = nullptr;

                sock_ = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
                if (sock_ == INVALID_SOCKET)
                    return false;

                sockaddr_in addr{};
                addr.sin_family = AF_INET;
                addr.sin_port = htons(port);
                addr.sin_addr.s_addr = inet_addr(ip);

                // Windows 上 SO_REUSEADDR 允许端口劫持: 已有服务监听时新 socket
                // 仍可能 bind 成功, 客户端请求被不可预测地分流。使用独占绑定，
                // 让端口冲突稳定地返回失败。
                BOOL exclusive = TRUE;
                if (setsockopt(sock_, SOL_SOCKET, SO_EXCLUSIVEADDRUSE, reinterpret_cast<const char*>(&exclusive),
                               sizeof(exclusive)) == SOCKET_ERROR) {
                    close();
                    return false;
                }

                if (bind(sock_, (sockaddr*)&addr, sizeof(addr)) == SOCKET_ERROR) {
                    closesocket(sock_);
                    sock_ = INVALID_SOCKET;
                    return false;
                }
                if (listen(sock_, SOMAXCONN) == SOCKET_ERROR) {
                    closesocket(sock_);
                    sock_ = INVALID_SOCKET;
                    return false;
                }

                // 获取 AcceptEx 函数指针 (需要 WSAIoctl)
                GUID guid = WSAID_ACCEPTEX;
                DWORD bytes = 0;
                if (WSAIoctl(sock_, SIO_GET_EXTENSION_FUNCTION_POINTER, &guid, sizeof(guid), &accept_ex_,
                             sizeof(accept_ex_), &bytes, nullptr, nullptr) == SOCKET_ERROR ||
                    !accept_ex_) {
                    close();
                    return false;
                }

                if (!associate_current_iocp(sock_)) {
                    close();
                    return false;
                }
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

                explicit accept_awaiter(TcpListener* owner, bool should_attach = true)
                    : listener(owner), attach(should_attach) {}
                ~accept_awaiter() {
                    if (accepted != INVALID_SOCKET)
                        closesocket(accepted);
                }
                accept_awaiter(accept_awaiter&& other) noexcept
                    : listener(other.listener), op(other.op), accepted(std::exchange(other.accepted, INVALID_SOCKET)),
                      attach(other.attach) {
                    std::memcpy(addr_buf, other.addr_buf, sizeof(addr_buf));
                }
                accept_awaiter(const accept_awaiter&) = delete;
                accept_awaiter& operator=(const accept_awaiter&) = delete;
                accept_awaiter& operator=(accept_awaiter&&) = delete;

                bool await_ready() noexcept { return false; }

                /// 取消挂起的 accept (同 read_awaiter::cancel_op)
                static void cancel_op(void* self) {
                    auto* aw = static_cast<accept_awaiter*>(self);
                    if (aw->listener->sock_ != INVALID_SOCKET)
                        CancelIoEx(reinterpret_cast<HANDLE>(aw->listener->sock_), &aw->op.ov);
                }

                void await_suspend(std::coroutine_handle<> h) {
                    op.continuation = h;
                    auto* iocp = require_current_iocp(op, h);
                    if (!iocp)
                        return;
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
                        // 同步成功也由 IOCP 完成包统一恢复 (见 ConnectEx 注释)。
                        iocp->op_start();
                    } else if (WSAGetLastError() != WSA_IO_PENDING) {
                        // 立即失败: 不会有 IOCP 通知, 手动恢复
                        op.error = WSAGetLastError();
                        closesocket(accepted);
                        accepted = INVALID_SOCKET;
                        EventLoop::get().schedule(h);
                    } else {
                        // WSA_IO_PENDING → 异步进行, 等 IOCP 通知
                        iocp->op_start();
                    }
                }

                TcpStream await_resume() {
                    if (op.error || accepted == INVALID_SOCKET) {
                        if (accepted != INVALID_SOCKET)
                            closesocket(accepted);
                        accepted = INVALID_SOCKET;
                        return TcpStream{}; // 无效流, valid()==false
                    }
                    // AcceptEx 完成后必须设置 SO_UPDATE_ACCEPT_CONTEXT
                    // 否则新 socket 无法正常进行 I/O 操作
                    setsockopt(accepted, SOL_SOCKET, SO_UPDATE_ACCEPT_CONTEXT, (char*)&listener->sock_,
                               sizeof(listener->sock_));
                    // accept 出的 socket 按 attach 标志决定是否关联 IOCP
                    return TcpStream(std::exchange(accepted, INVALID_SOCKET), attach);
                }
            };

            auto accept() { return accept_awaiter{this}; }

            /// 同 accept(), 但返回的连接不关联 IOCP:
            /// 多线程服务器用它把连接投递到 worker 线程, 在目标线程
            /// 调用 TcpStream::reattach() 完成关联 (首次关联)。
            auto accept_noattach() { return accept_awaiter{this, false}; }

            /// 同步关闭监听 (服务器停止用)。
            /// 挂起的 AcceptEx 会以 ERROR_OPERATION_ABORTED 完成包返回,
            /// accept() 立即返回无效流, 不会泄漏 OVERLAPPED。
            void close() {
                if (sock_ != INVALID_SOCKET) {
                    closesocket(sock_);
                    sock_ = INVALID_SOCKET;
                }
            }

            bool valid() const { return sock_ != INVALID_SOCKET; }

          private:
            SOCKET sock_ = INVALID_SOCKET;
            LPFN_ACCEPTEX accept_ex_ = nullptr; // AcceptEx 函数指针
        };

        // ==================================================================
        // UdpSocket — UDP 数据报套接字 (异步 recvfrom/sendto)
        // ==================================================================
        class UdpSocket {
          public:
            UdpSocket() = default;

            explicit UdpSocket(SOCKET s) : sock_(s) {
                if (sock_ != INVALID_SOCKET && !associate_current_iocp(sock_))
                    close();
            }

            ~UdpSocket() {
                if (sock_ != INVALID_SOCKET)
                    closesocket(sock_);
            }

            UdpSocket(UdpSocket&& other) noexcept : sock_(std::exchange(other.sock_, INVALID_SOCKET)) {}

            UdpSocket& operator=(UdpSocket&& other) noexcept {
                if (this != &other) {
                    if (sock_ != INVALID_SOCKET)
                        closesocket(sock_);
                    sock_ = std::exchange(other.sock_, INVALID_SOCKET);
                }
                return *this;
            }

            UdpSocket(const UdpSocket&) = delete;
            UdpSocket& operator=(const UdpSocket&) = delete;

            /// 绑定本地地址 (同步, 一次性)
            bool bind_listen(const char* ip, unsigned short port) {
                ensure_winsock();
                close();
                sock_ = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
                if (sock_ == INVALID_SOCKET)
                    return false;

                sockaddr_in addr{};
                addr.sin_family = AF_INET;
                addr.sin_port = htons(port);
                addr.sin_addr.s_addr = inet_addr(ip);

                if (bind(sock_, (sockaddr*)&addr, sizeof(addr)) == SOCKET_ERROR) {
                    close();
                    return false;
                }

                if (!associate_current_iocp(sock_)) {
                    close();
                    return false;
                }
                return true;
            }

            // ---- 异步 recvfrom ----

            struct recvfrom_awaiter {
                UdpSocket* socket;
                char* buf;
                size_t len;
                sockaddr_in* sender;
                detail::iocp_op op;
                WSABUF wsa_buf;
                sockaddr_in from_addr{};
                int from_len = sizeof(sockaddr_in);

                bool await_ready() noexcept { return false; }

                static void cancel_op(void* self) {
                    auto* aw = static_cast<recvfrom_awaiter*>(self);
                    if (aw->socket && aw->socket->valid())
                        CancelIoEx(reinterpret_cast<HANDLE>(aw->socket->sock_), &aw->op.ov);
                }

                void await_suspend(std::coroutine_handle<> h) {
                    op.continuation = h;
                    auto* iocp = require_current_iocp(op, h);
                    if (!iocp)
                        return;
                    wsa_buf.len = (ULONG)len;
                    wsa_buf.buf = buf;
                    from_len = sizeof(sockaddr_in);
                    DWORD flags = 0;
                    DWORD received = 0;
                    int rc = WSARecvFrom(socket->sock_, &wsa_buf, 1, &received, &flags, (sockaddr*)&from_addr,
                                         &from_len, &op.ov, nullptr);
                    if (rc == SOCKET_ERROR && WSAGetLastError() != WSA_IO_PENDING) {
                        op.error = WSAGetLastError();
                        EventLoop::get().schedule(h);
                    } else {
                        iocp->op_start();
                    }
                }

                int await_resume() {
                    if (op.error) {
                        io::set_error(op.error);
                        return -1;
                    }
                    if (sender)
                        std::memcpy(sender, &from_addr, sizeof(sockaddr_in));
                    return (int)op.transferred;
                }
            };

            /// co_await socket.recvfrom(buf, size, &sender) → 字节数 (0=空包, -1=错误)
            auto recvfrom(char* buf, size_t len, sockaddr_in* sender = nullptr) {
                return recvfrom_awaiter{this, buf, len, sender, {}, {}, {}, (int)sizeof(sockaddr_in)};
            }

            // ---- 异步 sendto ----

            struct sendto_awaiter {
                UdpSocket* socket;
                const char* buf;
                size_t len;
                sockaddr_in dest;
                detail::iocp_op op;
                WSABUF wsa_buf;

                bool await_ready() noexcept { return false; }

                static void cancel_op(void* self) {
                    auto* aw = static_cast<sendto_awaiter*>(self);
                    if (aw->socket && aw->socket->valid())
                        CancelIoEx(reinterpret_cast<HANDLE>(aw->socket->sock_), &aw->op.ov);
                }

                void await_suspend(std::coroutine_handle<> h) {
                    op.continuation = h;
                    auto* iocp = require_current_iocp(op, h);
                    if (!iocp)
                        return;
                    wsa_buf.len = (ULONG)len;
                    wsa_buf.buf = const_cast<char*>(buf);
                    DWORD sent = 0;
                    int rc = WSASendTo(socket->sock_, &wsa_buf, 1, &sent, 0, (sockaddr*)&dest, sizeof(dest), &op.ov,
                                       nullptr);
                    if (rc == SOCKET_ERROR && WSAGetLastError() != WSA_IO_PENDING) {
                        op.error = WSAGetLastError();
                        EventLoop::get().schedule(h);
                    } else {
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

            /// co_await socket.sendto(buf, size, dest) → 发送字节数 (-1=错误)
            auto sendto(const char* buf, size_t len, sockaddr_in dest) {
                return sendto_awaiter{this, buf, len, dest, {}, {}};
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

#endif // _WIN32

#ifdef CORO_URING_ENABLED

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
                net::UringEventSource* uring_ = nullptr;

                ~read_awaiter() {
                    if (uring_)
                        uring_->untrack_op(&op);
                }

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
                        uring_ = u;
                        u->track_op(&op);
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
                net::UringEventSource* uring_ = nullptr;

                ~write_awaiter() {
                    if (uring_)
                        uring_->untrack_op(&op);
                }

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
                        uring_ = u;
                        u->track_op(&op);
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
                net::UringEventSource* uring_ = nullptr;

                ~connect_awaiter() {
                    if (uring_)
                        uring_->untrack_op(&op);
                    if (fd >= 0)
                        ::close(fd);
                }
                connect_awaiter() = default;
                connect_awaiter(connect_awaiter&& other) noexcept
                    : fd(std::exchange(other.fd, -1)), addr(other.addr), op(other.op),
                      uring_(std::exchange(other.uring_, nullptr)) {}
                connect_awaiter(const connect_awaiter&) = delete;
                connect_awaiter& operator=(const connect_awaiter&) = delete;
                connect_awaiter& operator=(connect_awaiter&&) = delete;

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
                        uring_ = u;
                        u->track_op(&op);
                    } else {
                        op.result = -ENOTSUP;
                        EventLoop::get().schedule(h);
                    }
                }

                TcpStream await_resume() {
                    if (op.error) {
                        ::close(fd);
                        fd = -1;
                        io::set_error(op.error);
                        return TcpStream{}; // 连接失败, valid()==false
                    }
                    return TcpStream(std::exchange(fd, -1));
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

            /// 中止收发但不关闭 fd；用于跨线程唤醒挂起 I/O，避免 fd 重用竞态。
            void shutdown() noexcept {
                if (fd_ >= 0)
                    ::shutdown(fd_, SHUT_RDWR);
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

                // 允许端口复用: 防止 TIME_WAIT 状态导致 bind 失败
                int opt = 1;
                ::setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

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
                net::UringEventSource* uring_ = nullptr;

                explicit accept_awaiter(TcpListener* owner) : listener(owner) {}

                ~accept_awaiter() {
                    if (uring_)
                        uring_->untrack_op(&op);
                    // accept 成功与取消竞态时 await_resume 可能被跳过；
                    // 此时 CQE 中的新 fd 仍由 awaiter 负责关闭。
                    if (uring_ && !op.error && op.result >= 0)
                        ::close(op.result);
                }
                accept_awaiter(accept_awaiter&& other) noexcept
                    : listener(other.listener), op(other.op), uring_(std::exchange(other.uring_, nullptr)) {}
                accept_awaiter(const accept_awaiter&) = delete;
                accept_awaiter& operator=(const accept_awaiter&) = delete;
                accept_awaiter& operator=(accept_awaiter&&) = delete;

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
                        uring_ = u;
                        u->track_op(&op);
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
                    return TcpStream(std::exchange(op.result, -1)); // cqe->res 即新连接的 fd
                }
            };

            auto accept() { return accept_awaiter{this}; }

            /// io_uring 无 socket 关联概念, 与 accept() 等价 (接口对称,
            /// 方便多线程服务器按平台无差异地调用)。
            auto accept_noattach() { return accept_awaiter{this}; }

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

        // ==================================================================
        // UdpSocket — UDP 数据报套接字 (异步 recvfrom/sendto)
        // ==================================================================
        class UdpSocket {
          public:
            UdpSocket() = default;
            explicit UdpSocket(int fd) : fd_(fd) {}

            ~UdpSocket() {
                if (fd_ >= 0)
                    ::close(fd_);
            }

            UdpSocket(UdpSocket&& other) noexcept : fd_(std::exchange(other.fd_, -1)) {}

            UdpSocket& operator=(UdpSocket&& other) noexcept {
                if (this != &other) {
                    if (fd_ >= 0)
                        ::close(fd_);
                    fd_ = std::exchange(other.fd_, -1);
                }
                return *this;
            }

            UdpSocket(const UdpSocket&) = delete;
            UdpSocket& operator=(const UdpSocket&) = delete;

            /// 绑定本地地址 (同步, 一次性)
            bool bind_listen(const char* ip, unsigned short port) {
                fd_ = ::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK, 0);
                if (fd_ < 0)
                    return false;

                int opt = 1;
                ::setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

                sockaddr_in addr{};
                addr.sin_family = AF_INET;
                addr.sin_port = htons(port);
                addr.sin_addr.s_addr = inet_addr(ip);

                if (::bind(fd_, (sockaddr*)&addr, sizeof(addr)) < 0)
                    return false;
                return true;
            }

            // ---- 异步 recvfrom ----

            struct recvfrom_awaiter {
                UdpSocket* socket;
                char* buf;
                size_t len;
                sockaddr_in* sender;
                detail::uring_op op;
                net::UringEventSource* uring_ = nullptr;
                sockaddr_in from_addr{};
                socklen_t from_len = sizeof(sockaddr_in);

                ~recvfrom_awaiter() {
                    if (uring_)
                        uring_->untrack_op(&op);
                }

                bool await_ready() noexcept { return false; }

                static void cancel_op(void* self) {
                    auto* aw = static_cast<recvfrom_awaiter*>(self);
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
                    from_len = sizeof(sockaddr_in);
                    if (auto* u = current_uring()) {
                        io_uring_sqe* sqe = io_uring_get_sqe(u->handle());
                        if (!sqe) {
                            op.result = -ENOBUFS;
                            EventLoop::get().schedule(h);
                            return;
                        }
                        io_uring_prep_recvfrom(sqe, socket->fd_, buf, len, (sockaddr*)&from_addr, &from_len);
                        uring_submit_op(u, u->handle(), &op, sqe);
                        uring_ = u;
                        u->track_op(&op);
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
                    if (sender)
                        std::memcpy(sender, &from_addr, sizeof(sockaddr_in));
                    return op.result;
                }
            };

            auto recvfrom(char* buf, size_t len, sockaddr_in* sender = nullptr) {
                return recvfrom_awaiter{this, buf, len, sender, {}, {}, {}, sizeof(sockaddr_in)};
            }

            // ---- 异步 sendto ----

            struct sendto_awaiter {
                UdpSocket* socket;
                const char* buf;
                size_t len;
                sockaddr_in dest;
                detail::uring_op op;
                net::UringEventSource* uring_ = nullptr;

                ~sendto_awaiter() {
                    if (uring_)
                        uring_->untrack_op(&op);
                }

                bool await_ready() noexcept { return false; }

                static void cancel_op(void* self) {
                    auto* aw = static_cast<sendto_awaiter*>(self);
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
                        io_uring_prep_sendto(sqe, socket->fd_, buf, len, 0, (sockaddr*)&dest, sizeof(dest));
                        uring_submit_op(u, u->handle(), &op, sqe);
                        uring_ = u;
                        u->track_op(&op);
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

            auto sendto(const char* buf, size_t len, sockaddr_in dest) {
                return sendto_awaiter{this, buf, len, dest, {}, {}};
            }

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

#endif // CORO_URING_ENABLED

    } // namespace net
} // namespace coro
