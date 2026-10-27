#pragma once

// net_iocp.hpp — coro::net 的 Windows IOCP 实现 (TcpStream / TcpListener / UdpSocket)
// 给谁用: 由 net.hpp 在 _WIN32 下自动包含; 直接 include 它则只能在 Windows 上编译。
// 跨平台入口与用法见 net.hpp。

#include "event_loop.hpp"
#include "io.hpp"
#include "iocp_event_source.hpp"
#include "net_common.hpp"

#include <coroutine>

#ifdef _WIN32
// WIN32_LEAN_AND_MEAN / NOMINMAX 已由 event_source.hpp 单点定义 (经 io.hpp/event_loop.hpp 引入)
#include <mswsock.h>
#include <winsock2.h>
#include <windows.h>
#include <ws2tcpip.h>
#endif

#include <atomic>
#include <cstring>
#include <string>
#include <utility>

namespace coro {
    namespace net {

#ifdef _WIN32

        // ==================================================================
        // Winsock 初始化 (幂等, 自动 WSAStartup)
        // ==================================================================
        inline bool ensure_winsock() {
            // WSAStartup 可能因资源不足/受限令牌而失败; 忽略返回值会让后续
            // socket() 失败并报出与根因无关的错误码。
            static const int rc = []() {
                WSADATA data;
                return WSAStartup(MAKEWORD(2, 2), &data);
            }();
            if (rc != 0) {
                io::set_error((int)WSAEINVAL);
                return false;
            }
            return true;
        }

        /// 获取当前事件循环的 IOCP 事件源: 类型化访问已收口到 io 层
        /// (detail::current_iocp, memo 化)。这里只保留 net 语义的两个便捷封装。
        inline bool associate_current_iocp(SOCKET socket) {
            auto* iocp = detail::current_iocp();
            if (iocp && iocp->associate(socket))
                return true;
            io::set_error(iocp ? (int)GetLastError() : (int)WSAEOPNOTSUPP);
            return false;
        }

        inline IocpEventSource* require_current_iocp(detail::iocp_op& op, std::coroutine_handle<> h) {
            if (auto* iocp = detail::current_iocp())
                return iocp;
            op.error = WSAEOPNOTSUPP;
            EventLoop::get().schedule(h);
            return nullptr;
        }

        /// getsockname + ntohs: 回查 socket 实际绑定的端口。
        /// bind 到端口 0 时由系统分配, 调用方靠它拿到真实端口。
        inline unsigned short local_port_of(SOCKET s) {
            if (s == INVALID_SOCKET)
                return 0;
            sockaddr_in name{};
            int len = static_cast<int>(sizeof(name));
            if (::getsockname(s, reinterpret_cast<sockaddr*>(&name), &len) == SOCKET_ERROR)
                return 0;
            return ntohs(name.sin_port);
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

            ~TcpStream() { close(); }

            TcpStream(TcpStream&& other) noexcept : sock_(std::exchange(other.sock_, INVALID_SOCKET)) {}

            TcpStream& operator=(TcpStream&& other) noexcept {
                if (this != &other) {
                    close();
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
                    // 否则后续某些 socket 操作可能失败; 该 setsockopt 本身失败
                    // 意味着连接不可用, 不能把半成品当成功返回。
                    if (setsockopt(sock, SOL_SOCKET, SO_UPDATE_CONNECT_CONTEXT, nullptr, 0) == SOCKET_ERROR) {
                        const int error = WSAGetLastError();
                        closesocket(sock);
                        sock = INVALID_SOCKET;
                        io::set_error(error);
                        return TcpStream{};
                    }
                    // connect() 创建时已经关联当前 IOCP; 不要重复关联同一句柄。
                    // Nagle: 请求-响应型协议里“小包等到 ACK 才发”会直接把往返拉长一个
                    // 数量级, 因此建立连接时默认关闭 (应用如需批量小包可自行开启)。
                    BOOL nodelay = TRUE;
                    setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<const char*>(&nodelay),
                               sizeof(nodelay));
                    return TcpStream(std::exchange(sock, INVALID_SOCKET), false);
                }
            };

            /// 创建 socket 并异步连接到 ip:port (零配置: 自动关联 IOCP)
            static connect_awaiter connect(const char* ip, unsigned short port) {
                connect_awaiter aw;
                in_addr parsed{};
                if (!detail_net::parse_ipv4(ip, parsed)) {
                    aw.op.error = (int)WSAEINVAL; // 非法地址: 不把 INADDR_NONE 当目标
                    return aw;
                }
                if (!ensure_winsock()) { // 必须先初始化 Winsock
                    aw.op.error = (int)WSAEINVAL;
                    return aw;
                }

                aw.sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
                aw.addr.sin_family = AF_INET;
                aw.addr.sin_port = htons(port);
                aw.addr.sin_addr = parsed;
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
                    // 与 TcpListener::close / UdpSocket 同一合同: 挂起的重叠
                    // I/O 先显式取消 (仅 closesocket 不承诺完成包投递),
                    // 保证 OVERLAPPED 先于协程帧销毁被事件循环消费。
                    // 无挂起操作时 CancelIoEx 仅返回 FALSE, 无副作用。
                    CancelIoEx(reinterpret_cast<HANDLE>(sock_), nullptr);
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

            TcpListener(TcpListener&& other) noexcept
                : sock_(std::exchange(other.sock_, INVALID_SOCKET)),
                  accept_ex_(std::exchange(other.accept_ex_, nullptr)) {}

            TcpListener& operator=(TcpListener&& other) noexcept {
                if (this != &other) {
                    close(); // CancelIoEx + closesocket: 与析构同一合同
                    sock_ = std::exchange(other.sock_, INVALID_SOCKET);
                    accept_ex_ = std::exchange(other.accept_ex_, nullptr);
                }
                return *this;
            }

            TcpListener(const TcpListener&) = delete;
            TcpListener& operator=(const TcpListener&) = delete;

            /// 绑定并监听 (同步, 一次性; 零配置: 自动关联 IOCP)
            /// 实际绑定的端口: 支持先 bind 到 0, 再回查系统分配的真实端口
            unsigned short local_port() const { return local_port_of(sock_); }

            bool bind_listen(const char* ip, unsigned short port, int backlog = SOMAXCONN) {
                if (!ensure_winsock()) // 必须先初始化 Winsock
                    return false;
                in_addr parsed{};
                if (!detail_net::parse_ipv4(ip, parsed))
                    return false;
                close();
                accept_ex_ = nullptr;

                sock_ = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
                if (sock_ == INVALID_SOCKET)
                    return false;

                sockaddr_in addr{};
                addr.sin_family = AF_INET;
                addr.sin_port = htons(port);
                addr.sin_addr = parsed;

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
                if (listen(sock_, backlog) == SOCKET_ERROR) {
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
                        // 保留底层错误快照, 不把失败伪装成"对端立刻关闭"
                        io::set_error(op.error ? op.error : (int)WSAEINVAL);
                        return TcpStream{}; // 无效流, valid()==false
                    }
                    // AcceptEx 完成后必须设置 SO_UPDATE_ACCEPT_CONTEXT
                    // 否则新 socket 无法正常进行 I/O 操作; 设置失败说明连接不可用
                    if (setsockopt(accepted, SOL_SOCKET, SO_UPDATE_ACCEPT_CONTEXT, (char*)&listener->sock_,
                                   sizeof(listener->sock_)) == SOCKET_ERROR) {
                        const int error = WSAGetLastError();
                        closesocket(accepted);
                        accepted = INVALID_SOCKET;
                        io::set_error(error);
                        return TcpStream{};
                    }
                    // accept 出的 socket 按 attach 标志决定是否关联 IOCP
                    // 与 connect 侧一致: 服务端同样默认关 Nagle, 避免回写的小响应被拖延
                    BOOL nodelay = TRUE;
                    setsockopt(accepted, IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<const char*>(&nodelay),
                               sizeof(nodelay));
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
            ///
            /// 先显式 CancelIoEx: closesocket 通常确实会让重叠 I/O  abort,
            /// 但"关闭带挂起异步操作的句柄"的**完成包投递并不被文档保证**;
            /// 而本仓库其它取消路径 (cancel_op / UdpSocket::close) 已经统一用
            /// CancelIoEx, 这里跟进可把行为从"依赖未保证的副作用"变成明确合同。
            /// 无挂起操作时 CancelIoEx 仅返回 FALSE, 无副作用。
            void close() {
                if (sock_ != INVALID_SOCKET) {
                    CancelIoEx(reinterpret_cast<HANDLE>(sock_), nullptr);
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

            ~UdpSocket() { close(); }

            UdpSocket(UdpSocket&& other) noexcept : sock_(std::exchange(other.sock_, INVALID_SOCKET)) {}

            UdpSocket& operator=(UdpSocket&& other) noexcept {
                if (this != &other) {
                    close();
                    sock_ = std::exchange(other.sock_, INVALID_SOCKET);
                }
                return *this;
            }

            UdpSocket(const UdpSocket&) = delete;
            UdpSocket& operator=(const UdpSocket&) = delete;

            /// 绑定本地地址 (同步, 一次性)
            /// 实际绑定的端口: 支持先 bind 到 0, 再回查系统分配的真实端口
            unsigned short local_port() const { return local_port_of(sock_); }

            bool bind_listen(const char* ip, unsigned short port) {
                if (!ensure_winsock())
                    return false;
                in_addr parsed{};
                if (!detail_net::parse_ipv4(ip, parsed))
                    return false;
                close();
                sock_ = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
                if (sock_ == INVALID_SOCKET)
                    return false;

                sockaddr_in addr{};
                addr.sin_family = AF_INET;
                addr.sin_port = htons(port);
                addr.sin_addr = parsed;

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
                if (valid()) {
                    // 与 TcpListener::close / TcpStream 同一合同: 挂起的重叠
                    // I/O 先显式取消 (仅 closesocket 不承诺完成包投递),
                    // 保证 OVERLAPPED 先于协程帧销毁被事件循环消费。
                    // 无挂起操作时 CancelIoEx 仅返回 FALSE, 无副作用。
                    CancelIoEx(reinterpret_cast<HANDLE>(sock_), nullptr);
                    closesocket(sock_);
                    sock_ = INVALID_SOCKET;
                }
            }

            /// 中止收发但保留句柄所有权。可由其他线程调用, 用于唤醒挂起的
            /// recvfrom/sendto; 真正 closesocket 仍由拥有 UdpSocket 的线程执行。
            /// (与 Linux 侧 UdpSocket::shutdown 及 TcpStream::shutdown 对齐)
            void shutdown() noexcept {
                if (sock_ != INVALID_SOCKET) {
                    // ::shutdown 本身不保证唤醒已提交的重叠 WSARecvFrom/WSASendTo;
                    // CancelIoEx(nullptr) 让该句柄的全部挂起操作经 IOCP 以
                    // ERROR_OPERATION_ABORTED 完成, 等待者才能安全收尾。
                    CancelIoEx(reinterpret_cast<HANDLE>(sock_), nullptr);
                    ::shutdown(sock_, SD_BOTH);
                }
            }

            bool valid() const { return sock_ != INVALID_SOCKET; }

          private:
            SOCKET sock_ = INVALID_SOCKET;
        };

#endif // _WIN32

    } // namespace net
} // namespace coro
