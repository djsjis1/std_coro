#pragma once

// net_uring.hpp — coro::net 的 Linux io_uring 实现 (TcpStream / TcpListener / UdpSocket)
// 给谁用: 由 net.hpp 在 Linux 下自动包含; 直接 include 它则只能在启用 io_uring 的
// Linux 上编译。跨平台入口与用法见 net.hpp。

#include "event_loop.hpp"
#include "io.hpp"
#include "net_common.hpp"
#include "uring_event_source.hpp"

#include <coroutine>

#if defined(__linux__)
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>
#endif

#include <atomic>
#include <cstring>
#include <string>
#include <utility>

namespace coro {
    namespace net {

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
        // 事件源获取与 SQE 提交统一走 io 层 (detail::current_uring /
        // detail::uring_submit), 与 fs/pipe/unix 共用同一套设施。
        // ==================================================================

        /// getsockname + ntohs: 回查 fd 实际绑定的端口 (bind 到 0 时系统分配)。
        inline unsigned short local_port_of(int fd) {
            if (fd < 0)
                return 0;
            sockaddr_in name{};
            socklen_t len = sizeof(name);
            if (::getsockname(fd, reinterpret_cast<sockaddr*>(&name), &len) != 0)
                return 0;
            return ntohs(name.sin_port);
        }

        // ==================================================================
        // TcpStream — 已连接的 TCP 套接字 (异步 read/write)
        // ==================================================================
        class TcpStream {
          public:
            TcpStream() = default;
            explicit TcpStream(int fd) : fd_(fd) {}

            ~TcpStream() { close(); }

            TcpStream(TcpStream&& other) noexcept : fd_(std::exchange(other.fd_, -1)) {}

            TcpStream& operator=(TcpStream&& other) noexcept {
                if (this != &other) {
                    close();
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
                    if (auto* u = detail::current_uring()) {
                        detail::submit_uring_cancel(u, &aw->op);
                    }
                }

                void await_suspend(std::coroutine_handle<> h) {
                    op.continuation = h;
                    if (auto* u = detail::current_uring()) {
                        io_uring_sqe* sqe = io_uring_get_sqe(u->handle());
                        if (!sqe) {
                            // 提交队列满: 无法提交, 立即报错
                            op.result = -ENOBUFS;
                            EventLoop::get().schedule(h);
                            return;
                        }
                        op.fd = stream->fd_; // close 前可按 fd 批量取消
                        io_uring_prep_recv(sqe, stream->fd_, buf, len, 0);
                        detail::uring_submit(u, sqe, &op);
                        uring_ = u;
                        u->track_op(&op);
                    } else {
                        // 没有 io_uring 事件源: 立即失败
                        op.result = -ENOTSUP;
                        EventLoop::get().schedule(h);
                    }
                }

                int await_resume() {
                    if (const int err = detail::uring_op_error(op)) {
                        io::set_error(err);
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
                    if (auto* u = detail::current_uring()) {
                        detail::submit_uring_cancel(u, &aw->op);
                    }
                }

                void await_suspend(std::coroutine_handle<> h) {
                    op.continuation = h;
                    if (auto* u = detail::current_uring()) {
                        io_uring_sqe* sqe = io_uring_get_sqe(u->handle());
                        if (!sqe) {
                            op.result = -ENOBUFS;
                            EventLoop::get().schedule(h);
                            return;
                        }
                        op.fd = stream->fd_; // close 前可按 fd 批量取消
                        io_uring_prep_send(sqe, stream->fd_, buf, len, 0);
                        detail::uring_submit(u, sqe, &op);
                        uring_ = u;
                        u->track_op(&op);
                    } else {
                        op.result = -ENOTSUP;
                        EventLoop::get().schedule(h);
                    }
                }

                int await_resume() {
                    if (const int err = detail::uring_op_error(op)) {
                        io::set_error(err);
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
                    if (auto* u = detail::current_uring()) {
                        detail::submit_uring_cancel(u, &aw->op);
                    }
                }

                void await_suspend(std::coroutine_handle<> h) {
                    op.continuation = h;
                    if (op.result < 0) { // 参数校验已失败 (如非法 IP): 不再提交 syscall
                        EventLoop::get().schedule(h);
                        return;
                    }
                    if (auto* u = detail::current_uring()) {
                        io_uring_sqe* sqe = io_uring_get_sqe(u->handle());
                        if (!sqe) {
                            op.result = -ENOBUFS;
                            EventLoop::get().schedule(h);
                            return;
                        }
                        op.fd = fd; // close 前可按 fd 批量取消
                        io_uring_prep_connect(sqe, fd, (sockaddr*)&addr, sizeof(addr));
                        detail::uring_submit(u, sqe, &op);
                        uring_ = u;
                        u->track_op(&op);
                    } else {
                        op.result = -ENOTSUP;
                        EventLoop::get().schedule(h);
                    }
                }

                TcpStream await_resume() {
                    if (const int err = detail::uring_op_error(op)) {
                        if (fd >= 0)
                            ::close(fd);
                        fd = -1;
                        io::set_error(err);
                        return TcpStream{}; // 连接失败, valid()==false
                    }
                    return TcpStream(std::exchange(fd, -1));
                }
            };

            /// 创建 socket 并异步连接到 ip:port (零配置)
            static connect_awaiter connect(const char* ip, unsigned short port) {
                connect_awaiter aw;
                in_addr parsed{};
                if (!detail_net::parse_ipv4(ip, parsed)) {
                    aw.op.result = -EINVAL; // 非法地址: await_suspend 不再提交 syscall
                    return aw;
                }
                aw.fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
                aw.addr.sin_family = AF_INET;
                aw.addr.sin_port = htons(port);
                aw.addr.sin_addr = parsed;
                return aw;
            }

            // ---- 同步关闭 ----

            /// io_uring 无 socket 关联概念 (每次操作直接提交 SQE), 跨线程传递
            /// fd 即可使用。返回 bool 与 Windows 版签名对齐 (恒成功),
            /// 多线程服务器可按平台无差异地 `if (!s.reattach()) ...`。
            bool reattach() { return true; }

            void close() {
                if (fd_ >= 0) {
                    // 与 TcpListener/UdpSocket 同一合同: 挂起的 recv/send SQE
                    // 持有文件引用, 仅 close 不会让它们完成 (事件循环会永久等待);
                    // 先按 fd 提交取消, 再 shutdown 解除收发, 最后释放描述符。
                    if (auto* u = detail::current_uring())
                        u->cancel_fd_ops(fd_);
                    ::shutdown(fd_, SHUT_RDWR);
                    ::close(fd_);
                    fd_ = -1;
                }
            }

            /// 中止收发但不关闭 fd；用于跨线程唤醒挂起 I/O，避免 fd 复用竞态。
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

            ~TcpListener() { close(); }

            TcpListener(TcpListener&& other) noexcept : fd_(std::exchange(other.fd_, -1)) {}

            TcpListener& operator=(TcpListener&& other) noexcept {
                if (this != &other) {
                    close();
                    fd_ = std::exchange(other.fd_, -1);
                }
                return *this;
            }

            TcpListener(const TcpListener&) = delete;
            TcpListener& operator=(const TcpListener&) = delete;

            /// 绑定并监听 (同步, 一次性; 零配置)
            /// 实际绑定的端口: 支持先 bind 到 0, 再回查系统分配的真实端口
            unsigned short local_port() const { return local_port_of(fd_); }

            bool bind_listen(const char* ip, unsigned short port, int backlog = SOMAXCONN) {
                close(); // 幂等: 释放旧 fd, 失败后重试不泄漏描述符
                in_addr parsed{};
                if (!detail_net::parse_ipv4(ip, parsed))
                    return false;
                fd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
                if (fd_ < 0)
                    return false;

                // 允许端口复用: 防止 TIME_WAIT 状态导致 bind 失败
                int opt = 1;
                ::setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
#ifdef SO_REUSEPORT
                ::setsockopt(fd_, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));
#endif

                sockaddr_in addr{};
                addr.sin_family = AF_INET;
                addr.sin_port = htons(port);
                addr.sin_addr = parsed;

                if (::bind(fd_, (sockaddr*)&addr, sizeof(addr)) < 0) {
                    close();
                    return false;
                }
                if (::listen(fd_, backlog) < 0) {
                    close();
                    return false;
                }
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
                    if (auto* u = detail::current_uring()) {
                        detail::submit_uring_cancel(u, &aw->op);
                    }
                }

                void await_suspend(std::coroutine_handle<> h) {
                    op.continuation = h;
                    if (auto* u = detail::current_uring()) {
                        io_uring_sqe* sqe = io_uring_get_sqe(u->handle());
                        if (!sqe) {
                            op.result = -ENOBUFS;
                            EventLoop::get().schedule(h);
                            return;
                        }
                        // accept 出的 fd 也要原子设 CLOEXEC: 事后 fcntl 会留下
                        // “fork 已发生、CLOEXEC 未设”的窗口, 子进程会继承不该有的 socket。
                        op.fd = listener->fd_; // close 前可按 fd 批量取消
                        io_uring_prep_accept(sqe, listener->fd_, nullptr, nullptr, SOCK_CLOEXEC);
                        detail::uring_submit(u, sqe, &op);
                        uring_ = u;
                        u->track_op(&op);
                    } else {
                        op.result = -ENOTSUP;
                        EventLoop::get().schedule(h);
                    }
                }

                TcpStream await_resume() {
                    if (const int err = detail::uring_op_error(op)) {
                        io::set_error(err);
                        return TcpStream{}; // 无效流, valid()==false
                    }
                    return TcpStream(std::exchange(op.result, -1)); // cqe->res 即新连接的 fd
                }
            };

            auto accept() { return accept_awaiter{this}; }

            /// io_uring 无 socket 关联概念, 与 accept() 等价 (接口对称,
            /// 方便多线程服务器按平台无差异地调用)。
            auto accept_noattach() { return accept_awaiter{this}; }

            /// 同步关闭监听 (服务器停止用)，唤醒挂起的 accept 并返回无效流。
            void close() {
                if (fd_ >= 0) {
                    // io_uring 的 accept 持有文件引用，仅 close 不会使其完成。
                    // 先按 fd 提交取消，再 shutdown 解除监听，避免服务停止时永久挂起。
                    if (auto* u = detail::current_uring())
                        u->cancel_fd_ops(fd_);
                    ::shutdown(fd_, SHUT_RDWR);
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

            ~UdpSocket() { close(); }

            UdpSocket(UdpSocket&& other) noexcept : fd_(std::exchange(other.fd_, -1)) {}

            UdpSocket& operator=(UdpSocket&& other) noexcept {
                if (this != &other) {
                    close();
                    fd_ = std::exchange(other.fd_, -1);
                }
                return *this;
            }

            UdpSocket(const UdpSocket&) = delete;
            UdpSocket& operator=(const UdpSocket&) = delete;

            /// 绑定本地地址 (同步, 一次性)
            /// 实际绑定的端口: 支持先 bind 到 0, 再回查系统分配的真实端口
            unsigned short local_port() const { return local_port_of(fd_); }

            bool bind_listen(const char* ip, unsigned short port) {
                close(); // 幂等: 释放旧 fd, 失败后重试不泄漏描述符
                in_addr parsed{};
                if (!detail_net::parse_ipv4(ip, parsed))
                    return false;
                fd_ = ::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
                if (fd_ < 0)
                    return false;

                int opt = 1;
                ::setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
#ifdef SO_REUSEPORT
                ::setsockopt(fd_, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));
#endif

                sockaddr_in addr{};
                addr.sin_family = AF_INET;
                addr.sin_port = htons(port);
                addr.sin_addr = parsed;

                if (::bind(fd_, (sockaddr*)&addr, sizeof(addr)) < 0) {
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
                detail::uring_op op;
                net::UringEventSource* uring_ = nullptr;
                sockaddr_in from_addr{};
                socklen_t from_len = sizeof(sockaddr_in);
                // 描述符随 awaiter 存活，直到异步操作完成。
                iovec iov_{};
                msghdr msg_{};

                ~recvfrom_awaiter() {
                    if (uring_)
                        uring_->untrack_op(&op);
                }

                bool await_ready() noexcept { return false; }

                static void cancel_op(void* self) {
                    auto* aw = static_cast<recvfrom_awaiter*>(self);
                    if (auto* u = detail::current_uring()) {
                        detail::submit_uring_cancel(u, &aw->op);
                    }
                }

                void await_suspend(std::coroutine_handle<> h) {
                    op.continuation = h;
                    from_len = sizeof(sockaddr_in);
                    if (auto* u = detail::current_uring()) {
                        io_uring_sqe* sqe = io_uring_get_sqe(u->handle());
                        if (!sqe) {
                            op.result = -ENOBUFS;
                            EventLoop::get().schedule(h);
                            return;
                        }
                        iov_.iov_base = buf;
                        iov_.iov_len = len;
                        std::memset(&msg_, 0, sizeof(msg_));
                        msg_.msg_name = &from_addr;
                        msg_.msg_namelen = from_len;
                        msg_.msg_iov = &iov_;
                        msg_.msg_iovlen = 1;
                        op.fd = socket->fd_; // close 前可按 fd 批量取消
                        io_uring_prep_recvmsg(sqe, socket->fd_, &msg_, 0);
                        detail::uring_submit(u, sqe, &op);
                        uring_ = u;
                        u->track_op(&op);
                    } else {
                        op.result = -ENOTSUP;
                        EventLoop::get().schedule(h);
                    }
                }

                int await_resume() {
                    if (const int err = detail::uring_op_error(op)) {
                        io::set_error(err);
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
                // 描述符随 awaiter 存活，直到异步操作完成。
                iovec iov_{};
                msghdr msg_{};

                ~sendto_awaiter() {
                    if (uring_)
                        uring_->untrack_op(&op);
                }

                bool await_ready() noexcept { return false; }

                static void cancel_op(void* self) {
                    auto* aw = static_cast<sendto_awaiter*>(self);
                    if (auto* u = detail::current_uring()) {
                        detail::submit_uring_cancel(u, &aw->op);
                    }
                }

                void await_suspend(std::coroutine_handle<> h) {
                    op.continuation = h;
                    if (auto* u = detail::current_uring()) {
                        io_uring_sqe* sqe = io_uring_get_sqe(u->handle());
                        if (!sqe) {
                            op.result = -ENOBUFS;
                            EventLoop::get().schedule(h);
                            return;
                        }
                        iov_.iov_base = const_cast<char*>(buf);
                        iov_.iov_len = len;
                        std::memset(&msg_, 0, sizeof(msg_));
                        msg_.msg_name = &dest;
                        msg_.msg_namelen = sizeof(dest);
                        msg_.msg_iov = &iov_;
                        msg_.msg_iovlen = 1;
                        op.fd = socket->fd_; // close 前可按 fd 批量取消
                        io_uring_prep_sendmsg(sqe, socket->fd_, &msg_, 0);
                        detail::uring_submit(u, sqe, &op);
                        uring_ = u;
                        u->track_op(&op);
                    } else {
                        op.result = -ENOTSUP;
                        EventLoop::get().schedule(h);
                    }
                }

                int await_resume() {
                    if (const int err = detail::uring_op_error(op)) {
                        io::set_error(err);
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
                    // io_uring 上挂起的 recvmsg/sendmsg 持有文件引用, 仅 close 不会让它们
                    // 完成: 服务端关停时事件循环会永久等待。未连接 UDP socket 的
                    // shutdown 会失败 (ENOTCONN), 必须按 fd 提交取消才能唤醒挂起的收发。
                    if (auto* u = detail::current_uring())
                        u->cancel_fd_ops(fd_);
                    ::shutdown(fd_, SHUT_RDWR);
                    ::close(fd_);
                    fd_ = -1;
                }
            }

            /// 中止收发但保留描述符: 供其他线程唤醒挂起的 recvfrom, 避免 fd 复用竞态。
            void shutdown() noexcept {
                if (fd_ >= 0)
                    ::shutdown(fd_, SHUT_RDWR);
            }

            bool valid() const { return fd_ >= 0; }

          private:
            int fd_ = -1;
        };

#endif // CORO_URING_ENABLED

    } // namespace net
} // namespace coro
