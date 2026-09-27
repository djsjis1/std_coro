#pragma once

#include "event_loop.hpp"
#include "io.hpp"
#include "net.hpp"
#include "task.hpp"

#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <cerrno>
#include <coroutine>
#include <cstring>
#include <string>
#include <utility>

#if defined(__linux__) && defined(CORO_HAS_URING) && CORO_HAS_URING
#define CORO_HAS_UNIX 1
#else
#define CORO_HAS_UNIX 0
#endif

// ============================================================================
// coro::net — Linux Unix Domain Socket (计划 M4c)
// ============================================================================
//
// 只在 Linux + io_uring 下有效；其余平台/配置下本头**编译为空**（CORO_HAS_UNIX=0），
// 因此纯核心与非 Linux 消费者零成本，也不需要一套"假实现"来凑接口。
//
// 用途: 同机进程间通信。相比 TCP 回环省掉协议栈与端口争用，且权限由文件系统控制:
//
//   coro::net::UnixListener listener;
//   listener.bind("/tmp/my.sock");                 // 或 bind_abstract("@my")
//   auto conn = co_await listener.accept();
//   co_await conn.write(msg, len);
//
//   auto sock = co_await coro::net::UnixStream::connect("/tmp/my.sock");
//   int n = co_await sock.read(buf, sizeof buf);   // 0 = 对端关闭
//
// 三条实现要点:
//   1. **关闭必须先 shutdown 再 close**。挂起的 io_uring accept/recv 持有该 fd 的引用,
//      只 ::close 不会让它完成 (TcpListener 与 UdpSocket 都为此踩过: 表现为事件循环
//      永久等待、进程不退出)。
//   2. awaiter 与 TcpStream 同形态: `detail::uring_op` 内嵌在 awaiter 里, 挂起时
//      track_op、析构时 untrack_op; 取消走 ASYNC_CANCEL 让原操作以 -ECANCELED 完成,
//      保证 CQE 先于协程帧销毁被消费。
//      注意: 取消钩子**不需要也不允许**手动赋值 —— Task 的 await_transform 包装器会
//      通过 has_cancel_op trait 检测到 awaiter 提供了 `static cancel_op(void*)`, 自动把
//      `&awaiter` 作为 self 注册进 promise。uring_op 本身只有 continuation/result/error
//      三个字段。
//   3. 文件系统路径 socket 在 bind 前 unlink 陈旧 inode (否则 EADDRINUSE), 但**只删
//      自己创建的那个** —— 析构时 unlink 自己 bind 的路径, 不碰别人的文件。
// ============================================================================

namespace coro {
    namespace net {
#if CORO_HAS_UNIX

        /// 地址构造: 文件系统路径与 systemd 风格的抽象名 (@ 前缀, 内核命名空间内可见)
        inline sockaddr_un make_unix_address(const std::string& path) {
            sockaddr_un addr{};
            addr.sun_family = AF_UNIX;
            if (!path.empty() && path[0] == '@') {
                // 抽象命名空间: 首字节为 '\0', 其余按字面存, 不需要文件系统权限
                addr.sun_path[0] = '\0';
                const std::size_t name_len = path.size() - 1;
                const std::size_t room = sizeof(addr.sun_path) - 1;
                const std::size_t take = name_len < room ? name_len : room;
                std::memcpy(addr.sun_path + 1, path.data() + 1, take);
                return addr;
            }
            const std::size_t room = sizeof(addr.sun_path);
            if (path.size() >= room) {
                // 静默截断会连到错误的路径, 比报错更难查 → 直接判错
                errno = ENAMETOOLONG;
                return sockaddr_un{};
            }
            std::memcpy(addr.sun_path, path.data(), path.size());
            return addr;
        }

        inline socklen_t unix_address_length(const sockaddr_un& addr) {
            if (addr.sun_family != AF_UNIX)
                return sizeof(sa_family_t);
            if (addr.sun_path[0] == '\0') {
                // 抽象名: 长度必须精确到名字末尾, 否则内核会算进尾部 NUL
                const std::size_t name_len = strnlen(addr.sun_path + 1, sizeof(addr.sun_path) - 1);
                return static_cast<socklen_t>(offsetof(sockaddr_un, sun_path) + 1 + name_len);
            }
            return static_cast<socklen_t>(offsetof(sockaddr_un, sun_path) + strlen(addr.sun_path) + 1);
        }

        class UnixListener;

        /// Unix 流式连接端
        class UnixStream {
            friend class UnixListener;

          public:
            UnixStream() = default;
            explicit UnixStream(int fd) : fd_(fd) {}
            ~UnixStream() { close(); }

            UnixStream(UnixStream&& other) noexcept : fd_(std::exchange(other.fd_, -1)) {}
            UnixStream& operator=(UnixStream&& other) noexcept {
                if (this != &other) {
                    close();
                    fd_ = std::exchange(other.fd_, -1);
                }
                return *this;
            }
            UnixStream(const UnixStream&) = delete;
            UnixStream& operator=(const UnixStream&) = delete;

            bool valid() const noexcept { return fd_ >= 0; }
            int fd() const noexcept { return fd_; }

            /// 关闭: 先 shutdown 打断挂起操作的阻塞语义, 再 close 释放 fd
            void close() {
                if (fd_ >= 0) {
                    ::shutdown(fd_, SHUT_RDWR);
                    ::close(fd_);
                    fd_ = -1;
                }
            }

            /// co_await sock.read(buf, len) → >=0 字节数 (0 = 对端关闭), -1 错误 (errno 已设)
            struct read_awaiter {
                UnixStream* self;
                char* buf;
                size_t len;
                detail::uring_op op;
                UringEventSource* uring_ = nullptr;

                ~read_awaiter() {
                    if (uring_)
                        uring_->untrack_op(&op);
                }

                bool await_ready() const noexcept { return false; }

                static void cancel_op(void* ptr) {
                    auto* aw = static_cast<read_awaiter*>(ptr);
                    if (auto* u = current_uring()) {
                        if (io_uring_sqe* sqe = io_uring_get_sqe(u->handle())) {
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
                            op.error = ENOBUFS;
                            EventLoop::get().schedule(h);
                            return;
                        }
                        io_uring_prep_recv(sqe, self->fd_, buf, len, 0);
                        uring_submit_op(u, u->handle(), &op, sqe);
                        uring_ = u;
                        u->track_op(&op);
                    } else {
                        op.result = -ENOTSUP;
                        op.error = ENOTSUP;
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

            struct write_awaiter {
                UnixStream* self;
                const char* buf;
                size_t len;
                detail::uring_op op;
                UringEventSource* uring_ = nullptr;

                ~write_awaiter() {
                    if (uring_)
                        uring_->untrack_op(&op);
                }

                bool await_ready() const noexcept { return false; }

                static void cancel_op(void* ptr) {
                    auto* aw = static_cast<write_awaiter*>(ptr);
                    if (auto* u = current_uring()) {
                        if (io_uring_sqe* sqe = io_uring_get_sqe(u->handle())) {
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
                            op.error = ENOBUFS;
                            EventLoop::get().schedule(h);
                            return;
                        }
                        io_uring_prep_send(sqe, self->fd_, buf, len, MSG_NOSIGNAL);
                        uring_submit_op(u, u->handle(), &op, sqe);
                        uring_ = u;
                        u->track_op(&op);
                    } else {
                        op.result = -ENOTSUP;
                        op.error = ENOTSUP;
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

            /// 连到 Unix 路径 (或 @抽象名)。失败返回无效对象并设 errno。
            /// path 必须按值进帧: 协程参数会被拷进帧, 而调用方的字符串可能在我们从
            /// connect() 挂起点恢复之前就析构。改成 const& 正好制造悬空引用 ——
            /// cppcheck 的 passedByValue 建议对普通函数成立, 对协程入口不成立。
            // cppcheck-suppress passedByValue
            static Task<UnixStream> connect(std::string path) {
                sockaddr_un addr = make_unix_address(path);
                int fd = ::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
                if (fd < 0)
                    co_return UnixStream{};
                const socklen_t alen = unix_address_length(addr);
                if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), alen) != 0) {
                    const int saved = errno;
                    ::close(fd);
                    errno = saved;
                    co_return UnixStream{};
                }
                co_return UnixStream{fd};
            }

          private:
            int fd_ = -1;
        };

        /// 监听端
        class UnixListener {
          public:
            UnixListener() = default;
            ~UnixListener() { close(); }

            UnixListener(const UnixListener&) = delete;
            UnixListener& operator=(const UnixListener&) = delete;
            UnixListener(UnixListener&& other) noexcept
                : fd_(std::exchange(other.fd_, -1)), path_(std::move(other.path_)) {
                other.path_.clear();
            }
            UnixListener& operator=(UnixListener&& other) noexcept {
                if (this != &other) {
                    close();
                    fd_ = std::exchange(other.fd_, -1);
                    path_ = std::move(other.path_);
                    other.path_.clear();
                }
                return *this;
            }

            bool valid() const noexcept { return fd_ >= 0; }

            /// 绑定文件系统路径 (自动 unlink 同名陈旧 inode) 并开始监听
            bool bind(const std::string& path, int backlog = SOMAXCONN) {
                sockaddr_un addr = make_unix_address(path);
                if (addr.sun_family != AF_UNIX)
                    return false; // 路径过长: errno 已由 make_unix_address 设好
                const bool abstract = !path.empty() && path[0] == '@';
                if (!abstract)
                    ::unlink(path.c_str()); // 只清我们要占用的路径, 不碰别人的文件
                int fd = ::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
                if (fd < 0)
                    return false;
                const socklen_t alen = unix_address_length(addr);
                if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), alen) != 0 || ::listen(fd, backlog) != 0) {
                    const int saved = errno;
                    ::close(fd);
                    errno = saved;
                    if (!abstract)
                        ::unlink(path.c_str());
                    return false;
                }
                fd_ = fd;
                path_ = abstract ? std::string{} : path; // 只有文件路径需要在析构时清理
                return true;
            }

            /// 绑定 systemd 风格抽象名 ("@name"), 不依赖文件系统权限
            bool bind_abstract(const std::string& name, int backlog = SOMAXCONN) {
                std::string key = (name.empty() || name[0] == '@') ? name : "@" + name;
                return bind(key, backlog);
            }

            /// 关闭: 先 shutdown 唤醒挂起的 accept (只 close 会让事件循环永久等待)
            void close() {
                if (fd_ >= 0) {
                    ::shutdown(fd_, SHUT_RDWR);
                    ::close(fd_);
                    fd_ = -1;
                }
                if (!path_.empty()) {
                    ::unlink(path_.c_str());
                    path_.clear();
                }
            }

            /// 接受一个连接。失败返回无效 UnixStream (errno 已设)。
            struct accept_awaiter {
                UnixListener* self;
                detail::uring_op op;
                UringEventSource* uring_ = nullptr;

                ~accept_awaiter() {
                    if (uring_)
                        uring_->untrack_op(&op);
                }

                bool await_ready() const noexcept { return false; }

                static void cancel_op(void* ptr) {
                    auto* aw = static_cast<accept_awaiter*>(ptr);
                    if (auto* u = current_uring()) {
                        if (io_uring_sqe* sqe = io_uring_get_sqe(u->handle())) {
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
                            op.error = ENOBUFS;
                            EventLoop::get().schedule(h);
                            return;
                        }
                        io_uring_prep_accept(sqe, self->fd_, nullptr, nullptr, 0);
                        uring_submit_op(u, u->handle(), &op, sqe);
                        uring_ = u;
                        u->track_op(&op);
                    } else {
                        op.result = -ENOTSUP;
                        op.error = ENOTSUP;
                        EventLoop::get().schedule(h);
                    }
                }

                UnixStream await_resume() {
                    if (op.error) {
                        io::set_error(op.error);
                        return UnixStream{};
                    }
                    if (op.result < 0) {
                        errno = static_cast<int>(-op.result);
                        return UnixStream{};
                    }
                    return UnixStream{op.result};
                }
            };

            auto accept() { return accept_awaiter{this}; }

          private:
            int fd_ = -1;
            std::string path_;
        };

        /// 一对互联的 Unix 流 (无监听、无路径): 同进程内两个协程或父子进程用
        inline bool unix_pair(UnixStream& a, UnixStream& b) {
            int sv[2] = {-1, -1};
            if (::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sv) != 0)
                return false;
            a = UnixStream{sv[0]};
            b = UnixStream{sv[1]};
            return true;
        }
#endif // CORO_HAS_UNIX
    } // namespace net
} // namespace coro
