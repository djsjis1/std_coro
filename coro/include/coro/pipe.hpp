#pragma once

#include "io.hpp"
#include "task.hpp"

#ifdef _WIN32
// windows.h 已由 io.hpp 引入
#elif defined(__linux__)
#include <fcntl.h>
#include <unistd.h>
#endif

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <utility>

// ============================================================================
// coro::pipe — 异步管道 (对标 asyncio 的管道 / POSIX pipe 的协程化)
// ============================================================================
//
// API:
//   auto [rd, wr] = coro::pipe::pair();        // 一条单向通道 (POSIX pipe 语义)
//   // 写端 (任意线程的协程):
//   int n = co_await wr.write(data, len);      // 满则挂起 (背压)
//   // 读端:
//   int n = co_await rd.read(buf, sizeof(buf)); // 空则挂起; 0 = 对端关闭
//
// 平台实现 (与 net/fs 同一条完成路径):
//   Windows → 命名管道 (CreateNamedPipeW + FILE_FLAG_OVERLAPPED ×2 端)
//             注: 匿名管道 (CreatePipe) 不支持 OVERLAPPED, 不能接入 IOCP
//   Linux   → pipe2(O_NONBLOCK | O_CLOEXEC) + io_uring READ/WRITE
//
// 语义:
//   - read: >=0 字节数; 0 = 对端写端已全部关闭 (EOF); -1 错误
//   - write: >=0 字节数; -1 错误 (含对端读端已关闭 → EPIPE 语义)
//   - 背压: 管道缓冲区满时 write 挂起, 直到读端腾出空间 (天然流控)
//   - 取消: 挂起的 read/write 可被 Task::cancel 取消 (CancelIoEx / ASYNC_CANCEL)
//
// 默认缓冲 64KB (两平台同量级, 可在 pair() 指定)。
// ============================================================================

namespace coro
{
    namespace pipe
    {

#ifdef _WIN32

        // ==================================================================
        // Windows 实现 — 命名管道对 (同进程内的两端)
        // ==================================================================

        class PipeEnd
        {
        public:
            PipeEnd() = default;

            /// 接管已打开的 OVERLAPPED 管道句柄并关联当前 loop 的 IOCP
            explicit PipeEnd(HANDLE h) : handle_(h)
            {
                if (valid())
                    if (auto *iocp = EventLoop::get().iocp())
                        iocp->associate(handle_);
            }

            ~PipeEnd() { close(); }

            PipeEnd(PipeEnd &&other) noexcept
                : handle_(std::exchange(other.handle_, INVALID_HANDLE_VALUE)) {}

            PipeEnd &operator=(PipeEnd &&other) noexcept
            {
                if (this != &other)
                {
                    close();
                    handle_ = std::exchange(other.handle_, INVALID_HANDLE_VALUE);
                }
                return *this;
            }

            PipeEnd(const PipeEnd &) = delete;
            PipeEnd &operator=(const PipeEnd &) = delete;

            bool valid() const { return handle_ != INVALID_HANDLE_VALUE; }

            // ---- 异步读 (读端) ----

            /// co_await rd.read(buf, len)
            /// 返回: >=0 字节数; 0 = 对端写端已关闭 (EOF); -1 错误
            struct read_awaiter
            {
                PipeEnd *pipe;
                char *buf;
                size_t len;
                detail::iocp_op op;

                bool await_ready() const noexcept { return false; }

                static void cancel_op(void *self)
                {
                    auto *aw = static_cast<read_awaiter *>(self);
                    if (aw->pipe && aw->pipe->valid())
                        CancelIoEx(aw->pipe->handle_, &aw->op.ov);
                }

                void await_suspend(std::coroutine_handle<> h)
                {
                    op.continuation = h;
                    // 管道是流式设备: OVERLAPPED 偏移字段被忽略
                    op.ov.Offset = 0;
                    op.ov.OffsetHigh = 0;
                    DWORD got = 0;
                    BOOL rc = ReadFile(pipe->handle_, buf, (DWORD)len, &got, &op.ov);
                    if (!rc && GetLastError() != ERROR_IO_PENDING)
                    {
                        op.error = GetLastError();
                        EventLoop::get().schedule(h);
                    }
                    else
                    {
                        if (auto *iocp = EventLoop::get().iocp())
                            iocp->op_start();
                    }
                }

                int await_resume()
                {
                    if (op.error)
                    {
                        // 对端写端关闭: 管道断开, 语义等同 EOF (0 字节)
                        if (op.error == ERROR_BROKEN_PIPE ||
                            op.error == ERROR_PIPE_NOT_CONNECTED)
                            return 0;
                        io::set_error(op.error);
                        return -1;
                    }
                    return (int)op.transferred;
                }
            };

            read_awaiter read(char *buf, size_t len)
            {
                return read_awaiter{this, buf, len, {}};
            }

            // ---- 异步写 (写端) ----

            /// co_await wr.write(buf, len)
            /// 返回: >=0 字节数; -1 错误 (对端读端已关闭时错误码为
            /// ERROR_BROKEN_PIPE / ERROR_NO_DATA, 映射到 errno 为 EPIPE)
            struct write_awaiter
            {
                PipeEnd *pipe;
                const char *buf;
                size_t len;
                detail::iocp_op op;

                bool await_ready() const noexcept { return false; }

                static void cancel_op(void *self)
                {
                    auto *aw = static_cast<write_awaiter *>(self);
                    if (aw->pipe && aw->pipe->valid())
                        CancelIoEx(aw->pipe->handle_, &aw->op.ov);
                }

                void await_suspend(std::coroutine_handle<> h)
                {
                    op.continuation = h;
                    op.ov.Offset = 0;
                    op.ov.OffsetHigh = 0;
                    DWORD put = 0;
                    BOOL rc = WriteFile(pipe->handle_, buf, (DWORD)len, &put, &op.ov);
                    if (!rc && GetLastError() != ERROR_IO_PENDING)
                    {
                        op.error = GetLastError();
                        EventLoop::get().schedule(h);
                    }
                    else
                    {
                        // 缓冲区满时挂起, 读端腾出空间后完成 (背压)
                        if (auto *iocp = EventLoop::get().iocp())
                            iocp->op_start();
                    }
                }

                int await_resume()
                {
                    if (op.error)
                    {
                        io::set_error(op.error);
                        return -1;
                    }
                    return (int)op.transferred;
                }
            };

            write_awaiter write(const char *buf, size_t len)
            {
                return write_awaiter{this, buf, len, {}};
            }

            // ---- 同步操作 ----

            /// 关闭本端。读端关闭后, 对端 write 报错; 写端关闭后, 对端 read 得 0。
            /// 有挂起 IO 时不可调用 (生命周期约定同 net/fs)。
            void close()
            {
                if (valid())
                {
                    CloseHandle(handle_);
                    handle_ = INVALID_HANDLE_VALUE;
                }
            }

            HANDLE native() const { return handle_; }

        private:
            HANDLE handle_ = INVALID_HANDLE_VALUE;
        };

        /// 创建一条单向管道 (POSIX pipe 语义): 返回 {读端, 写端}。
        /// 同步调用 (创建是纯用户态/注册表操作, 微秒级), 需在事件循环线程调用
        /// (两端构造时关联当前 loop 的 IOCP)。
        /// buffer: 管道内核缓冲区字节数 (写满则 write 挂起 = 背压)。
        inline std::pair<PipeEnd, PipeEnd> pair(size_t buffer = 64 * 1024)
        {
            // 唯一管道名: 进程内计数器保证
            static std::atomic<uint64_t> counter{0};
            char name[96];
            std::snprintf(name, sizeof(name), "\\\\.\\pipe\\coro_pipe_%lu_%llu",
                          (unsigned long)GetCurrentProcessId(),
                          (unsigned long long)counter.fetch_add(1));
            // 名字是纯 ASCII, 直接加宽
            std::wstring wname(name, name + std::strlen(name));

            // 服务端 = 读端: PIPE_ACCESS_INBOUND (数据只能 客户端→服务端)
            HANDLE server = CreateNamedPipeW(
                wname.c_str(),
                PIPE_ACCESS_INBOUND | FILE_FLAG_OVERLAPPED,
                PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_WAIT,
                1,                       // nMaxInstances: 单实例
                (DWORD)buffer,           // out 缓冲 (读方向)
                (DWORD)buffer,           // in 缓冲 (写方向)
                0, nullptr);
            if (server == INVALID_HANDLE_VALUE)
            {
                io::set_error((int)GetLastError());
                return {PipeEnd{}, PipeEnd{}};
            }

            // 客户端 = 写端: 立即连接 (同进程, 无竞态窗口)
            HANDLE client = CreateFileW(wname.c_str(), GENERIC_WRITE, 0, nullptr,
                                        OPEN_EXISTING, FILE_FLAG_OVERLAPPED, nullptr);
            if (client == INVALID_HANDLE_VALUE)
            {
                io::set_error((int)GetLastError());
                CloseHandle(server);
                return {PipeEnd{}, PipeEnd{}};
            }

            // 完成服务端连接 (客户端已连上 → ERROR_PIPE_CONNECTED 即成功)
            if (!ConnectNamedPipe(server, nullptr) &&
                GetLastError() != ERROR_PIPE_CONNECTED)
            {
                io::set_error((int)GetLastError());
                CloseHandle(server);
                CloseHandle(client);
                return {PipeEnd{}, PipeEnd{}};
            }
            return {PipeEnd(server), PipeEnd(client)};
        }

#elif defined(__linux__)

        // ==================================================================
        // Linux 实现 — pipe2 + io_uring
        // ==================================================================
        // 注意: 本仓库在 Windows 上开发, 本节按 net/fs 的 io_uring 模式
        // 对称编写, 需要 Linux 环境编译验证。

        class PipeEnd
        {
        public:
            PipeEnd() = default;
            explicit PipeEnd(int fd) : fd_(fd) {}

            ~PipeEnd() { close(); }

            PipeEnd(PipeEnd &&other) noexcept
                : fd_(std::exchange(other.fd_, -1)) {}

            PipeEnd &operator=(PipeEnd &&other) noexcept
            {
                if (this != &other)
                {
                    close();
                    fd_ = std::exchange(other.fd_, -1);
                }
                return *this;
            }

            PipeEnd(const PipeEnd &) = delete;
            PipeEnd &operator=(const PipeEnd &) = delete;

            bool valid() const { return fd_ >= 0; }

            struct read_awaiter
            {
                PipeEnd *pipe;
                char *buf;
                size_t len;
                detail::uring_op op;

                bool await_ready() const noexcept { return false; }

                static void cancel_op(void *self)
                {
                    auto *aw = static_cast<read_awaiter *>(self);
                    if (auto *u = EventLoop::get().uring())
                    {
                        io_uring_sqe *sqe = io_uring_get_sqe(u->handle());
                        if (sqe)
                        {
                            io_uring_prep_cancel(sqe, &aw->op, 0);
                            io_uring_submit(u->handle());
                        }
                    }
                }

                void await_suspend(std::coroutine_handle<> h)
                {
                    op.continuation = h;
                    auto *u = EventLoop::get().uring();
                    if (!u)
                    {
                        op.result = -ENOTSUP;
                        EventLoop::get().schedule(h);
                        return;
                    }
                    io_uring_sqe *sqe = io_uring_get_sqe(u->handle());
                    if (!sqe)
                    {
                        op.result = -ENOBUFS;
                        EventLoop::get().schedule(h);
                        return;
                    }
                    // 非寻位设备: offset=-1 (使用文件位置, 管道忽略之)
                    io_uring_prep_read(sqe, pipe->fd_, buf, (unsigned)len, -1);
                    detail::uring_submit(u, sqe, &op);
                }

                int await_resume()
                {
                    if (op.error)
                    {
                        io::set_error(op.error);
                        return -1;
                    }
                    return op.result; // >=0 字节; 0 = 对端关闭 (EOF)
                }
            };

            read_awaiter read(char *buf, size_t len)
            {
                return read_awaiter{this, buf, len, {}};
            }

            struct write_awaiter
            {
                PipeEnd *pipe;
                const char *buf;
                size_t len;
                detail::uring_op op;

                bool await_ready() const noexcept { return false; }

                static void cancel_op(void *self)
                {
                    auto *aw = static_cast<write_awaiter *>(self);
                    if (auto *u = EventLoop::get().uring())
                    {
                        io_uring_sqe *sqe = io_uring_get_sqe(u->handle());
                        if (sqe)
                        {
                            io_uring_prep_cancel(sqe, &aw->op, 0);
                            io_uring_submit(u->handle());
                        }
                    }
                }

                void await_suspend(std::coroutine_handle<> h)
                {
                    op.continuation = h;
                    auto *u = EventLoop::get().uring();
                    if (!u)
                    {
                        op.result = -ENOTSUP;
                        EventLoop::get().schedule(h);
                        return;
                    }
                    io_uring_sqe *sqe = io_uring_get_sqe(u->handle());
                    if (!sqe)
                    {
                        op.result = -ENOBUFS;
                        EventLoop::get().schedule(h);
                        return;
                    }
                    io_uring_prep_write(sqe, pipe->fd_, buf, (unsigned)len, -1);
                    detail::uring_submit(u, sqe, &op);
                }

                int await_resume()
                {
                    if (op.error)
                    {
                        io::set_error(op.error);
                        return -1;
                    }
                    return op.result;
                }
            };

            write_awaiter write(const char *buf, size_t len)
            {
                return write_awaiter{this, buf, len, {}};
            }

            void close()
            {
                if (fd_ >= 0)
                {
                    ::close(fd_);
                    fd_ = -1;
                }
            }

            int native() const { return fd_; }

        private:
            int fd_ = -1;
        };

        /// 创建一条单向管道 (POSIX pipe 语义)。buffer 参数被忽略
        /// (Linux 管道缓冲由内核管理, 默认 64KB)。
        inline std::pair<PipeEnd, PipeEnd> pair(size_t = 0)
        {
            int fds[2];
            if (pipe2(fds, O_NONBLOCK | O_CLOEXEC) != 0)
            {
                io::set_error(errno);
                return {PipeEnd{}, PipeEnd{}};
            }
            return {PipeEnd(fds[0]), PipeEnd(fds[1])};
        }

#endif

    } // namespace pipe

#ifdef __linux__

    // ======================================================================
    // coro::io::poll — fd 就绪轮询 (仅 Linux; Windows socket 是完成制, 无此概念)
    // ======================================================================
    //
    // 用法:
    //   uint32_t revents = co_await coro::io::poll(fd, POLLIN);   // 挂起到可读
    //   // revents 含 POLLIN / POLLOUT / POLLERR / POLLHUP 位
    //
    // 基于 IORING_OP_POLL_ADD (单次触发)。任意 fd: tty / 设备 / 管道 / socket。
    // ======================================================================
    namespace io
    {
        struct poll_awaiter
        {
            int fd;
            short events; // POLLIN / POLLOUT / POLLRDHUP ... (poll.h 语义)
            detail::uring_op op;

            bool await_ready() const noexcept { return false; }

            static void cancel_op(void *self)
            {
                auto *aw = static_cast<poll_awaiter *>(self);
                if (auto *u = EventLoop::get().uring())
                {
                    io_uring_sqe *sqe = io_uring_get_sqe(u->handle());
                    if (sqe)
                    {
                        io_uring_prep_cancel(sqe, &aw->op, 0);
                        io_uring_submit(u->handle());
                    }
                }
            }

            void await_suspend(std::coroutine_handle<> h)
            {
                op.continuation = h;
                auto *u = EventLoop::get().uring();
                if (!u)
                {
                    op.result = -ENOTSUP;
                    EventLoop::get().schedule(h);
                    return;
                }
                io_uring_sqe *sqe = io_uring_get_sqe(u->handle());
                if (!sqe)
                {
                    op.result = -ENOBUFS;
                    EventLoop::get().schedule(h);
                    return;
                }
                io_uring_prep_poll_add(sqe, fd, events);
                detail::uring_submit(u, sqe, &op);
            }

            uint32_t await_resume() { return (uint32_t)op.result; }
        };

        inline poll_awaiter poll(int fd, short events)
        {
            return poll_awaiter{fd, events, {}};
        }
    } // namespace io

#endif // __linux__

} // namespace coro
