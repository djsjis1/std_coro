#pragma once

#include "io.hpp"
#include "task.hpp"
#include "thread.hpp"

#ifdef _WIN32
// windows.h / winsock2.h 已由 io.hpp 引入
#elif defined(__linux__)
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

#include <cstdint>
#include <string>
#include <string_view>
#include <utility>

// ============================================================================
// coro::fs — 异步文件 IO (对标 Python aiofiles / asyncio 的文件场景)
// ============================================================================
//
// 平台实现 (与 coro::net 同一条完成路径, 零配置):
//   Windows → CreateFileW(FILE_FLAG_OVERLAPPED) + ReadFile/WriteFile + IOCP
//   Linux   → io_uring (IORING_OP_OPENAT/READ/WRITE/FSYNC)
//
// API 速览:
//   coro::fs::File f = co_await coro::fs::open("data.bin", coro::fs::mode::read);
//   int n = co_await f.read_at(buf, sizeof(buf), offset);   // >=0 字节, 0=EOF, -1 错误
//   int n = co_await f.write_at(data, len, offset);
//   co_await f.fsync();
//   auto st = coro::fs::stat("data.bin");                   // 同步 (元数据操作)
//   std::string s = co_await coro::fs::read_all("a.txt");
//
// 设计要点:
//   - 定位读写 (read_at/write_at 显式 offset, 不共享文件游标):
//     同一文件可被多个协程并发分块读写, 无游标竞争 —— 这是异步文件 IO
//     最自然的形态 (io_uring READ/WRITE 原生带 offset; Windows 用
//     OVERLAPPED.Offset/OffsetHigh)。
//   - EOF 语义: read_at 在文件尾返回 0 (实测: Windows 越过 EOF 的 ReadFile
//     返回 ERROR_IO_PENDING, 完成包携带 ERROR_HANDLE_EOF —— 统一转成 0)。
//   - 取消: read_at/write_at 挂起时可被 Task::cancel 取消 (CancelIoEx /
//     IORING_OP_ASYNC_CANCEL), 与网络层同一套机制。
//   - open/stat 在 Windows 上是同步调用 (元数据操作, 微秒级, OS 已缓存);
//     Linux 上 open 走异步 IORING_OP_OPENAT。stat 两平台都同步。
//   - fsync: Windows 无异步刷盘公共 API → to_thread(FlushFileBuffers);
//     Linux 用 IORING_OP_FSYNC。
//
// 生命周期约定 (同 net): 操作挂起期间 File 必须保持存活; 不要在有挂起
// 读/写时 close/析构。Windows 下 append 模式 (mode::append) 的 write_at
// 忽略 offset, 由 OS 原子追加到文件尾。
// ============================================================================

namespace coro
{
    namespace fs
    {

        // ==================================================================
        // 打开模式 (组合语义对标 fopen)
        // ==================================================================
        enum class mode : unsigned
        {
            read = 1,      // "r"  只读, 文件必须存在
            write = 2,     // "w"  创建或截断 (等价 write|create|truncate 的快捷写法)
            create = 4,    // 不存在则创建 (与 write 组合时不截断)
            truncate = 8,  // 存在则清空
            append = 16,   // "a"  追加写 (隐含 create; write_at 的 offset 被忽略)
            exclusive = 32 // 与 create 组合: 已存在则报错 (原子创建)
        };
        constexpr mode operator|(mode a, mode b) { return mode(unsigned(a) | unsigned(b)); }
        constexpr bool operator&(mode a, mode b) { return (unsigned(a) & unsigned(b)) != 0; }

        /// 文件元信息 (coro::fs::stat 的返回值)
        struct stat_info
        {
            uint64_t size = 0;      // 字节数
            int64_t mtime_sec = 0;  // 最后修改时间 (Unix 秒)
            bool is_dir = false;    // 是否目录
            bool exists = false;    // 是否存在
        };

#ifdef _WIN32

        // ==================================================================
        // Windows 实现 — IOCP 文件 IO
        // ==================================================================

        namespace detail_fs
        {
            /// UTF-8 → UTF-16 (Windows API 需要宽字符路径)
            inline std::wstring utf8_to_wide(std::string_view s)
            {
                int n = MultiByteToWideChar(CP_UTF8, 0, s.data(), (int)s.size(), nullptr, 0);
                std::wstring w((size_t)(n > 0 ? n : 0), L'\0');
                if (n > 0)
                    MultiByteToWideChar(CP_UTF8, 0, s.data(), (int)s.size(), w.data(), n);
                return w;
            }
        } // namespace detail_fs

        class File
        {
        public:
            File() = default;

            /// 接管已打开的 OVERLAPPED 句柄并关联当前 loop 的 IOCP
            /// (正常用法是 co_await fs::open(), 手动构造用于接管外部句柄)
            explicit File(HANDLE h) : handle_(h)
            {
                if (valid())
                    if (auto *iocp = EventLoop::get().iocp())
                        iocp->associate(handle_);
            }

            ~File() { close(); }

            File(File &&other) noexcept
                : handle_(std::exchange(other.handle_, INVALID_HANDLE_VALUE)) {}

            File &operator=(File &&other) noexcept
            {
                if (this != &other)
                {
                    close();
                    handle_ = std::exchange(other.handle_, INVALID_HANDLE_VALUE);
                }
                return *this;
            }

            File(const File &) = delete;
            File &operator=(const File &) = delete;

            bool valid() const { return handle_ != INVALID_HANDLE_VALUE; }

            // ---- 异步定位读 ----

            /// co_await f.read_at(buf, len, offset)
            /// 返回: >=0 实际读取字节数; 0 = 到达文件尾 (EOF); -1 失败
            ///       (错误码: errno 已转换, 原生码在 io::last_error())
            struct read_at_awaiter
            {
                File *file;
                char *buf;
                size_t len;
                uint64_t offset;
                coro::detail::iocp_op op;

                bool await_ready() const noexcept { return false; }

                /// 取消挂起的读 (Task::cancel 钩子, 同网络层)
                static void cancel_op(void *self)
                {
                    auto *aw = static_cast<read_at_awaiter *>(self);
                    if (aw->file && aw->file->valid())
                        CancelIoEx(aw->file->handle_, &aw->op.ov);
                }

                void await_suspend(std::coroutine_handle<> h)
                {
                    op.continuation = h;
                    // OVERLAPPED 的 64 位偏移 = Offset (低32) + OffsetHigh (高32)
                    op.ov.Offset = (DWORD)(offset & 0xFFFFFFFFull);
                    op.ov.OffsetHigh = (DWORD)(offset >> 32);
                    DWORD read = 0;
                    BOOL rc = ReadFile(file->handle_, buf, (DWORD)len, &read, &op.ov);
                    if (!rc && GetLastError() != ERROR_IO_PENDING)
                    {
                        // 立即失败 (句柄无效等): 不会投递完成包, 手动恢复。
                        // 注意 EOF 不走这条路 —— 实测越过 EOF 的 ReadFile
                        // 返回 ERROR_IO_PENDING, 完成包携带 ERROR_HANDLE_EOF。
                        op.error = GetLastError();
                        EventLoop::get().schedule(h);
                    }
                    else
                    {
                        // IO_PENDING 或同步完成: 关联 IOCP 的句柄都会投递完成包
                        if (auto *iocp = EventLoop::get().iocp())
                            iocp->op_start();
                    }
                }

                int await_resume()
                {
                    if (op.error)
                    {
                        if (op.error == ERROR_HANDLE_EOF)
                            return 0; // 文件尾: 语义上"读了 0 字节"
                        io::set_error(op.error);
                        return -1;
                    }
                    return (int)op.transferred;
                }
            };

            read_at_awaiter read_at(char *buf, size_t len, uint64_t offset)
            {
                return read_at_awaiter{this, buf, len, offset, {}};
            }

            // ---- 异步定位写 ----

            /// co_await f.write_at(buf, len, offset)
            /// 返回: >=0 实际写入字节数; -1 失败。
            /// append 模式 (打开时 mode::append): offset 被忽略, OS 原子追加。
            struct write_at_awaiter
            {
                File *file;
                const char *buf;
                size_t len;
                uint64_t offset;
                coro::detail::iocp_op op;

                bool await_ready() const noexcept { return false; }

                static void cancel_op(void *self)
                {
                    auto *aw = static_cast<write_at_awaiter *>(self);
                    if (aw->file && aw->file->valid())
                        CancelIoEx(aw->file->handle_, &aw->op.ov);
                }

                void await_suspend(std::coroutine_handle<> h)
                {
                    op.continuation = h;
                    op.ov.Offset = (DWORD)(offset & 0xFFFFFFFFull);
                    op.ov.OffsetHigh = (DWORD)(offset >> 32);
                    DWORD written = 0;
                    BOOL rc = WriteFile(file->handle_, buf, (DWORD)len, &written, &op.ov);
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
                        io::set_error(op.error);
                        return -1;
                    }
                    return (int)op.transferred;
                }
            };

            write_at_awaiter write_at(const char *buf, size_t len, uint64_t offset)
            {
                return write_at_awaiter{this, buf, len, offset, {}};
            }

            // ---- 刷盘 ----

            /// 将文件数据刷到磁盘 (fsync 语义)。
            /// Windows 无异步刷盘公共 API → 经 to_thread 线程池执行
            /// FlushFileBuffers (不阻塞事件循环线程)。
            /// 返回 false 时错误码在 io::last_error()。
            Task<bool> fsync()
            {
                if (!valid())
                    return to_thread([] { return false; });
                HANDLE h = handle_;
                return to_thread([h]() -> bool
                                 {
                                     if (!FlushFileBuffers(h))
                                     {
                                         io::set_error((int)GetLastError());
                                         return false;
                                     }
                                     return true;
                                 });
            }

            // ---- 同步操作 ----

            void close()
            {
                if (valid())
                {
                    CloseHandle(handle_);
                    handle_ = INVALID_HANDLE_VALUE;
                }
            }

            /// 底层句柄 (高级用法)
            HANDLE native() const { return handle_; }

        private:
            HANDLE handle_ = INVALID_HANDLE_VALUE;
        };

        /// 打开文件 (Windows: 同步 CreateFileW — 元数据操作, 微秒级;
        /// 需要严格异步打开的场景请用 Linux/io_uring 路径)。
        /// 失败返回无效 File (valid()==false), 错误码在 io::last_error()。
        inline Task<File> open(std::string_view path, mode m)
        {
            bool rd = (m & mode::read), wr = (m & mode::write), ap = (m & mode::append);

            DWORD access = 0;
            if (rd)
                access |= GENERIC_READ;
            if (wr && !ap)
                access |= GENERIC_WRITE;
            if (ap)
                access |= FILE_APPEND_DATA; // 追加写: Offset 被忽略, 原子追加

            DWORD creation;
            if (m & mode::exclusive)
                creation = CREATE_NEW;
            else if ((m & mode::truncate) || (wr && !(m & mode::create) && !ap))
                creation = CREATE_ALWAYS; // "w" 语义: 创建或截断
            else if (wr || ap || (m & mode::create))
                creation = OPEN_ALWAYS; // 创建但不截断
            else
                creation = OPEN_EXISTING; // "r" 语义

            HANDLE h = CreateFileW(detail_fs::utf8_to_wide(path).c_str(), access,
                                   FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                                   nullptr, creation, FILE_FLAG_OVERLAPPED, nullptr);
            if (h == INVALID_HANDLE_VALUE)
                io::set_error((int)GetLastError());
            co_return File(h); // 构造时自动关联当前 loop 的 IOCP
        }

        /// 查询文件元信息 (同步: 元数据被 OS 缓存, 非热路径)。
        /// 文件不存在时返回 exists=false (并设置 io::last_error)。
        inline stat_info stat(std::string_view path)
        {
            stat_info st;
            WIN32_FILE_ATTRIBUTE_DATA fa;
            if (!GetFileAttributesExW(detail_fs::utf8_to_wide(path).c_str(),
                                      GetFileExInfoStandard, &fa))
            {
                io::set_error((int)GetLastError());
                return st; // exists = false
            }
            st.exists = true;
            st.is_dir = !!(fa.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY);
            st.size = ((uint64_t)fa.nFileSizeHigh << 32) | fa.nFileSizeLow;
            // FILETIME (1601 起 100ns) → Unix 秒
            ULARGE_INTEGER ft;
            ft.HighPart = fa.ftLastWriteTime.dwHighDateTime;
            ft.LowPart = fa.ftLastWriteTime.dwLowDateTime;
            st.mtime_sec = (int64_t)((ft.QuadPart - 116444736000000000ull) / 10000000ull);
            return st;
        }

#elif defined(__linux__)

        // ==================================================================
        // Linux 实现 — io_uring 文件 IO (结构与 Windows 层对称)
        // ==================================================================
        // 注意: 本仓库在 Windows 上开发, 本节按 net.hpp 的 io_uring 模式
        // 对称编写, 需要 Linux 环境编译验证。

        class File
        {
        public:
            File() = default;
            explicit File(int fd) : fd_(fd) {}

            ~File() { close(); }

            File(File &&other) noexcept
                : fd_(std::exchange(other.fd_, -1)) {}

            File &operator=(File &&other) noexcept
            {
                if (this != &other)
                {
                    close();
                    fd_ = std::exchange(other.fd_, -1);
                }
                return *this;
            }

            File(const File &) = delete;
            File &operator=(const File &) = delete;

            bool valid() const { return fd_ >= 0; }

            // ---- 异步定位读 ----

            struct read_at_awaiter
            {
                File *file;
                char *buf;
                size_t len;
                uint64_t offset;
                coro::detail::uring_op op;

                bool await_ready() const noexcept { return false; }

                static void cancel_op(void *self)
                {
                    auto *aw = static_cast<read_at_awaiter *>(self);
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
                    // IORING_OP_READ 自带偏移: 定位读, 不移动文件游标
                    io_uring_prep_read(sqe, file->fd_, buf, (unsigned)len, (__s64)offset);
                    coro::detail::uring_submit(u, sqe, &op);
                }

                int await_resume()
                {
                    if (op.error)
                    {
                        io::set_error(op.error);
                        return -1;
                    }
                    return op.result; // >=0 字节; 0 = EOF
                }
            };

            read_at_awaiter read_at(char *buf, size_t len, uint64_t offset)
            {
                return read_at_awaiter{this, buf, len, offset, {}};
            }

            // ---- 异步定位写 ----

            struct write_at_awaiter
            {
                File *file;
                const char *buf;
                size_t len;
                uint64_t offset;
                coro::detail::uring_op op;

                bool await_ready() const noexcept { return false; }

                static void cancel_op(void *self)
                {
                    auto *aw = static_cast<write_at_awaiter *>(self);
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
                    io_uring_prep_write(sqe, file->fd_, buf, (unsigned)len, (__s64)offset);
                    coro::detail::uring_submit(u, sqe, &op);
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

            write_at_awaiter write_at(const char *buf, size_t len, uint64_t offset)
            {
                return write_at_awaiter{this, buf, len, offset, {}};
            }

            // ---- 异步刷盘 (IORING_OP_FSYNC) ----

            struct fsync_awaiter
            {
                File *file;
                coro::detail::uring_op op;

                bool await_ready() const noexcept { return false; }

                static void cancel_op(void *self)
                {
                    auto *aw = static_cast<fsync_awaiter *>(self);
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
                    io_uring_prep_fsync(sqe, file->fd_, 0);
                    coro::detail::uring_submit(u, sqe, &op);
                }

                bool await_resume()
                {
                    if (op.error)
                    {
                        io::set_error(op.error);
                        return false;
                    }
                    return op.result == 0;
                }
            };

            fsync_awaiter fsync() { return fsync_awaiter{this, {}}; }

            // ---- 同步操作 ----

            /// 注意: io_uring 的 IORING_OP_CLOSE 存在延迟回收语义, 直接
            /// 同步 close 即可 (close 不阻塞); 有挂起操作时不可调用。
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

        /// 打开文件 (异步: IORING_OP_OPENAT)。
        /// 失败返回无效 File, 错误码在 io::last_error()。
        inline Task<File> open(std::string_view path, mode m)
        {
            struct open_awaiter
            {
                std::string path; // 拥有路径 (SQE 引用它的 c_str)
                int flags;
                mode_t mode_bits;
                coro::detail::uring_op op;

                bool await_ready() const noexcept { return false; }

                static void cancel_op(void *self)
                {
                    auto *aw = static_cast<open_awaiter *>(self);
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
                    io_uring_prep_openat(sqe, AT_FDCWD, path.c_str(), flags, mode_bits);
                    coro::detail::uring_submit(u, sqe, &op);
                }

                File await_resume()
                {
                    if (op.error)
                        io::set_error(op.error);
                    return File(op.result); // cqe->res 即新 fd (失败为负)
                }
            };

            int flags = O_CLOEXEC;
            bool rd = (m & mode::read), wr = (m & mode::write), ap = (m & mode::append);
            if (rd && (wr || ap))
                flags |= O_RDWR;
            else if (wr || ap)
                flags |= O_WRONLY;
            else
                flags |= O_RDONLY;
            if ((m & mode::create) || wr || ap)
                flags |= O_CREAT;
            if ((m & mode::truncate) || (wr && !(m & mode::create) && !ap))
                flags |= O_TRUNC;
            if (ap)
                flags |= O_APPEND;
            if (m & mode::exclusive)
                flags |= O_EXCL;

            open_awaiter aw{std::string(path), flags, 0644, {}};
            co_return co_await aw;
        }

        /// 查询文件元信息 (同步: 元数据被 OS 缓存, 非热路径)
        inline stat_info stat(std::string_view path)
        {
            stat_info st;
            struct ::stat sb;
            if (::stat(std::string(path).c_str(), &sb) != 0)
            {
                io::set_error(errno);
                return st;
            }
            st.exists = true;
            st.is_dir = S_ISDIR(sb.st_mode);
            st.size = (uint64_t)sb.st_size;
            st.mtime_sec = (int64_t)sb.st_mtime;
            return st;
        }

#endif

        // ==================================================================
        // 便捷函数 (跨平台)
        // ==================================================================

        /// 一次性读入整个文件。
        /// 失败 (不存在/无权限/读错误) 返回空串, 错误码在 io::last_error()
        /// (空文件也返回空串但 io::last_error()==0, 以此区分)。
        inline Task<std::string> read_all(std::string_view path)
        {
            std::string out;
            stat_info st = stat(path);
            if (!st.exists || st.is_dir)
            {
                io::set_error(st.exists ? (int)EACCES
#ifdef _WIN32
                                        : (int)ERROR_FILE_NOT_FOUND
#else
                                        : ENOENT
#endif
                );
                co_return out;
            }
            File f = co_await open(path, mode::read);
            if (!f.valid())
                co_return out;
            out.resize((size_t)st.size);
            uint64_t off = 0;
            while (off < st.size)
            {
                int n = co_await f.read_at(out.data() + off,
                                           (size_t)(st.size - off), off);
                if (n < 0)
                {
                    out.clear();
                    co_return out;
                }
                if (n == 0)
                    break; // EOF (文件被并发截断): 返回已读部分
                off += (uint64_t)n;
            }
            co_return out;
        }

        /// 一次性写入整个文件 (默认 "w" 语义: 创建或截断)。
        /// 返回 false 时错误码在 io::last_error()。
        inline Task<bool> write_all(std::string_view path, std::string_view data,
                                    mode m = mode::write)
        {
            File f = co_await open(path, m);
            if (!f.valid())
                co_return false;
            uint64_t off = 0;
            while (off < data.size())
            {
                int n = co_await f.write_at(data.data() + off,
                                            (size_t)(data.size() - off), off);
                if (n <= 0)
                    co_return false;
                off += (uint64_t)n;
            }
            co_return true;
        }

    } // namespace fs
} // namespace coro
