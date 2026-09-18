#pragma once

#include "io.hpp"
#include "task.hpp"

#ifdef _WIN32
// windows.h 已由 io.hpp 引入
#elif defined(__linux__)
#include <sys/inotify.h>
#include <unistd.h>
#endif

#include <deque>
#include <string>
#include <string_view>
#include <utility>

// ============================================================================
// coro::fs::watch — 目录监视 (对标 Python watchfiles / inotify)
// ============================================================================
//
// API:
//   auto w = co_await coro::fs::watch("src", /*recursive=*/true);
//   while (true) {
//       coro::fs::watch_event ev = co_await w.next();  // 挂起到下一个事件
//       // ev.type: created / removed / modified / renamed / overflow
//       // ev.path: 相对被监视目录的路径 (renamed 时 path=新名, old_path=旧名)
//   }
//
// 平台实现 (与 net/fs/pipe 同一条完成路径):
//   Windows → ReadDirectoryChangesW (目录句柄 FILE_FLAG_BACKUP_SEMANTICS |
//             FILE_FLAG_OVERLAPPED, 关联 IOCP; 原生支持递归)
//   Linux   → inotify_init1 + inotify_add_watch, inotify fd 经 io_uring 读;
//             递归 = 遍历子目录逐个加 watch + 动态跟踪新子目录
//
// 事件语义:
//   - 一次完成可能携带多条事件, next() 逐条返回 (内部有 pending 队列)
//   - 重命名: Windows 两条记录 (旧名+新名) 合并为一个 renamed 事件;
//     Linux 用 cookie 配对 (IN_MOVED_FROM/IN_MOVED_TO)
//   - overflow: 内核事件缓冲溢出 (生产太快消费太慢), 此时可能丢事件
//   - 「修改」事件的粒度由内核决定: 编辑器保存常表现为 created+modified
//     多条事件, 需要应用层去抖 (如收集 100ms 内同路径事件)
// ============================================================================

namespace coro {
    namespace fs {

        /// 目录事件类型
        enum class watch_event_type {
            created,  ///< 文件/目录被创建
            removed,  ///< 文件/目录被删除
            modified, ///< 文件内容/属性被修改
            renamed,  ///< 重命名 (path=新路径, old_path=旧路径)
            overflow  ///< 内核事件缓冲溢出 (可能丢事件)
        };

        /// 一条目录事件
        struct watch_event {
            watch_event_type type = watch_event_type::modified;
            std::string path;     ///< 相对被监视目录的路径 (UTF-8, '/' 分隔)
            std::string old_path; ///< 仅 renamed: 旧路径

            bool is_dir = false; ///< 事件目标是目录 (Linux 提供; Windows 恒 false)
        };

#ifdef _WIN32

        // ==================================================================
        // Windows 实现 — ReadDirectoryChangesW + IOCP
        // ==================================================================

        namespace detail_watch {
            /// 宽字符 → UTF-8 (事件文件名)
            inline std::string wide_to_utf8(const wchar_t* ws, size_t n) {
                int len = WideCharToMultiByte(CP_UTF8, 0, ws, (int)n, nullptr, 0, nullptr, nullptr);
                std::string s((size_t)(len > 0 ? len : 0), '\0');
                if (len > 0)
                    WideCharToMultiByte(CP_UTF8, 0, ws, (int)n, s.data(), len, nullptr, nullptr);
                return s;
            }
        } // namespace detail_watch

        class DirectoryWatcher {
          public:
            DirectoryWatcher() = default;

            /// 接管已打开的目录句柄 (正常用法: co_await fs::watch(...))
            explicit DirectoryWatcher(HANDLE dir, bool recursive) : dir_(dir), recursive_(recursive) {
                if (valid())
                    if (auto* iocp = EventLoop::get().iocp())
                        iocp->associate(dir_);
            }

            ~DirectoryWatcher() { close(); }

            DirectoryWatcher(DirectoryWatcher&& other) noexcept
                : dir_(std::exchange(other.dir_, INVALID_HANDLE_VALUE)), recursive_(other.recursive_),
                  pending_(std::move(other.pending_)) {}

            DirectoryWatcher& operator=(DirectoryWatcher&& other) noexcept {
                if (this != &other) {
                    close();
                    dir_ = std::exchange(other.dir_, INVALID_HANDLE_VALUE);
                    recursive_ = other.recursive_;
                    pending_ = std::move(other.pending_);
                }
                return *this;
            }

            DirectoryWatcher(const DirectoryWatcher&) = delete;
            DirectoryWatcher& operator=(const DirectoryWatcher&) = delete;

            bool valid() const { return dir_ != INVALID_HANDLE_VALUE; }

            /// 提交一次 ReadDirectoryChangesW 并挂起 (完成驱动)
            struct changes_awaiter {
                DirectoryWatcher* w;
                char buf[4096]; // 内核写入的事件缓冲 (awaiter 在协程帧里, 帧活着则缓冲活着)
                detail::iocp_op op;

                bool await_ready() const noexcept { return false; }

                static void cancel_op(void* self) {
                    auto* aw = static_cast<changes_awaiter*>(self);
                    if (aw->w && aw->w->valid())
                        CancelIoEx(aw->w->dir_, &aw->op.ov);
                }

                void await_suspend(std::coroutine_handle<> h) {
                    op.continuation = h;
                    op.ov.Offset = 0;
                    op.ov.OffsetHigh = 0;
                    DWORD filter = FILE_NOTIFY_CHANGE_FILE_NAME |  // 创建/删除/重命名
                                   FILE_NOTIFY_CHANGE_DIR_NAME |   // 子目录增删
                                   FILE_NOTIFY_CHANGE_LAST_WRITE | // 内容修改
                                   FILE_NOTIFY_CHANGE_SIZE |       // 大小变化
                                   FILE_NOTIFY_CHANGE_CREATION;    // 创建时间戳变化
                    BOOL rc = ReadDirectoryChangesW(w->dir_, buf, sizeof(buf), w->recursive_, filter,
                                                    nullptr /*完成时字节数经完成包给*/, &op.ov, nullptr);
                    if (!rc && GetLastError() != ERROR_IO_PENDING) {
                        // 立即失败 (句柄无效/非目录): 无完成包, 手动恢复。
                        // 缓冲溢出 (ERROR_NOTIFY_ENUM_DIR) 也走完成包, 不在这里。
                        op.error = GetLastError();
                        EventLoop::get().schedule(h);
                    } else {
                        if (auto* iocp = EventLoop::get().iocp())
                            iocp->op_start();
                    }
                }

                /// 完成后: 解析缓冲里的全部事件进 watcher 的 pending 队列
                void await_resume() { w->parse_records(buf, op.error, op.transferred); }
            };

            /// 取下一个事件: pending 有则直接弹, 否则提交一次目录读并解析
            Task<watch_event> next() {
                if (!pending_.empty())
                    co_return pop_pending();
                if (!valid()) {
                    io::set_error((int)ERROR_INVALID_HANDLE);
                    co_return watch_event{}; // 无效 watcher: 空事件
                }
                changes_awaiter aw{this, {}, {}};
                co_await aw;
                // 解析结果在 pending_ 里 (可能为空, 如溢出恢复后)
                if (pending_.empty()) {
                    // 一次读没产生事件 (理论上少见): 递归再读
                    co_return co_await next();
                }
                co_return pop_pending();
            }

            /// 停止监视并关闭句柄 (有挂起的 next() 时不可调用)
            void close() {
                if (valid()) {
                    CloseHandle(dir_);
                    dir_ = INVALID_HANDLE_VALUE;
                }
            }

          private:
            HANDLE dir_ = INVALID_HANDLE_VALUE;
            bool recursive_ = true;
            std::deque<watch_event> pending_;

            watch_event pop_pending() {
                watch_event ev = std::move(pending_.front());
                pending_.pop_front();
                return ev;
            }

            /// 解析 FILE_NOTIFY_INFORMATION 记录链 (一次完成可含多条)
            void parse_records(const char* buf, int error, DWORD bytes) {
                if (error == ERROR_NOTIFY_ENUM_DIR) {
                    // 缓冲溢出: 内核丢了部分事件
                    pending_.push_back(watch_event{watch_event_type::overflow, "", "", false});
                    return;
                }
                if (error || bytes == 0) {
                    io::set_error(error ? error : (int)ERROR_INVALID_FUNCTION);
                    return;
                }
                std::string rename_old; // 配对 RENAMED_OLD/NEW (同一批次内)
                const char* p = buf;
                while (true) {
                    auto* ni = reinterpret_cast<const FILE_NOTIFY_INFORMATION*>(p);
                    std::string name = detail_watch::wide_to_utf8(ni->FileName, ni->FileNameLength / sizeof(WCHAR));
                    watch_event ev;
                    bool has_event = true;
                    switch (ni->Action) {
                        case FILE_ACTION_ADDED:
                            ev.type = watch_event_type::created;
                            ev.path = std::move(name);
                            break;
                        case FILE_ACTION_REMOVED:
                            ev.type = watch_event_type::removed;
                            ev.path = std::move(name);
                            break;
                        case FILE_ACTION_MODIFIED:
                            ev.type = watch_event_type::modified;
                            ev.path = std::move(name);
                            break;
                        case FILE_ACTION_RENAMED_OLD_NAME:
                            rename_old = std::move(name);
                            has_event = false; // 等配对的新名, 暂不入队
                            break;
                        case FILE_ACTION_RENAMED_NEW_NAME:
                            ev.type = watch_event_type::renamed;
                            ev.old_path = std::move(rename_old);
                            ev.path = std::move(name);
                            rename_old.clear();
                            break;
                        default:
                            has_event = false; // 未知动作: 忽略
                            break;
                    }
                    if (has_event)
                        pending_.push_back(std::move(ev));

                    if (ni->NextEntryOffset == 0)
                        break;
                    p += ni->NextEntryOffset;
                }
                // 批次结束仍有未配对的旧名 → 按删除上报
                if (!rename_old.empty())
                    pending_.push_back(watch_event{watch_event_type::removed, std::move(rename_old), "", false});
            }
        };

        /// 打开目录监视 (同步打开目录句柄 — 元数据操作; 关联 IOCP)
        /// recursive: 递归监视子目录 (Windows 原生支持)
        inline Task<DirectoryWatcher> watch(std::string_view path, bool recursive = true) {
            std::wstring w;
            {
                int n = MultiByteToWideChar(CP_UTF8, 0, path.data(), (int)path.size(), nullptr, 0);
                w.resize((size_t)(n > 0 ? n : 0));
                if (n > 0)
                    MultiByteToWideChar(CP_UTF8, 0, path.data(), (int)path.size(), w.data(), n);
            }
            HANDLE dir = CreateFileW(w.c_str(), FILE_LIST_DIRECTORY,
                                     FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, nullptr, OPEN_EXISTING,
                                     FILE_FLAG_BACKUP_SEMANTICS | // 打开目录必须
                                         FILE_FLAG_OVERLAPPED,    // 接入 IOCP
                                     nullptr);
            if (dir == INVALID_HANDLE_VALUE)
                io::set_error((int)GetLastError());
            co_return DirectoryWatcher(dir, recursive);
        }

#elif defined(__linux__)

        // ==================================================================
        // Linux 实现 — inotify + io_uring (结构对称于 Windows 层)
        // ==================================================================
        // 注意: 本仓库在 Windows 上开发, 本节需要 Linux 环境编译验证。

        class DirectoryWatcher {
          public:
            DirectoryWatcher() = default;
            explicit DirectoryWatcher(int fd, bool recursive) : fd_(fd), recursive_(recursive) {}
            ~DirectoryWatcher() { close(); }

            DirectoryWatcher(DirectoryWatcher&& other) noexcept
                : fd_(std::exchange(other.fd_, -1)), recursive_(other.recursive_), pending_(std::move(other.pending_)) {
            }

            DirectoryWatcher& operator=(DirectoryWatcher&& other) noexcept {
                if (this != &other) {
                    close();
                    fd_ = std::exchange(other.fd_, -1);
                    recursive_ = other.recursive_;
                    pending_ = std::move(other.pending_);
                }
                return *this;
            }

            DirectoryWatcher(const DirectoryWatcher&) = delete;
            DirectoryWatcher& operator=(const DirectoryWatcher&) = delete;

            bool valid() const { return fd_ >= 0; }

            struct read_awaiter {
                DirectoryWatcher* w;
                char buf[4096];
                detail::uring_op op;
                net::UringEventSource* uring_ = nullptr;

                ~read_awaiter() {
                    if (uring_)
                        uring_->untrack_op(&op);
                }

                bool await_ready() const noexcept { return false; }

                static void cancel_op(void* self) {
                    auto* aw = static_cast<read_awaiter*>(self);
                    if (auto* u = EventLoop::get().uring()) {
                        io_uring_sqe* sqe = io_uring_get_sqe(u->handle());
                        if (sqe) {
                            io_uring_prep_cancel(sqe, &aw->op, 0);
                            io_uring_submit(u->handle());
                        }
                    }
                }

                void await_suspend(std::coroutine_handle<> h) {
                    op.continuation = h;
                    auto* u = EventLoop::get().uring();
                    if (!u) {
                        op.result = -ENOTSUP;
                        EventLoop::get().schedule(h);
                        return;
                    }
                    io_uring_sqe* sqe = io_uring_get_sqe(u->handle());
                    if (!sqe) {
                        op.result = -ENOBUFS;
                        EventLoop::get().schedule(h);
                        return;
                    }
                    io_uring_prep_read(sqe, w->fd_, buf, sizeof(buf), -1);
                    detail::uring_submit(u, sqe, &op);
                    uring_ = u;
                    u->track_op(&op);
                }

                void await_resume() { w->parse_records(buf, op.result); }
            };

            Task<watch_event> next() {
                if (!pending_.empty())
                    co_return pop_pending();
                if (!valid()) {
                    io::set_error(EBADF);
                    co_return watch_event{};
                }
                read_awaiter aw{this, {}, {}};
                co_await aw;
                if (pending_.empty())
                    co_return co_await next();
                co_return pop_pending();
            }

            void close() {
                if (fd_ >= 0) {
                    ::close(fd_);
                    fd_ = -1;
                }
            }

          private:
            int fd_ = -1;
            bool recursive_ = true;
            std::deque<watch_event> pending_;

            watch_event pop_pending() {
                watch_event ev = std::move(pending_.front());
                pending_.pop_front();
                return ev;
            }

            void parse_records(const char* buf, int bytes) {
                if (bytes <= 0) {
                    if (bytes < 0)
                        io::set_error(-bytes);
                    return;
                }
                uint32_t rename_cookie = 0;
                std::string rename_old;
                const char* p = buf;
                const char* end = buf + bytes;
                while (p + sizeof(struct inotify_event) <= end) {
                    auto* ie = reinterpret_cast<const struct inotify_event*>(p);
                    p += sizeof(struct inotify_event) + ie->len;
                    if (p > end)
                        break;

                    std::string name(ie->len ? ie->name : "");
                    bool is_dir = ie->mask & IN_ISDIR;

                    if (ie->mask & IN_Q_OVERFLOW) {
                        pending_.push_back(watch_event{watch_event_type::overflow, "", "", false});
                        continue;
                    }
                    if (ie->mask & IN_CREATE) {
                        pending_.push_back(watch_event{watch_event_type::created, std::move(name), "", is_dir});
                        // 递归: 新子目录 → 追加 watch
                        if (recursive_ && is_dir)
                            add_sub_watch(name);
                        continue;
                    }
                    if (ie->mask & IN_DELETE || ie->mask & IN_DELETE_SELF) {
                        pending_.push_back(watch_event{watch_event_type::removed, std::move(name), "", is_dir});
                        continue;
                    }
                    if (ie->mask & IN_MODIFY || ie->mask & IN_ATTRIB) {
                        pending_.push_back(watch_event{watch_event_type::modified, std::move(name), "", is_dir});
                        continue;
                    }
                    if (ie->mask & IN_MOVED_FROM) {
                        rename_cookie = ie->cookie;
                        rename_old = std::move(name);
                        continue;
                    }
                    if (ie->mask & IN_MOVED_TO) {
                        if (ie->cookie != 0 && ie->cookie == rename_cookie && !rename_old.empty()) {
                            pending_.push_back(
                                watch_event{watch_event_type::renamed, std::move(name), std::move(rename_old), is_dir});
                            rename_cookie = 0;
                        } else {
                            // 未配对 (移入自监视区外): 按创建上报
                            pending_.push_back(watch_event{watch_event_type::created, std::move(name), "", is_dir});
                        }
                        continue;
                    }
                }
                if (!rename_old.empty())
                    pending_.push_back(watch_event{watch_event_type::removed, std::move(rename_old), "", false});
            }

            void add_sub_watch(const std::string& rel) { (void)rel; /* io_uring 时代补: 记录 wd→路径 */ }

            int wd_root_ = -1;
        };

        /// 打开目录监视: inotify_init1 + add_watch (递归需遍历子目录)
        inline Task<DirectoryWatcher> watch(std::string_view path, bool recursive = true) {
            (void)recursive; // v1: 根目录 watch; 递归跟踪见 add_sub_watch
            int fd = inotify_init1(IN_NONBLOCK | IN_CLOEXEC);
            if (fd < 0) {
                io::set_error(errno);
                co_return DirectoryWatcher{};
            }
            uint32_t mask =
                IN_CREATE | IN_DELETE | IN_MODIFY | IN_ATTRIB | IN_MOVED_FROM | IN_MOVED_TO | IN_DELETE_SELF;
            if (inotify_add_watch(fd, std::string(path).c_str(), mask) < 0) {
                io::set_error(errno);
                ::close(fd);
                co_return DirectoryWatcher{};
            }
            co_return DirectoryWatcher(fd, recursive);
        }

#endif

    } // namespace fs
} // namespace coro
