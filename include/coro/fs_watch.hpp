#pragma once

#include "io.hpp"
#include "task.hpp"
#if defined(CORO_URING_ENABLED)
#include "wait.hpp"
#endif

#ifdef _WIN32
// windows.h 已由 io.hpp 引入
#elif defined(CORO_URING_ENABLED)
#include <cerrno>
#include <poll.h>
#include <sys/inotify.h>
#include <unistd.h>
#endif

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <deque>
#include <filesystem>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

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
//   Linux   → inotify_init1 + inotify_add_watch, io_uring 等 POLLIN 后非阻塞读 fd;
//             递归模式维护 wd→相对路径映射，并动态跟踪新增/移入的子目录。
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
                if (valid()) {
                    auto* iocp = detail::current_iocp();
                    if (!iocp || !iocp->associate(dir_)) {
                        fail(iocp ? (int)GetLastError() : (int)ERROR_NOT_SUPPORTED);
                    }
                }
            }

            ~DirectoryWatcher() { close(); }

            DirectoryWatcher(DirectoryWatcher&& other) noexcept
                : dir_(std::exchange(other.dir_, INVALID_HANDLE_VALUE)), recursive_(other.recursive_),
                  pending_(std::move(other.pending_)), pending_rename_old_(std::move(other.pending_rename_old_)),
                  terminal_error_(std::exchange(other.terminal_error_, 0)) {}

            DirectoryWatcher& operator=(DirectoryWatcher&& other) noexcept {
                if (this != &other) {
                    close();
                    dir_ = std::exchange(other.dir_, INVALID_HANDLE_VALUE);
                    recursive_ = other.recursive_;
                    pending_ = std::move(other.pending_);
                    pending_rename_old_ = std::move(other.pending_rename_old_);
                    terminal_error_ = std::exchange(other.terminal_error_, 0);
                }
                return *this;
            }

            DirectoryWatcher(const DirectoryWatcher&) = delete;
            DirectoryWatcher& operator=(const DirectoryWatcher&) = delete;

            bool valid() const { return dir_ != INVALID_HANDLE_VALUE; }
            int error() const noexcept { return terminal_error_; }

            /// 提交一次 ReadDirectoryChangesW 并挂起 (完成驱动)。
            /// 事件缓冲挂在 watcher 上复用: 事件流消费是 `while (true) next()`
            /// 热循环, 每次挂起都新分配 4KB 协程帧与库其余部分"热路径零分配"
            /// 的水准不符。watcher 按设计是单消费者 (pending 队列非并发),
            /// 复用无竞争。
            struct changes_awaiter {
                DirectoryWatcher* w;
                detail::iocp_op op;

                bool await_ready() const noexcept { return false; }

                static void cancel_op(void* self) {
                    auto* aw = static_cast<changes_awaiter*>(self);
                    if (aw->w && aw->w->valid())
                        CancelIoEx(aw->w->dir_, &aw->op.ov);
                }

                void await_suspend(std::coroutine_handle<> h) {
                    op.continuation = h;
                    auto* iocp = detail::current_iocp();
                    if (!iocp) {
                        op.error = ERROR_NOT_SUPPORTED;
                        EventLoop::get().schedule(h);
                        return;
                    }
                    op.ov.Offset = 0;
                    op.ov.OffsetHigh = 0;
                    DWORD filter = FILE_NOTIFY_CHANGE_FILE_NAME |  // 创建/删除/重命名
                                   FILE_NOTIFY_CHANGE_DIR_NAME |   // 子目录增删
                                   FILE_NOTIFY_CHANGE_LAST_WRITE | // 内容修改
                                   FILE_NOTIFY_CHANGE_SIZE |       // 大小变化
                                   FILE_NOTIFY_CHANGE_CREATION;    // 创建时间戳变化
                    BOOL rc =
                        ReadDirectoryChangesW(w->dir_, w->event_buf_.data(), (DWORD)w->event_buf_.size(), w->recursive_,
                                              filter, nullptr /*完成时字节数经完成包给*/, &op.ov, nullptr);
                    if (!rc && GetLastError() != ERROR_IO_PENDING) {
                        // 立即失败 (句柄无效/非目录): 无完成包, 手动恢复。
                        // 缓冲溢出 (ERROR_NOTIFY_ENUM_DIR) 也走完成包, 不在这里。
                        op.error = GetLastError();
                        EventLoop::get().schedule(h);
                    } else {
                        iocp->op_start();
                    }
                }

                /// 完成后: 解析缓冲里的全部事件进 watcher 的 pending 队列
                void await_resume() { w->parse_records(w->event_buf_.data(), op.error, op.transferred); }
            };

            /// 取下一个事件: pending 有则直接弹, 否则提交一次目录读并解析
            Task<watch_event> next() {
                while (true) {
                    if (!pending_.empty())
                        co_return pop_pending();
                    if (!valid()) {
                        io::set_error(terminal_error_ ? terminal_error_ : (int)ERROR_INVALID_HANDLE);
                        co_return watch_event{};
                    }
                    changes_awaiter aw{this, {}};
                    co_await aw;
                    // 无事件时复用当前协程帧；终止错误已关闭 watcher。
                }
            }

            /// 停止监视并关闭句柄。挂起的 next() 会以 ERROR_OPERATION_ABORTED
            /// 完成包唤醒并返回默认事件。
            void close() {
                if (valid()) {
                    // 与 net/pipe/fs 层同一合同: 先显式取消挂起的重叠读再关句柄
                    // (仅 CloseHandle 不承诺完成包投递)。
                    CancelIoEx(dir_, nullptr);
                    CloseHandle(dir_);
                    dir_ = INVALID_HANDLE_VALUE;
                }
            }

          private:
            HANDLE dir_ = INVALID_HANDLE_VALUE;
            bool recursive_ = true;
            /// ReadDirectoryChangesW 事件缓冲 (跨 next() 复用, 单消费者无竞争)
            std::vector<char> event_buf_ = std::vector<char>(4096);
            std::deque<watch_event> pending_;
            /// 跨完成批次的 RENAMED_OLD 待配对名 (内核可能把 OLD/NEW 拆成两个包)
            std::string pending_rename_old_;
            int terminal_error_ = 0;

            void fail(int error) {
                terminal_error_ = error;
                pending_.clear();
                close();
                io::set_error(error);
            }

            watch_event pop_pending() {
                watch_event ev = std::move(pending_.front());
                pending_.pop_front();
                return ev;
            }

            /// 解析 FILE_NOTIFY_INFORMATION 记录链 (一次完成可含多条)
            void parse_records(const char* buf, int error, DWORD bytes) {
                if (error == ERROR_NOTIFY_ENUM_DIR || (!error && bytes == 0)) {
                    // 缓冲溢出: 内核丢了部分事件
                    pending_.push_back(watch_event{watch_event_type::overflow, "", "", false});
                    return;
                }
                if (error || bytes > 4096) {
                    fail(error ? error : (int)ERROR_INVALID_DATA);
                    return;
                }
                const char* p = buf;
                const char* end = buf + bytes;
                constexpr size_t header_size = offsetof(FILE_NOTIFY_INFORMATION, FileName);
                while (true) {
                    if (static_cast<size_t>(end - p) < header_size) {
                        fail(ERROR_INVALID_DATA);
                        return;
                    }
                    auto* ni = reinterpret_cast<const FILE_NOTIFY_INFORMATION*>(p);
                    if (ni->FileNameLength % sizeof(WCHAR) != 0 ||
                        ni->FileNameLength > static_cast<size_t>(end - p) - header_size) {
                        fail(ERROR_INVALID_DATA);
                        return;
                    }
                    std::string name = detail_watch::wide_to_utf8(ni->FileName, ni->FileNameLength / sizeof(WCHAR));
                    std::replace(name.begin(), name.end(), '\\', '/');
                    watch_event ev;
                    bool has_event = true;
                    // OLD/NEW 可能被内核拆到两次完成里: 待配对的旧名存在成员上, 不再
                    // 每批末尾当成 removed 上报 (旧实现会伪造一个误报的删除)。
                    if (ni->Action != FILE_ACTION_RENAMED_NEW_NAME && !pending_rename_old_.empty()) {
                        pending_.push_back(
                            watch_event{watch_event_type::removed, std::move(pending_rename_old_), "", false});
                        pending_rename_old_.clear();
                    }
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
                            if (!pending_rename_old_.empty()) {
                                // 连续两个 OLD: 前一个无法配对 → 明确降级为删除
                                pending_.push_back(
                                    watch_event{watch_event_type::removed, std::move(pending_rename_old_), "", false});
                            }
                            pending_rename_old_ = std::move(name);
                            has_event = false; // 等配对的新名
                            break;
                        case FILE_ACTION_RENAMED_NEW_NAME:
                            if (pending_rename_old_.empty()) {
                                // 没有旧名可配 (跨批次丢失或监视从中间开始):
                                // 目标确实出现了, 按创建上报, 不伪造一个空 old_path 的 rename。
                                ev.type = watch_event_type::created;
                                ev.path = std::move(name);
                            } else {
                                ev.type = watch_event_type::renamed;
                                ev.old_path = std::move(pending_rename_old_);
                                ev.path = std::move(name);
                                pending_rename_old_.clear();
                            }
                            break;
                        default:
                            has_event = false; // 未知动作: 忽略
                            break;
                    }
                    if (has_event)
                        pending_.push_back(std::move(ev));

                    if (ni->NextEntryOffset == 0)
                        break;
                    if (ni->NextEntryOffset < header_size + ni->FileNameLength ||
                        ni->NextEntryOffset >= static_cast<size_t>(end - p) ||
                        ni->NextEntryOffset % alignof(FILE_NOTIFY_INFORMATION) != 0) {
                        fail(ERROR_INVALID_DATA);
                        return;
                    }
                    p += ni->NextEntryOffset;
                }
            }
        };

        /// 打开目录监视 (同步打开目录句柄 — 元数据操作; 关联 IOCP)
        /// recursive: 递归监视子目录 (Windows 原生支持)
        ///
        /// 注意入口与实现的分离: watch() 是**普通函数**, 在调用时就把路径复制成
        /// owning string; 而协程体要到首次恢复才执行 —— 若它自己读 string_view,
        /// 调用点传的临时 string 早已销毁 (真实悬空读取)。
        inline Task<DirectoryWatcher> watch_owned(std::string path, bool recursive = true);

        inline Task<DirectoryWatcher> watch(std::string_view path, bool recursive = true) {
            return watch_owned(std::string(path), recursive);
        }

        // cppcheck-suppress passedByValue: 协程入口 —— path 必须按值进协程帧,
        // 调用点的临时 string 在首次恢复前就析构 (const& 反而制造悬空)
        inline Task<DirectoryWatcher> watch_owned(std::string path, bool recursive) {
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

#elif defined(CORO_URING_ENABLED)

        // ==================================================================
        // Linux 实现 — inotify + io_uring (结构对称于 Windows 层)
        // ==================================================================
        // 注意: 本仓库在 Windows 上开发, 本节需要 Linux 环境编译验证。

        class DirectoryWatcher {
          public:
            DirectoryWatcher() = default;
            explicit DirectoryWatcher(int fd, std::string root, bool recursive) : fd_(fd), recursive_(recursive) {
                std::error_code ec;
                // 保留局部错误码检查，失败时设置 I/O 错误并关闭已接管的 fd。
                // cppcheck-suppress useInitializationList
                root_path_ = std::filesystem::absolute(std::filesystem::path(std::move(root)), ec).lexically_normal();
                if (ec) {
                    io::set_error(ec.value());
                    close();
                    return;
                }
                if (!add_watch_tree(""))
                    close();
            }
            ~DirectoryWatcher() { close(); }

            DirectoryWatcher(DirectoryWatcher&& other) noexcept
                : fd_(std::exchange(other.fd_, -1)), recursive_(other.recursive_),
                  root_path_(std::move(other.root_path_)), event_buf_(std::move(other.event_buf_)),
                  pending_(std::move(other.pending_)), wd_paths_(std::move(other.wd_paths_)),
                  pending_moves_(std::move(other.pending_moves_)),
                  terminal_error_(std::exchange(other.terminal_error_, 0)),
                  needs_rescan_(std::exchange(other.needs_rescan_, false)) {}

            DirectoryWatcher& operator=(DirectoryWatcher&& other) noexcept {
                if (this != &other) {
                    close();
                    fd_ = std::exchange(other.fd_, -1);
                    recursive_ = other.recursive_;
                    root_path_ = std::move(other.root_path_);
                    event_buf_ = std::move(other.event_buf_);
                    pending_ = std::move(other.pending_);
                    wd_paths_ = std::move(other.wd_paths_);
                    pending_moves_ = std::move(other.pending_moves_);
                    terminal_error_ = std::exchange(other.terminal_error_, 0);
                    needs_rescan_ = std::exchange(other.needs_rescan_, false);
                }
                return *this;
            }

            DirectoryWatcher(const DirectoryWatcher&) = delete;
            DirectoryWatcher& operator=(const DirectoryWatcher&) = delete;

            bool valid() const { return fd_ >= 0; }

            /// 终止性错误 (读失败/句柄失效); 0 = 无。与 Windows 层对齐:
            /// 失效后 next() 返回默认事件, 但调用方可以凭这个值区分“真没事件”。
            int error() const noexcept { return terminal_error_; }

            /// overflow 之后 wd→路径映射可能已与真实目录树失配,
            /// 调用方应重建 watcher (或自行重扫) 后再继续消费。
            bool needs_rescan() const noexcept { return needs_rescan_; }

            struct read_awaiter {
                DirectoryWatcher* w;
                detail::uring_op op;
                net::UringEventSource* uring_ = nullptr;

                ~read_awaiter() {
                    if (uring_)
                        uring_->untrack_op(&op);
                }

                bool await_ready() const noexcept { return false; }

                static void cancel_op(void* self) {
                    auto* aw = static_cast<read_awaiter*>(self);
                    if (auto* u = detail::current_uring())
                        detail::submit_uring_cancel(u, &aw->op);
                }

                void await_suspend(std::coroutine_handle<> h) {
                    op.continuation = h;
                    auto* u = detail::current_uring();
                    if (!u) {
                        op.result = -ENOTSUP;
                        op.error = ENOTSUP;
                        EventLoop::get().schedule(h);
                        return;
                    }
                    io_uring_sqe* sqe = io_uring_get_sqe(u->handle());
                    if (!sqe) {
                        op.result = -ENOBUFS;
                        op.error = ENOBUFS;
                        EventLoop::get().schedule(h);
                        return;
                    }
                    // inotify fd 必须保持 O_NONBLOCK，而直接 IORING_OP_READ 在
                    // 部分内核上会立即返回 EAGAIN。先用 poll 等就绪，
                    // 完成时再在 loop 线程做一次不阻塞 read。
                    op.fd = w->fd_; // close 前可按 fd 批量取消
                    io_uring_prep_poll_add(sqe, w->fd_, POLLIN);
                    detail::uring_submit(u, sqe, &op);
                    uring_ = u;
                    u->track_op(&op);
                }

                void await_resume() {
                    if (op.result < 0) {
                        w->parse_records(w->event_buf_.data(), op.result);
                        return;
                    }

                    ssize_t bytes;
                    do {
                        bytes = ::read(w->fd_, w->event_buf_.data(), w->event_buf_.size());
                    } while (bytes < 0 && errno == EINTR);
                    if (bytes < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
                        return; // 就绪到 read 之间被消费：重新等 poll
                    w->parse_records(w->event_buf_.data(), bytes < 0 ? -errno : static_cast<int>(bytes));
                }
            };

            Task<watch_event> next() {
                while (true) {
                    if (!pending_.empty())
                        co_return pop_pending();
                    if (!valid()) {
                        io::set_error(terminal_error_ ? terminal_error_ : EBADF);
                        co_return watch_event{};
                    }

                    if (pending_moves_.empty()) {
                        co_await read_batch();
                    } else {
                        // MOVED_FROM/MOVED_TO 可能被拆到两次 read。短暂等待
                        // 下一批；超时则把未配对项当作移出/删除。
                        try {
                            co_await coro::wait_for(read_batch(), std::chrono::milliseconds(20));
                        } catch (const TimeoutError&) {
                            flush_pending_moves();
                        }
                    }
                }
            }

            /// 关闭监视。Linux 侧: 挂起的 POLL 持有 inotify fd 引用, 仅
            /// ::close 不会让它完成 (事件循环永久等待 + 帧泄漏) —— 先按 fd 取消。
            void close() {
                if (fd_ >= 0) {
                    if (auto* u = detail::current_uring())
                        u->cancel_fd_ops(fd_);
                    ::close(fd_);
                    fd_ = -1;
                }
                wd_paths_.clear();
                pending_moves_.clear();
            }

          private:
            struct pending_move {
                std::string path;
                bool is_dir = false;
            };

            int fd_ = -1;
            bool recursive_ = true;
            std::filesystem::path root_path_;
            /// inotify 事件读取缓冲 (跨批次复用, 单消费者无竞争;
            /// 避免每批次在协程帧里重新分配 64KB)
            std::vector<char> event_buf_ = std::vector<char>(64 * 1024);
            std::deque<watch_event> pending_;
            std::unordered_map<int, std::string> wd_paths_;
            std::unordered_map<uint32_t, pending_move> pending_moves_;
            int terminal_error_ = 0;    ///< 终止性读错误 (0 = 无)
            bool needs_rescan_ = false; ///< overflow 后 wd 映射可能已失配

            static constexpr uint32_t watch_mask = IN_CREATE | IN_DELETE | IN_MODIFY | IN_ATTRIB | IN_MOVED_FROM |
                                                   IN_MOVED_TO | IN_DELETE_SELF | IN_MOVE_SELF;

            watch_event pop_pending() {
                watch_event ev = std::move(pending_.front());
                pending_.pop_front();
                return ev;
            }

            Task<> read_batch() {
                read_awaiter aw{this, {}, {}};
                co_await aw;
            }

            static std::string join_relative(const std::string& parent, std::string_view name) {
                if (parent.empty())
                    return std::string(name);
                if (name.empty())
                    return parent;
                return parent + "/" + std::string(name);
            }

            static bool is_same_or_child(std::string_view path, std::string_view prefix) {
                return path == prefix ||
                       (path.size() > prefix.size() && path.starts_with(prefix) && path[prefix.size()] == '/');
            }

            std::filesystem::path full_path(std::string_view relative) const {
                if (relative.empty())
                    return root_path_;
                return root_path_ / std::filesystem::path(relative);
            }

            bool add_watch(std::string relative) {
                const std::string native = full_path(relative).string();
                const int wd = ::inotify_add_watch(fd_, native.c_str(), watch_mask);
                if (wd < 0) {
                    io::set_error(errno);
                    return false;
                }
                std::replace(relative.begin(), relative.end(), '\\', '/');
                wd_paths_[wd] = std::move(relative);
                return true;
            }

            bool add_watch_tree(const std::string& relative) {
                if (!add_watch(relative))
                    return false;
                if (!recursive_)
                    return true;

                std::error_code ec;
                std::filesystem::recursive_directory_iterator it(full_path(relative), ec), end;
                if (ec) {
                    io::set_error(ec.value());
                    return false;
                }
                while (it != end) {
                    const bool symlink = it->is_symlink(ec);
                    if (ec) {
                        io::set_error(ec.value());
                        return false;
                    }
                    if (symlink) {
                        it.disable_recursion_pending();
                    } else {
                        const bool directory = it->is_directory(ec);
                        if (ec) {
                            io::set_error(ec.value());
                            return false;
                        }
                        if (directory) {
                            auto rel = it->path().lexically_relative(root_path_).generic_string();
                            if (!add_watch(std::move(rel)))
                                return false;
                        }
                    }
                    it.increment(ec);
                    if (ec) {
                        io::set_error(ec.value());
                        return false;
                    }
                }
                return true;
            }

            void update_watch_paths(const std::string& old_path, const std::string& new_path) {
                for (auto& [wd, path] : wd_paths_) {
                    (void)wd;
                    if (is_same_or_child(path, old_path))
                        path = new_path + path.substr(old_path.size());
                }
            }

            void remove_watch_tree(const std::string& path) {
                std::vector<int> removed;
                for (const auto& [wd, watched_path] : wd_paths_)
                    if (is_same_or_child(watched_path, path))
                        removed.push_back(wd);
                for (int wd : removed) {
                    wd_paths_.erase(wd);
                    (void)::inotify_rm_watch(fd_, wd);
                }
            }

            void flush_pending_moves() {
                for (auto& [cookie, move] : pending_moves_) {
                    (void)cookie;
                    pending_.push_back(watch_event{watch_event_type::removed, move.path, "", move.is_dir});
                    if (move.is_dir)
                        remove_watch_tree(move.path);
                }
                pending_moves_.clear();
            }

            void parse_records(const char* buf, int bytes) {
                if (bytes <= 0) {
                    if (bytes < 0) {
                        terminal_error_ = -bytes;
                        io::set_error(-bytes);
                        close();
                    }
                    return;
                }
                const char* p = buf;
                const char* end = buf + bytes;
                while (p + sizeof(struct inotify_event) <= end) {
                    auto* ie = reinterpret_cast<const struct inotify_event*>(p);
                    const size_t record_size = sizeof(struct inotify_event) + ie->len;
                    if ((size_t)(end - p) < record_size)
                        break;
                    p += record_size;

                    const std::string name(ie->len ? ie->name : "");
                    const bool is_dir = (ie->mask & IN_ISDIR) != 0;

                    if (ie->mask & IN_Q_OVERFLOW) {
                        pending_moves_.clear();
                        // 内核丢了事件: wd 映射可能已经过时, 标记需要重建索引
                        needs_rescan_ = true;
                        pending_.push_back(watch_event{watch_event_type::overflow, "", "", false});
                        continue;
                    }
                    if (ie->mask & IN_IGNORED) {
                        wd_paths_.erase(ie->wd);
                        continue;
                    }

                    auto parent = wd_paths_.find(ie->wd);
                    if (parent == wd_paths_.end())
                        continue;
                    const std::string path = join_relative(parent->second, name);

                    if (ie->mask & IN_MOVE_SELF) {
                        // 子目录由其父目录的 cookie 事件处理；根目录移动时
                        // 无法知道新路径，按移除上报并结束监视。
                        if (parent->second.empty()) {
                            pending_.push_back(watch_event{watch_event_type::removed, "", "", true});
                            close();
                        }
                        continue;
                    }
                    if (ie->mask & IN_MOVED_FROM) {
                        if (ie->cookie != 0)
                            pending_moves_[ie->cookie] = pending_move{path, is_dir};
                        else
                            pending_.push_back(watch_event{watch_event_type::removed, path, "", is_dir});
                        continue;
                    }
                    if (ie->mask & IN_MOVED_TO) {
                        auto old = pending_moves_.find(ie->cookie);
                        if (ie->cookie != 0 && old != pending_moves_.end()) {
                            const std::string old_path = std::move(old->second.path);
                            const bool moved_dir = is_dir || old->second.is_dir;
                            pending_moves_.erase(old);
                            pending_.push_back(watch_event{watch_event_type::renamed, path, old_path, moved_dir});
                            if (recursive_ && moved_dir)
                                update_watch_paths(old_path, path);
                        } else {
                            pending_.push_back(watch_event{watch_event_type::created, path, "", is_dir});
                            if (recursive_ && is_dir && !add_watch_tree(path))
                                pending_.push_back(watch_event{watch_event_type::overflow, "", "", false});
                        }
                        continue;
                    }
                    if (ie->mask & IN_CREATE) {
                        pending_.push_back(watch_event{watch_event_type::created, path, "", is_dir});
                        if (recursive_ && is_dir && !add_watch_tree(path))
                            pending_.push_back(watch_event{watch_event_type::overflow, "", "", false});
                        continue;
                    }
                    if (ie->mask & IN_DELETE_SELF) {
                        // 子目录的删除已由父 wd 上的 IN_DELETE 上报。
                        if (parent->second.empty()) {
                            pending_.push_back(watch_event{watch_event_type::removed, "", "", true});
                            close();
                        }
                        continue;
                    }
                    if (ie->mask & IN_DELETE) {
                        pending_.push_back(watch_event{watch_event_type::removed, path, "", is_dir});
                        continue;
                    }
                    if (ie->mask & IN_MODIFY || ie->mask & IN_ATTRIB) {
                        pending_.push_back(watch_event{watch_event_type::modified, path, "", is_dir});
                        continue;
                    }
                }
            }
        };

        /// 打开目录监视: inotify_init1 + add_watch
        ///
        /// 入口与实现分离同 Windows 层: watch() 立即复制路径, 协程体只接收 owning string。
        inline Task<DirectoryWatcher> watch_owned(std::string path, bool recursive = true);

        inline Task<DirectoryWatcher> watch(std::string_view path, bool recursive = true) {
            return watch_owned(std::string(path), recursive);
        }

        // cppcheck-suppress passedByValue: 协程入口 —— path 必须按值进协程帧,
        // 调用点的临时 string 在首次恢复前就析构 (const& 反而制造悬空)
        inline Task<DirectoryWatcher> watch_owned(std::string path, bool recursive) {
            int fd = inotify_init1(IN_NONBLOCK | IN_CLOEXEC);
            if (fd < 0) {
                io::set_error(errno);
                co_return DirectoryWatcher{};
            }
            co_return DirectoryWatcher(fd, std::move(path), recursive);
        }

#endif

    } // namespace fs
} // namespace coro
