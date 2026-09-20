#pragma once

#include "io.hpp"
#include "pipe.hpp"
#include "task.hpp"

#ifdef _WIN32
// windows.h 已由 io.hpp 引入
#elif defined(__linux__)
#include <cerrno>
#include <csignal>
#include <cstring>
#include <sys/wait.h>
#include <unistd.h>
#endif

#include <memory>
#include <algorithm>
#include <optional>
#include <string>
#include <utility>
#include <vector>

// ============================================================================
// coro::process — 子进程 (对标 asyncio.subprocess)
// ============================================================================
//
// API:
//   // 启动 (args[0] 是程序, 后续为参数):
//   coro::process::Process p = co_await coro::process::spawn(
//       {"cmd", "/c", "echo hello"}, {.capture_stdout = true});
//
//   // 读子进程输出 (capture_* 开启时):
//   std::string out = co_await p.stdout_pipe()->read_all();
//
//   // 等退出码 (挂起; 多个协程可同时 wait):
//   int code = co_await p.wait();
//
//   // 终止:
//   p.terminate();   // SIGTERM / TerminateProcess
//
// 平台实现:
//   Windows → CreateProcessW + 命名管道 stdio (匿名管道不支持 OVERLAPPED);
//             退出等待 = RegisterWaitForSingleObject (系统线程池回调)
//                        → Promise 跨线程路由 (future.hpp 的逐等待者 loop)
//   Linux   → fork/exec + pipe2(O_NONBLOCK|O_CLOEXEC); 退出等待 = 专用
//             收割线程 (waitpid) → Promise (避开全局信号掩码干扰)
//
// 生命周期: Process 析构不杀进程也不等待 —— 需要明确 wait()/terminate()。
// 子进程 stdin 管道用完应 close(), 否则子进程可能一直等不到 EOF。
// ============================================================================

namespace coro {
    namespace process {

        /// 启动选项
        struct options {
            bool capture_stdin = false;  ///< 创建 stdin 管道 (父进程写, 子进程读)
            bool capture_stdout = false; ///< 创建 stdout 管道 (子进程写, 父进程读)
            bool capture_stderr = false; ///< 创建 stderr 管道
        };

#ifdef _WIN32

        namespace detail_proc {
            struct wait_context {
                HANDLE process;
                std::shared_ptr<Promise<int>> promise;
            };

            /// argv → Windows 命令行 (带引号转义)
            inline std::string quote_arg(const std::string& a) {
                if (!a.empty() && a.find_first_of(" \t\"") == std::string::npos)
                    return a;
                std::string r = "\"";
                size_t backslashes = 0;
                for (char c : a) {
                    if (c == '\\') {
                        backslashes++;
                        continue;
                    }
                    if (c == '"') {
                        r.append(backslashes * 2 + 1, '\\'); // 引号前反斜杠翻倍 + 转义引号
                        r += '"';
                    } else {
                        r.append(backslashes, '\\');
                        r += c;
                    }
                    backslashes = 0;
                }
                r.append(backslashes * 2, '\\'); // 尾部反斜杠翻倍 (不吞闭引号)
                r += '"';
                return r;
            }

            inline std::wstring utf8_to_wide(std::string_view s) {
                int n = MultiByteToWideChar(CP_UTF8, 0, s.data(), (int)s.size(), nullptr, 0);
                std::wstring w((size_t)(n > 0 ? n : 0), L'\0');
                if (n > 0)
                    MultiByteToWideChar(CP_UTF8, 0, s.data(), (int)s.size(), w.data(), n);
                return w;
            }
        } // namespace detail_proc

        class Process {
          public:
            Process() = default;
            ~Process() { cleanup(); }

            Process(Process&& other) noexcept { move_from(std::move(other)); }
            Process& operator=(Process&& other) noexcept {
                if (this != &other) {
                    cleanup();
                    move_from(std::move(other));
                }
                return *this;
            }
            Process(const Process&) = delete;
            Process& operator=(const Process&) = delete;

            bool valid() const { return proc_ != nullptr; }
            int pid() const { return (int)(uintptr_t)proc_; }

            pipe::PipeEnd* stdin_pipe() { return stdin_ ? &*stdin_ : nullptr; }
            pipe::PipeEnd* stdout_pipe() { return stdout_ ? &*stdout_ : nullptr; }
            pipe::PipeEnd* stderr_pipe() { return stderr_ ? &*stderr_ : nullptr; }

            /// 等待退出 (挂起; 返回退出码)。终止后的退出码为进程自定义值。
            Task<int> wait() {
                if (!exit_future_.valid())
                    co_return -1;
                co_return co_await exit_future_;
            }

            /// 启动实现 (静态工厂: 需要访问私有字段填充状态; 正常入口是 spawn())
            static Task<Process> create(std::vector<std::string> args, options opt);

            /// 请求终止 (TerminateProcess, 退出码 1)
            void terminate() {
                if (proc_)
                    TerminateProcess(proc_, 1);
            }

            /// terminate 的别名 (POSIX 语义映射: 强杀)
            void kill() { terminate(); }

          private:
            void cleanup() {
                if (wait_registration_) {
                    UnregisterWaitEx(wait_registration_, INVALID_HANDLE_VALUE);
                    wait_registration_ = nullptr;
                }
                // RegisterWait 回调只借用裸指针；注销并等待回调退出后，
                // 由 Process 持有的 shared_ptr 统一释放上下文。
                wait_context_.reset();
                if (proc_) {
                    CloseHandle(proc_);
                    proc_ = nullptr;
                }
                stdin_.reset();
                stdout_.reset();
                stderr_.reset();
            }

            void move_from(Process&& o) {
                proc_ = std::exchange(o.proc_, nullptr);
                wait_registration_ = std::exchange(o.wait_registration_, nullptr);
                wait_context_ = std::move(o.wait_context_);
                stdin_ = std::move(o.stdin_);
                stdout_ = std::move(o.stdout_);
                stderr_ = std::move(o.stderr_);
                exit_future_ = std::move(o.exit_future_);
            }

            HANDLE proc_ = nullptr;
            HANDLE wait_registration_ = nullptr;
            std::shared_ptr<detail_proc::wait_context> wait_context_;
            std::optional<pipe::PipeEnd> stdin_, stdout_, stderr_;
            Future<int> exit_future_;
        };

        /// 启动实现 (Process 的静态成员, 填充私有状态)。
        /// 任何失败路径: 清理已建资源 + io::set_error + 返回无效 Process。
        inline Task<Process> Process::create(std::vector<std::string> args, options opt) {
            using namespace detail_proc;
            Process out;

            if (args.empty() || args.front().empty()) {
                io::set_error((int)ERROR_INVALID_PARAMETER);
                co_return Process{};
            }

            // ---- stdio 管道 ----
            // 每条管道: 服务端 = 父进程侧 (OVERLAPPED, 不继承),
            //           客户端 = 子进程侧 (继承, 经 STARTUPINFO 传入)
            HANDLE child_stdin = INVALID_HANDLE_VALUE, child_stdout = INVALID_HANDLE_VALUE,
                   child_stderr = INVALID_HANDLE_VALUE;
            SECURITY_ATTRIBUTES inheritable{sizeof(SECURITY_ATTRIBUTES), nullptr, TRUE};

            auto make_pipe = [&](bool capture, DWORD server_access, DWORD client_access,
                                 std::optional<pipe::PipeEnd>* parent_side, HANDLE* child_side) -> bool {
                if (!capture)
                    return true;
                static std::atomic<uint64_t> n{0};
                char name[96];
                std::snprintf(name, sizeof(name), "\\\\.\\pipe\\coro_proc_%lu_%llu",
                              (unsigned long)GetCurrentProcessId(), (unsigned long long)n.fetch_add(1));
                std::wstring wname(name, name + std::strlen(name));
                HANDLE server =
                    CreateNamedPipeW(wname.c_str(), server_access | FILE_FLAG_OVERLAPPED,
                                     PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_WAIT, 1, 4096, 4096, 0, nullptr);
                if (server == INVALID_HANDLE_VALUE) {
                    io::set_error((int)GetLastError());
                    return false;
                }
                HANDLE client = CreateFileW(wname.c_str(), client_access, 0, &inheritable, OPEN_EXISTING,
                                            FILE_FLAG_OVERLAPPED, nullptr);
                if (client == INVALID_HANDLE_VALUE) {
                    io::set_error((int)GetLastError());
                    CloseHandle(server);
                    return false;
                }
                // 立即确认连接 (客户端已连上)
                if (!ConnectNamedPipe(server, nullptr) && GetLastError() != ERROR_PIPE_CONNECTED) {
                    io::set_error((int)GetLastError());
                    CloseHandle(server);
                    CloseHandle(client);
                    return false;
                }
                *parent_side = pipe::PipeEnd(server); // 关联当前 loop 的 IOCP
                *child_side = client;                 // 子进程继承
                return true;
            };

            // stdin:  父写 → 子读   (server OUTBOUND, client GENERIC_READ)
            // stdout: 子写 → 父读   (server INBOUND,  client GENERIC_WRITE)
            if (!make_pipe(opt.capture_stdin, PIPE_ACCESS_OUTBOUND, GENERIC_READ, &out.stdin_, &child_stdin) ||
                !make_pipe(opt.capture_stdout, PIPE_ACCESS_INBOUND, GENERIC_WRITE, &out.stdout_, &child_stdout) ||
                !make_pipe(opt.capture_stderr, PIPE_ACCESS_INBOUND, GENERIC_WRITE, &out.stderr_, &child_stderr)) {
                // 已建资源随 out 析构清理; 子进程侧句柄手动关
                for (HANDLE h : {child_stdin, child_stdout, child_stderr})
                    if (h != INVALID_HANDLE_VALUE)
                        CloseHandle(h);
                co_return Process{};
            }

            // 未捕获的 stdio 使用 NUL。句柄必须可继承，随后通过
            // PROC_THREAD_ATTRIBUTE_HANDLE_LIST 做白名单传递。
            HANDLE fallback = INVALID_HANDLE_VALUE;
            if (child_stdin == INVALID_HANDLE_VALUE || child_stdout == INVALID_HANDLE_VALUE ||
                child_stderr == INVALID_HANDLE_VALUE) {
                fallback = CreateFileW(L"NUL", GENERIC_READ | GENERIC_WRITE, FILE_SHARE_READ | FILE_SHARE_WRITE,
                                       &inheritable, OPEN_EXISTING, 0, nullptr);
                if (fallback == INVALID_HANDLE_VALUE) {
                    const int error = (int)GetLastError();
                    for (HANDLE h : {child_stdin, child_stdout, child_stderr})
                        if (h != INVALID_HANDLE_VALUE)
                            CloseHandle(h);
                    io::set_error(error);
                    co_return Process{};
                }
            }
            if (child_stdin == INVALID_HANDLE_VALUE)
                child_stdin = fallback;
            if (child_stdout == INVALID_HANDLE_VALUE)
                child_stdout = fallback;
            if (child_stderr == INVALID_HANDLE_VALUE)
                child_stderr = fallback;

            // ---- 命令行 + CreateProcessW ----
            std::string cmdline;
            for (size_t i = 0; i < args.size(); ++i) {
                if (i)
                    cmdline += ' ';
                cmdline += detail_proc::quote_arg(args[i]);
            }
            std::wstring wcmd = detail_proc::utf8_to_wide(cmdline);
            // CreateProcessW 要求可写缓冲
            std::vector<wchar_t> cmd_buf(wcmd.begin(), wcmd.end());
            cmd_buf.push_back(L'\0');

            STARTUPINFOEXW si{};
            si.StartupInfo.cb = sizeof(si);
            si.StartupInfo.dwFlags = STARTF_USESTDHANDLES;
            si.StartupInfo.hStdInput = child_stdin;
            si.StartupInfo.hStdOutput = child_stdout;
            si.StartupInfo.hStdError = child_stderr;

            std::vector<HANDLE> inherited;
            for (HANDLE h : {child_stdin, child_stdout, child_stderr})
                if (std::find(inherited.begin(), inherited.end(), h) == inherited.end())
                    inherited.push_back(h);

            SIZE_T attr_size = 0;
            InitializeProcThreadAttributeList(nullptr, 1, 0, &attr_size);
            std::vector<unsigned char> attr_storage(attr_size);
            si.lpAttributeList = reinterpret_cast<PPROC_THREAD_ATTRIBUTE_LIST>(attr_storage.data());
            const bool attr_initialized = !!InitializeProcThreadAttributeList(si.lpAttributeList, 1, 0, &attr_size);
            if (!attr_initialized ||
                !UpdateProcThreadAttribute(si.lpAttributeList, 0, PROC_THREAD_ATTRIBUTE_HANDLE_LIST, inherited.data(),
                                           inherited.size() * sizeof(HANDLE), nullptr, nullptr)) {
                const int error = (int)GetLastError();
                if (attr_initialized)
                    DeleteProcThreadAttributeList(si.lpAttributeList);
                for (HANDLE h : inherited)
                    CloseHandle(h);
                io::set_error(error);
                co_return Process{};
            }

            PROCESS_INFORMATION pi{};
            BOOL ok = CreateProcessW(nullptr, cmd_buf.data(), nullptr, nullptr, TRUE, EXTENDED_STARTUPINFO_PRESENT,
                                     nullptr, nullptr, &si.StartupInfo, &pi);
            DeleteProcThreadAttributeList(si.lpAttributeList);
            for (HANDLE h : {child_stdin, child_stdout, child_stderr})
                if (h != INVALID_HANDLE_VALUE && h != fallback)
                    CloseHandle(h); // 子进程侧已继承, 父侧立即关
            if (fallback != INVALID_HANDLE_VALUE)
                CloseHandle(fallback);

            if (!ok) {
                io::set_error((int)GetLastError());
                co_return Process{};
            }
            CloseHandle(pi.hThread); // 只留进程句柄
            out.proc_ = pi.hProcess;

            // ---- 退出通知: 系统线程池等进程句柄 → Promise 跨线程路由 ----
            auto state = std::make_shared<Promise<int>>();
            out.exit_future_ = state->get_future();
            out.wait_context_ = std::make_shared<wait_context>(wait_context{pi.hProcess, std::move(state)});
            if (!RegisterWaitForSingleObject(
                    &out.wait_registration_, pi.hProcess,
                    [](void* p, BOOLEAN) {
                        auto* c = static_cast<wait_context*>(p);
                        DWORD code = 255;
                        GetExitCodeProcess(c->process, &code);
                        c->promise->set_value((int)code); // 跨线程 → 等待者 loop
                    },
                    out.wait_context_.get(), INFINITE, WT_EXECUTEONLYONCE | WT_EXECUTEINWAITTHREAD)) {
                const int error = (int)GetLastError();
                out.wait_context_.reset();
                TerminateProcess(pi.hProcess, 1);
                WaitForSingleObject(pi.hProcess, INFINITE);
                io::set_error(error);
                co_return Process{};
            }

            co_return std::move(out);
        }

        /// 启动子进程。args[0] 是程序名 (PATH 可解析)。
        /// 失败返回无效 Process, 错误码在 io::last_error()。
        inline Task<Process> spawn(std::vector<std::string> args, options opt = {}) {
            co_return co_await Process::create(std::move(args), opt);
        }

#elif defined(CORO_URING_ENABLED)

        // ==================================================================
        // Linux 实现 — fork/exec + pipe2 + 收割线程
        // ==================================================================
        // 注意: 本仓库在 Windows 上开发, 本节需要 Linux 环境编译验证。
        // 设计: 不改动进程信号处理器 (避免与 signal.hpp 竞争),
        // 退出等待用专用收割线程阻塞 waitpid → Promise 跨线程路由。

        class Process {
          public:
            Process() = default;
            ~Process() = default;

            Process(Process&&) noexcept = default;
            Process& operator=(Process&&) noexcept = default;
            Process(const Process&) = delete;
            Process& operator=(const Process&) = delete;

            bool valid() const { return pid_ > 0; }
            int pid() const { return pid_; }

            pipe::PipeEnd* stdin_pipe() { return stdin_ ? &*stdin_ : nullptr; }
            pipe::PipeEnd* stdout_pipe() { return stdout_ ? &*stdout_ : nullptr; }
            pipe::PipeEnd* stderr_pipe() { return stderr_ ? &*stderr_ : nullptr; }

            Task<int> wait() {
                if (!exit_future_.valid())
                    co_return -1;
                co_return co_await exit_future_;
            }

            void terminate() {
                if (pid_ > 0)
                    ::kill(pid_, SIGTERM);
            }

            void kill() {
                if (pid_ > 0)
                    ::kill(pid_, SIGKILL);
            }

          private:
            friend Task<Process> spawn(std::vector<std::string>, options);

            int pid_ = -1;
            std::optional<pipe::PipeEnd> stdin_, stdout_, stderr_;
            Future<int> exit_future_;
        };

        inline Task<Process> spawn(std::vector<std::string> args, options opt = {}) {
            Process out;

            if (args.empty() || args.front().empty()) {
                io::set_error(EINVAL);
                co_return Process{};
            }

            int in_pipe[2] = {-1, -1}, out_pipe[2] = {-1, -1}, err_pipe[2] = {-1, -1};
            auto mk = [&](bool capture, int* fds) -> bool {
                if (!capture)
                    return true;
                if (pipe2(fds, O_NONBLOCK | O_CLOEXEC) != 0) {
                    io::set_error(errno);
                    return false;
                }
                return true;
            };
            if (!mk(opt.capture_stdin, in_pipe) || !mk(opt.capture_stdout, out_pipe) ||
                !mk(opt.capture_stderr, err_pipe)) {
                for (int f : {in_pipe[0], in_pipe[1], out_pipe[0], out_pipe[1], err_pipe[0], err_pipe[1]})
                    if (f >= 0)
                        ::close(f);
                co_return Process{};
            }

            // exec 状态管道: 成功 exec 时 O_CLOEXEC 自动关闭写端，父进程读到 EOF；
            // 失败时子进程写回 errno，避免把“程序不存在”误报成有效 Process。
            int exec_status[2] = {-1, -1};
            if (pipe2(exec_status, O_CLOEXEC) != 0) {
                io::set_error(errno);
                for (int f : {in_pipe[0], in_pipe[1], out_pipe[0], out_pipe[1], err_pipe[0], err_pipe[1]})
                    if (f >= 0)
                        ::close(f);
                co_return Process{};
            }

            // fork 后只调用 async-signal-safe 函数；argv 在 fork 前构造。
            std::vector<char*> argv;
            argv.reserve(args.size() + 1);
            for (auto& a : args)
                argv.push_back(a.data());
            argv.push_back(nullptr);

            pid_t pid = fork();
            if (pid < 0) {
                io::set_error(errno);
                ::close(exec_status[0]);
                ::close(exec_status[1]);
                for (int f : {in_pipe[0], in_pipe[1], out_pipe[0], out_pipe[1], err_pipe[0], err_pipe[1]})
                    if (f >= 0)
                        ::close(f);
                co_return Process{};
            }
            if (pid == 0) {
                ::close(exec_status[0]);
                // 子进程: 重接 stdio → execvp
                if (opt.capture_stdin && dup2(in_pipe[0], STDIN_FILENO) < 0)
                    goto exec_failed;
                if (opt.capture_stdout && dup2(out_pipe[1], STDOUT_FILENO) < 0)
                    goto exec_failed;
                if (opt.capture_stderr && dup2(err_pipe[1], STDERR_FILENO) < 0)
                    goto exec_failed;
                execvp(argv[0], argv.data());
            exec_failed : {
                const int error = errno;
                (void)::write(exec_status[1], &error, sizeof(error));
                _exit(127);
            }
            }

            ::close(exec_status[1]);

            // 父进程: 关子进程侧, 持父进程侧
            if (opt.capture_stdin) {
                ::close(in_pipe[0]);
                out.stdin_.emplace(in_pipe[1]);
            }
            if (opt.capture_stdout) {
                ::close(out_pipe[1]);
                out.stdout_.emplace(out_pipe[0]);
            }
            if (opt.capture_stderr) {
                ::close(err_pipe[1]);
                out.stderr_.emplace(err_pipe[0]);
            }

            int exec_error = 0;
            ssize_t status_bytes;
            do {
                status_bytes = ::read(exec_status[0], &exec_error, sizeof(exec_error));
            } while (status_bytes < 0 && errno == EINTR);
            ::close(exec_status[0]);
            if (status_bytes > 0) {
                int status = 0;
                (void)waitpid(pid, &status, 0);
                io::set_error(exec_error);
                co_return Process{};
            }
            if (status_bytes < 0) {
                const int error = errno;
                ::kill(pid, SIGKILL);
                int status = 0;
                (void)waitpid(pid, &status, 0);
                io::set_error(error);
                co_return Process{};
            }
            out.pid_ = pid;

            // 收割线程: 阻塞 waitpid → Promise (跨线程路由到等待者 loop)
            auto state = std::make_shared<Promise<int>>();
            out.exit_future_ = state->get_future();
            int reap_pid = pid;
            std::thread([state, reap_pid] {
                int status = 0;
                waitpid(reap_pid, &status, 0);
                int code = WIFEXITED(status) ? WEXITSTATUS(status) : 128 + WTERMSIG(status);
                state->set_value(code);
            }).detach();

            co_return std::move(out);
        }

#endif

        // ==================================================================
        // 便捷函数 — 运行到退出并收集 stdout
        // ==================================================================

        /// 运行命令到退出, 返回 {退出码, stdout 内容} (stderr 直通控制台)
        inline Task<std::pair<int, std::string>> run_capture(std::vector<std::string> args) {
            Process p = co_await spawn(std::move(args), {.capture_stdout = true});
            if (!p.valid())
                co_return std::make_pair(-1, std::string{});
            // 并发: 读输出 + 等退出 (读挂起在管道空/EOF 上, 退出后 EOF 唤醒)
            std::string out;
            std::vector<coro::Task<bool>> jobs;
            struct Reader {
                static coro::Task<bool> drain(pipe::PipeEnd* rd, std::string* into) {
                    if (!rd)
                        co_return true;
                    char buf[4096];
                    while (true) {
                        int n = co_await rd->read(buf, sizeof(buf));
                        if (n <= 0)
                            co_return true; // EOF / 错误
                        into->append(buf, (size_t)n);
                    }
                }
            };
            auto reader = coro::spawn(Reader::drain(p.stdout_pipe(), &out));
            int code = co_await p.wait();
            co_await std::move(reader);
            co_return std::make_pair(code, std::move(out));
        }

    } // namespace process
} // namespace coro
