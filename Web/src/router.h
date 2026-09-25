#pragma once

#include <algorithm>
#include <cctype>
#include <coro/coro.hpp>
#include <coro/fs.hpp>
#include <radix_router.h>

#include <filesystem>
#include <functional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "http_types.h"
#include "middleware.h"

// ============================================================================
// router.h — HTTP 路由层: 基于 radix_router 的高性能动态路由
// ============================================================================
//
// 架构 (参考 httprouter / find-my-way):
//   - 每个 HTTP 方法一棵独立的 radix_router 树 (GET/POST 互不干扰)
//   - 静态路由走快表整段比较; 动态路由 (:param / *wild) 走字典树 + DFS 回溯
//   - 匹配全程 string_view 零分配; 捕获的参数实体化后写入 req.params
//   - 动态路由未命中 → 静态文件目录 (static_dir) 兜底 → 404
//
// handler 签名是异步协程 Task<http_response>: 处理过程中可以 co_await
// (sleep / 网络请求 / 等待其他任务)。这正是协程服务器的优势——
// 单个事件循环线程内, 慢请求挂起时不占用 CPU, 不会阻塞其他连接。
// ============================================================================

class router {
  public:
    // handler 的函数签名: 接收 const http_request&, 返回协程 Task<http_response>.
    // std::function 让它可以用 lambda / 函数指针 / 任意可调用对象.
    // Task<http_response> 意味着 handler 内部可以 co_await 异步操作
    // (比如 sleep / 数据库查询), 挂起时不占 CPU.
    // 中间件可以修改请求 (参数是可写引用), handler 保持只读 const&.
    using handler_fn = std::function<coro::Task<http_response>(const http_request&)>;

    // 中间件类型别名 (定义见 middleware.h, 减少调用方改动)
    using terminal_fn = middleware::terminal_fn;
    using middleware_fn = middleware::middleware_fn;
    using next_fn = middleware::next_fn;

    // 路由条目: handler + 路由级中间件 (radix_router<Value> 的 Value)。
    // 必须定义在使用它的成员函数签名之前 —— 签名中的类型在声明点解析,
    // 不属于「完整类上下文」。
    struct route_entry {
        handler_fn handler;
        std::vector<middleware_fn> middlewares; // 内层链, 可为空
    };

    /// 注册 GET 处理器。路径支持:
    ///   静态:  "/user/list"
    ///   参数:  "/user/:id"      (handler 内用 req.param("id") 取)
    ///   通配:  "/files/*path"   (吞掉剩余全部路径)
    /// mws: 路由级中间件 (匹配命中后才执行, 可读 req.param; 先挂载的在外层)
    void get(const std::string& path, handler_fn fn, std::vector<middleware_fn> mws = {}) {
        add("GET", path, std::move(fn), std::move(mws));
    }

    /// 注册 POST 处理器
    void post(const std::string& path, handler_fn fn, std::vector<middleware_fn> mws = {}) {
        add("POST", path, std::move(fn), std::move(mws));
    }

    /// 注册任意 HTTP 方法的处理器
    void add(const std::string& method, const std::string& path, handler_fn fn, std::vector<middleware_fn> mws = {}) {
        check_frozen();
        // 每方法一棵树: operator[] 首次访问时创建该方法的空 router
        route_entry entry{std::move(fn), std::move(mws)};
        trees_[method].insert(path, std::move(entry));
    }

    /// 全局中间件: 包在所有路由 (含 404/405/静态文件) 外层, 按注册顺序执行,
    /// 先注册的在最外层 (aiohttp/gin 同语义)。此时路由参数尚未填充 ——
    /// 需要读取 req.param 的鉴权类中间件应挂在路由级。
    void use(middleware_fn mw) {
        check_frozen();
        middlewares_.push_back(std::move(mw));
    }

    /// 冻结路由表: 之后所有注册入口 (use/get/post/add/static_dir) 抛
    /// std::logic_error —— 不用 assert, Release 下同样生效。
    /// 顺序约定: 注册 → freeze() → 设置运行标志/开始 accept;
    /// 注册与冻结只在启动线程顺序进行, 不承诺并发注册安全。
    void freeze() { frozen_ = true; }

    /// 静态文件服务: 把 URL 前缀 mount 映射到磁盘目录 dir.
    /// 例: static_dir("/static", "/var/www")
    ///     请求 GET /static/css/style.css → 读取 /var/www/css/style.css 返回
    void static_dir(const std::string& mount, const std::string& dir) {
        check_frozen();
        std::string normalized = mount.empty() ? "/" : mount;
        while (normalized.size() > 1 && normalized.back() == '/')
            normalized.pop_back();
        if (normalized.front() != '/')
            normalized.insert(normalized.begin(), '/');
        static_dirs_.emplace_back(std::move(normalized), dir); // 存入 vector, 支持多个挂载点
    }

    /// 路由分发入口。无全局中间件时走快路径: 直接进入 route_core,
    /// 不创建链对象与协程帧 (默认零中间件的请求零额外开销)。
    coro::Task<http_response> route(http_request& req) const {
        if (middlewares_.empty())
            return route_core(req);
        return route_with_global(req);
    }

  private:
    /// 全局链消费协程: 链在协程内创建为局部对象并 co_await 到结束
    /// (Task 延迟执行, 临时链会悬空 —— 见 middleware.h 生命周期约定)。
    coro::Task<http_response> route_with_global(http_request& req) const {
        middleware::middleware_chain chain(middlewares_, [this](http_request& r) { return route_core(r); });
        co_return co_await chain.run(0, req);
    }

    /// 路由级链消费协程: 同上, 链尾终端是该路由的 handler
    coro::Task<http_response> run_route_middlewares(const route_entry& entry, http_request& req) const {
        middleware::middleware_chain chain(entry.middlewares, [&entry](http_request& r) { return entry.handler(r); });
        co_return co_await chain.run(0, req);
    }

    /// 路由匹配核心 (原 route 逻辑): 按优先级依次尝试:
    ///   1. radix_router 匹配 (当前方法的树), 命中时填充 req.params
    ///   2. 405 / OPTIONS: 路径存在但方法不同 (生产路由器内建能力)
    ///   3. 静态文件目录 (仅 GET)
    ///   4. 都不匹配 → 404
    /// 注意: req 为非 const 引用 —— 动态路由捕获的参数要写入 req.params
    coro::Task<http_response> route_core(http_request& req) const {
        // path_view(): 零分配的路径视图 (直接指向 req.url 内部)
        std::string_view path = req.path_view();

        // ---- 第一步: 在该方法的树里查找 ----
        if (auto it = trees_.find(req.method); it != trees_.end()) {
            radix_router<route_entry>::params_view caps; // 捕获结果 (string_view)
            if (const route_entry* entry = it->second.lookup(path, &caps)) {
                // 捕获的视图实体化成 string 存入 req.params
                // (handler 通过 req.param("id") 按名访问)
                req.params.reserve(caps.size());
                for (const auto& [k, v] : caps)
                    req.params.emplace_back(std::string(k), std::string(v));
                if (entry->middlewares.empty())
                    co_return co_await entry->handler(req);
                co_return co_await run_route_middlewares(*entry, req);
            }
        }

        // ---- 第二步: 405 / OPTIONS —— 路径存在但方法不对 ----
        // 生产路由器内建能力 (httprouter/gin 同款): 扫描其他方法的树,
        // 命中则返回 405 + Allow 头 (而非误导性的 404);
        // OPTIONS 请求直接回 200 + Allow (RFC 7231)。
        // 只在未命中路径上执行, 不影响热路径性能。
        std::string allow;
        for (const auto& [m, tree] : trees_) {
            if (m != req.method && tree.lookup(path))
                allow += (allow.empty() ? "" : ", ") + m;
        }
        if (!allow.empty()) {
            if (req.method == "OPTIONS") {
                // Allow 必须包含 OPTIONS 本身 (RFC 7231)
                http_response resp = http_response::text("");
                resp.header("Allow", allow + ", OPTIONS");
                co_return resp;
            }
            http_response resp = http_response::error(405, "method not allowed");
            resp.header("Allow", allow);
            co_return resp;
        }

        // ---- 第三步: 尝试静态文件目录 (仅 GET) ----
        for (const auto& [mount, dir] : static_dirs_) {
            // rfind(mount, 0): 检查 path 是否以 mount 开头 (类似 Python 的 startswith)
            // 返回 0 表示从位置 0 开始匹配成功
            // 必须按完整路径段匹配; 否则 mount="/static" 会错误匹配
            // "/static-secret" 并把它映射到静态目录根。
            const bool mount_match =
                path == mount || (path.size() > mount.size() && path.rfind(mount, 0) == 0 && path[mount.size()] == '/');
            if (!mount_match)
                continue; // 不匹配这个挂载点, 试下一个

            // 静态文件只支持 GET; 其他方法返回 405/OPTIONS 应答 (而非默默读文件)
            if (req.method != "GET") {
                http_response resp = (req.method == "OPTIONS") ? http_response::text("")
                                                               : http_response::error(405, "method not allowed");
                resp.header("Allow", "GET, OPTIONS");
                co_return resp;
            }

            // 提取相对路径: 去掉 mount 前缀
            // 例: mount="/static", path="/static/a.txt" → rel="/a.txt"
            std::string rel(path.substr(mount.size()));
            if (rel.empty())
                rel = "/"; // 请求的是挂载点根目录, 当作 "/" 处理

            // ---- 安全检查: 防止路径穿越攻击 ----
            // 攻击者可能请求 /static/../../etc/passwd 来读取系统文件.
            // rel 以 / 开头, 先去掉再检查 (因为 is_safe_path 期望相对路径)
            std::string rel_trim = (!rel.empty() && rel[0] == '/') ? rel.substr(1) : rel;
            if (!rel_trim.empty() && !is_safe_path(rel_trim))
                co_return http_response::error(403, "forbidden"); // 危险路径, 拒绝

            // ---- 读取文件并返回 ----
            // coro::fs: 真正的异步文件 IO (IOCP / io_uring 完成包驱动)。
            // 旧实现是 to_thread(同步 ifstream) —— 线程池兜底, 占用槽位
            // 且有跨线程开销; 现在直接挂在 worker 的事件循环上, 读大文件
            // 期间 worker 可以继续处理其他连接。
            std::string full_path = dir + "/" + rel_trim;
            // 规范化后再次确认 real path 仍位于挂载根目录内, 防止 symlink
            // 把一个看似安全的相对路径带出静态目录。
            try {
                const auto base = std::filesystem::weakly_canonical(std::filesystem::path(dir));
                const auto candidate = std::filesystem::weakly_canonical(std::filesystem::path(full_path));
                auto rel_to_base = std::filesystem::relative(candidate, base);
                if (rel_to_base.empty() || rel_to_base == ".") {
                    // 目录根本身不作为文件返回, 避免把目录读成 404 以外的异常。
                    co_return http_response::error(404, "not found");
                }
                auto rel_text = rel_to_base.generic_string();
                if (rel_text == ".." || rel_text.rfind("../", 0) == 0)
                    co_return http_response::error(403, "forbidden");
                full_path = candidate.string();
            } catch (const std::filesystem::filesystem_error&) {
                co_return http_response::error(404, "file not found: " + rel_trim);
            }
            coro::io::clear_error();
            std::string body = co_await coro::fs::read_all(full_path);
            if (coro::io::last_error() != 0)
                co_return http_response::error(404, "file not found: " + rel_trim);

            http_response resp;
            resp.header("Content-Type", mime_type(full_path));
            resp.body = std::move(body);
            co_return resp;
        }

        // ---- 第四步: 都没匹配, 返回 404 ----
        co_return http_response::error(404, "not found");
    }

    /// 路径安全校验: 防止攻击者通过精心构造的 URL 读取服务器上的敏感文件.
    /// 攻击示例:
    ///   - /static/../../etc/passwd       (.. 穿越)
    ///   - /static/..\..\windows\system32 (反斜杠绕过, Windows)
    ///   - /static/C:\windows\system32    (绝对路径逃逸)
    ///   - /static/%2e%2e/etc/passwd      (URL 编码绕过, %2e = '.')
    ///
    /// 防御策略:
    ///   1. 反斜杠转正斜杠 (统一格式, 防 Windows 绕过)
    ///   2. 拒绝绝对路径 (以 / 开头 或 盘符 C:)
    ///   3. 拒绝 URL 编码的点 (%2e / %2E)
    ///   4. 按 / 分割, 检查每个路径段是否为 ".."
    static bool is_safe_path(const std::string& rel) {
        std::string normalized = rel;
        if (normalized.find('\0') != std::string::npos)
            return false;
        // 步骤 1: 反斜杠统一转正斜杠
        std::replace(normalized.begin(), normalized.end(), '\\', '/');

        // 步骤 2: 拒绝绝对路径
        // 2a. 以 / 开头 (Unix 绝对路径)
        if (!normalized.empty() && normalized[0] == '/')
            return false;
        // 2b. 盘符 (Windows 绝对路径, 如 C:/windows)
        if (normalized.size() >= 2 && std::isalpha(static_cast<unsigned char>(normalized[0])) && normalized[1] == ':')
            return false;

        // 步骤 3: 拒绝 URL 编码的点 (%2e / %2E)
        // 攻击者可能用 %2e%2e 代替 .. 来绕过检查
        for (size_t i = 0; i + 2 < normalized.size(); ++i) {
            if (normalized[i] == '%' && normalized[i + 1] == '2' &&
                (normalized[i + 2] == 'e' || normalized[i + 2] == 'E'))
                return false;
        }

        // 步骤 4: 按 / 分割, 逐段检查是否为 ".."
        // 例: "a/b/../c" → 分割成 "a", "b", "..", "c"
        //      发现 ".." 立即返回 false
        std::string segment;
        for (size_t i = 0; i <= normalized.size(); ++i) {
            if (i == normalized.size() || normalized[i] == '/') {
                // 到达末尾或遇到分隔符, 检查当前段
                if (segment == "..")
                    return false;
                segment.clear(); // 重置, 准备下一段
            } else {
                segment += normalized[i]; // 累积当前段的字符
            }
        }
        return true; // 所有检查通过, 路径安全
    }

    void check_frozen() const {
        if (frozen_)
            throw std::logic_error("router is frozen: register before serve()");
    }

    // 每个 HTTP 方法一棵 radix_router 树 (httprouter 同款设计):
    //   GET 树 / POST 树 / ... 互不干扰, 同名路径不同方法各走各的.
    // unordered_map 的键是方法名 (GET/POST/...), 只有几个, 哈希开销可忽略.
    std::unordered_map<std::string, radix_router<route_entry>> trees_;
    // 静态文件目录: pair<URL 前缀, 磁盘目录>
    // 例: {"/static", "/var/www"} 表示 /static/* 映射到 /var/www/*
    std::vector<std::pair<std::string, std::string>> static_dirs_;
    // 全局中间件 (外层链): 包住 route_core 的全部输出 (含 404/静态)
    std::vector<middleware_fn> middlewares_;
    // 冻结标志: freeze() 后注册入口抛错; 之后多 worker 只读并发安全
    bool frozen_ = false;
};
