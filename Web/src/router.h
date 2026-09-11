#pragma once

#include <algorithm>
#include <cctype>
#include <coro/coro.hpp>
#include <coro/fs.hpp>
#include <radix_router.h>

#include <functional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "http_types.h"

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
    using handler_fn = std::function<coro::Task<http_response>(const http_request&)>;

    /// 注册 GET 处理器。路径支持:
    ///   静态:  "/user/list"
    ///   参数:  "/user/:id"      (handler 内用 req.param("id") 取)
    ///   通配:  "/files/*path"   (吞掉剩余全部路径)
    void get(const std::string& path, handler_fn fn) { add("GET", path, std::move(fn)); }

    /// 注册 POST 处理器
    void post(const std::string& path, handler_fn fn) { add("POST", path, std::move(fn)); }

    /// 注册任意 HTTP 方法的处理器
    void add(const std::string& method, const std::string& path, handler_fn fn) {
        // 每方法一棵树: operator[] 首次访问时创建该方法的空 router
        trees_[method].insert(path, std::move(fn));
    }

    /// 静态文件服务: 把 URL 前缀 mount 映射到磁盘目录 dir.
    /// 例: static_dir("/static", "/var/www")
    ///     请求 GET /static/css/style.css → 读取 /var/www/css/style.css 返回
    void static_dir(const std::string& mount, const std::string& dir) {
        static_dirs_.emplace_back(mount, dir); // 存入 vector, 支持多个挂载点
    }

    /// 路由分发: 按优先级依次尝试:
    ///   1. radix_router 匹配 (当前方法的树), 命中时填充 req.params
    ///   2. 405 / OPTIONS: 路径存在但方法不同 (生产路由器内建能力)
    ///   3. 静态文件目录 (仅 GET)
    ///   4. 都不匹配 → 404
    /// 注意: req 为非 const 引用 —— 动态路由捕获的参数要写入 req.params
    coro::Task<http_response> route(http_request& req) const {
        // path_view(): 零分配的路径视图 (直接指向 req.url 内部)
        std::string_view path = req.path_view();

        // ---- 第一步: 在该方法的树里查找 ----
        if (auto it = trees_.find(req.method); it != trees_.end()) {
            radix_router<handler_fn>::params_view caps; // 捕获结果 (string_view)
            if (const handler_fn* fn = it->second.lookup(path, &caps)) {
                // 捕获的视图实体化成 string 存入 req.params
                // (handler 通过 req.param("id") 按名访问)
                req.params.reserve(caps.size());
                for (const auto& [k, v] : caps)
                    req.params.emplace_back(std::string(k), std::string(v));
                co_return co_await (*fn)(req);
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
            if (path.rfind(mount, 0) != 0)
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

  private:
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

    // 每个 HTTP 方法一棵 radix_router 树 (httprouter 同款设计):
    //   GET 树 / POST 树 / ... 互不干扰, 同名路径不同方法各走各的.
    // unordered_map 的键是方法名 (GET/POST/...), 只有几个, 哈希开销可忽略.
    std::unordered_map<std::string, radix_router<handler_fn>> trees_;
    // 静态文件目录: pair<URL 前缀, 磁盘目录>
    // 例: {"/static", "/var/www"} 表示 /static/* 映射到 /var/www/*
    std::vector<std::pair<std::string, std::string>> static_dirs_;
};
