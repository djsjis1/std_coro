#pragma once

#include <map>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

// ============================================================================
// http_types.h — HTTP 请求/响应数据类型
// ============================================================================
//
// 位于「解析(http_parse/llhttp)」与「分发(router)」之间的数据层:
//   - http_request:  一条已解析完成的请求(llhttp 解析结果的快照)
//   - http_response: 服务器返回的响应(build() 序列化用 http_protocol)
// ============================================================================

/// 一条已解析完成的 HTTP 请求
struct http_request
{
    std::string method;                         // 请求方法, 如 GET / POST
    std::string url;                            // 原始请求目标, 如 /greet?name=coro
    std::string version;                        // 协议版本, 如 "1.1"
    std::string body;                           // 消息体(受 body_limit 限制)
    std::map<std::string, std::string> headers; // 头部(查询大小写不敏感)
    bool keep_alive = false;                    // 此请求之后连接是否复用

    /// 路由捕获的动态参数 (如 /user/:id → {"id", "42"}), 由 router 在分发时填充
    std::vector<std::pair<std::string, std::string>> params;

    /// 路径部分(去掉 query), 如 /greet?name=coro → /greet
    std::string path() const;

    /// path() 的零分配版本: 直接取 url 上的视图 (req 存活期间有效)。
    /// 热路径首选 —— 避免 path() 每次调用都 find+substr 分配字符串。
    std::string_view path_view() const;

    /// query 字符串(不含 '?'), 无 query 返回空串
    std::string query() const;

    /// 按字段名取头部值, 不存在返回空串(大小写不敏感)
    const std::string &header(const std::string &name) const;

    /// 按名称取动态路由参数, 不存在返回空串。例: req.param("id")
    const std::string &param(const std::string &name) const;
};

/// 服务器响应(由 handler 构造, web_server 序列化后写出)
struct http_response
{
    using header_list = std::vector<std::pair<std::string, std::string>>;

    int status = 200;    // 状态码
    header_list headers; // 响应头
    std::string body;    // 响应体

    // ---- 便捷工厂 ----

    static http_response text(std::string body, int status = 200);
    static http_response json(std::string body, int status = 200);
    static http_response html(std::string body, int status = 200);
    static http_response error(int status, const std::string &message);

    /// 读取本地文件作为响应体(同步 IO; Content-Type 按扩展名推断)
    static http_response file(const std::string &path);

    /// 添加响应头; replace=true 时替换同名字段
    void header(const std::string &name, const std::string &value, bool replace = false);

    /// 序列化为完整 HTTP 报文(强制 Content-Length, keep-alive 需要响应边界)
    std::string build() const;
};

/// 按文件扩展名推断 MIME 类型 (如 "a.html" → "text/html; charset=utf-8")
std::string mime_type(const std::string &path);
