#pragma once

#include <string>
#include <utility>
#include <vector>

// HTTP 报文构建器(请求/响应均可)。
//
// 安全与规范(参考 boost::beast / cpp-httplib):
//   - 写入时按 RFC 7230 校验:字段名/方法必须是 token,值中禁止 CR/LF/NUL,
//     防止 CRLF 头注入;非法输入抛出 std::invalid_argument
//   - build() 自动补空行;有消息体且未手动设置时自动补 Content-Length
//
// 两种用法:
//   1. 一站式(推荐):http_protocol::request(...) / json(...) / form(...) / response(...)
//   2. 链式细控:request_line(...) -> header(...) -> body(...) -> build()
class http_protocol {
public:
    using header_list = std::vector<std::pair<std::string, std::string>>;

    // ---- 一站式接口 ----

    // 构建完整请求,如 request("GET", "/", {{"Host", "example.com"}})
    static std::string request(const char *method, const char *url,
                               const header_list &headers = {},
                               const std::string &body = "",
                               const char *version = "HTTP/1.1");

    // JSON 请求:自动补 Content-Type: application/json(headers 中已设置则不覆盖)
    static std::string json(const char *method, const char *url,
                            const std::string &json_body,
                            const header_list &headers = {},
                            const char *version = "HTTP/1.1");

    // 表单提交:fields 自动 urlencode 为消息体,
    // 自动补 Content-Type: application/x-www-form-urlencoded
    static std::string form(const char *method, const char *url,
                            const header_list &fields,
                            const header_list &headers = {},
                            const char *version = "HTTP/1.1");

    // 构建完整响应,如 response(200, {{"Content-Type", "text/plain"}}, "ok")
    // reason 传 nullptr 时使用常见默认原因短语
    static std::string response(int status_code,
                                const header_list &headers = {},
                                const std::string &body = "",
                                const char *reason = nullptr,
                                const char *version = "HTTP/1.1");

    // URL 百分号编码(未保留字符原样,其余按 %XX)
    static std::string url_encode(const std::string &text);

    // ---- 链式接口(细控) ----

    // 请求行,如 request_line("GET", "/index.html")
    http_protocol &request_line(const char *method, const char *url,
                                const char *version = "HTTP/1.1");

    // 响应状态行,如 status_line(200);reason 传 nullptr 时使用常见默认原因短语
    http_protocol &status_line(int status_code, const char *reason = nullptr,
                               const char *version = "HTTP/1.1");

    // 添加头部;replace=true 时同名字段先删除再写入(参考 boost::beast 的 set 语义)
    http_protocol &header(const std::string &field, const std::string &value,
                          bool replace = false);

    http_protocol &body(const std::string &body);

    // 生成完整报文:起始行 + 头部 + 空行 + 消息体
    std::string build() const;

    // 清空所有内容,可复用
    void clear();

private:
    std::string start_line_;
    header_list headers_;
    std::string body_;
    bool has_body_ = false;
};
