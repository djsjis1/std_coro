#include "http_protocol.h"

#include <algorithm>
#include <cctype>
#include <stdexcept>

namespace {

// ---- 校验(参考 boost::beast / cpp-httplib,防止 CRLF 头注入) ----

// RFC 7230 section 3.2.6: token 字符集(tchar)
inline bool is_tchar(char c)
{
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
           c == '!' || c == '#' || c == '$' || c == '%' || c == '&' || c == '\'' ||
           c == '*' || c == '+' || c == '-' || c == '.' || c == '^' || c == '_' ||
           c == '`' || c == '|' || c == '~';
}

inline bool is_token(const std::string &s)
{
    if (s.empty())
        return false;
    for (char c : s)
    {
        if (!is_tchar(c))
            return false;
    }
    return true;
}

// 文本中不得出现 CR/LF/NUL(起始行与头部的通用防线)
inline bool is_safe_text(const std::string &s)
{
    for (char c : s)
    {
        if (c == '\r' || c == '\n' || c == '\0')
            return false;
    }
    return true;
}

// 请求目标不得出现空白与 CR/LF
inline bool is_safe_target(const std::string &s)
{
    if (s.empty())
        return false;
    for (char c : s)
    {
        if (c == ' ' || c == '\t' || c == '\r' || c == '\n' || c == '\0')
            return false;
    }
    return true;
}

// 版本号必须形如 HTTP/x.y
inline bool is_version(const std::string &v)
{
    if (v.rfind("HTTP/", 0) != 0)
        return false;
    size_t i = 5;
    if (i >= v.size() || !std::isdigit(static_cast<unsigned char>(v[i])))
        return false;
    while (i < v.size() && std::isdigit(static_cast<unsigned char>(v[i])))
        ++i;
    if (i >= v.size() || v[i] != '.')
        return false;
    ++i;
    if (i >= v.size() || !std::isdigit(static_cast<unsigned char>(v[i])))
        return false;
    while (i < v.size() && std::isdigit(static_cast<unsigned char>(v[i])))
        ++i;
    return i == v.size();
}

void require(bool ok, const char *what, const std::string &value)
{
    if (!ok)
    {
        throw std::invalid_argument(std::string("http_protocol: invalid ") + what + ": " + value);
    }
}

// ---- 工具 ----

bool iequals(const std::string &a, const char *b)
{
    size_t n = 0;
    while (b[n])
        ++n;
    if (a.size() != n)
        return false;
    for (size_t i = 0; i < n; ++i)
    {
        if (std::tolower(static_cast<unsigned char>(a[i])) !=
            std::tolower(static_cast<unsigned char>(b[i])))
            return false;
    }
    return true;
}

bool has_header(const http_protocol::header_list &headers, const char *field)
{
    return std::any_of(headers.begin(), headers.end(),
                       [field](const std::pair<std::string, std::string> &h) {
                           return iequals(h.first, field);
                       });
}

// 常见状态码的默认原因短语
const char *default_reason(int status_code)
{
    switch (status_code)
    {
    case 200: return "OK";
    case 201: return "Created";
    case 204: return "No Content";
    case 301: return "Moved Permanently";
    case 302: return "Found";
    case 304: return "Not Modified";
    case 400: return "Bad Request";
    case 401: return "Unauthorized";
    case 403: return "Forbidden";
    case 404: return "Not Found";
    case 405: return "Method Not Allowed";
    case 408: return "Request Timeout";
    case 409: return "Conflict";
    case 413: return "Payload Too Large";
    case 414: return "URI Too Long";
    case 415: return "Unsupported Media Type";
    case 429: return "Too Many Requests";
    case 500: return "Internal Server Error";
    case 501: return "Not Implemented";
    case 502: return "Bad Gateway";
    case 503: return "Service Unavailable";
    case 504: return "Gateway Timeout";
    default:  return "";
    }
}

} // namespace

// ---- 一站式接口 ----

std::string http_protocol::request(const char *method, const char *url,
                                   const header_list &headers,
                                   const std::string &body,
                                   const char *version)
{
    http_protocol p;
    p.request_line(method, url, version);
    for (const auto &h : headers)
    {
        p.header(h.first, h.second);
    }
    if (!body.empty())
    {
        p.body(body);
    }
    return p.build();
}

std::string http_protocol::json(const char *method, const char *url,
                                const std::string &json_body,
                                const header_list &headers,
                                const char *version)
{
    header_list all = headers;
    if (!has_header(all, "Content-Type"))
    {
        all.emplace_back("Content-Type", "application/json");
    }
    return request(method, url, all, json_body, version);
}

std::string http_protocol::form(const char *method, const char *url,
                                const header_list &fields,
                                const header_list &headers,
                                const char *version)
{
    std::string body;
    for (size_t i = 0; i < fields.size(); ++i)
    {
        if (i)
            body += '&';
        body += url_encode(fields[i].first);
        body += '=';
        body += url_encode(fields[i].second);
    }

    header_list all = headers;
    if (!has_header(all, "Content-Type"))
    {
        all.emplace_back("Content-Type", "application/x-www-form-urlencoded");
    }
    return request(method, url, all, body, version);
}

std::string http_protocol::response(int status_code,
                                    const header_list &headers,
                                    const std::string &body,
                                    const char *reason,
                                    const char *version)
{
    http_protocol p;
    p.status_line(status_code, reason, version);
    for (const auto &h : headers)
    {
        p.header(h.first, h.second);
    }
    if (!body.empty())
    {
        p.body(body);
    }
    return p.build();
}

std::string http_protocol::url_encode(const std::string &text)
{
    static const char hex[] = "0123456789ABCDEF";
    std::string out;
    out.reserve(text.size() * 3);
    for (unsigned char c : text)
    {
        // RFC 3986 未保留字符原样输出
        if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~')
        {
            out += static_cast<char>(c);
        }
        else
        {
            out += '%';
            out += hex[c >> 4];
            out += hex[c & 0x0F];
        }
    }
    return out;
}

// ---- 链式接口 ----

http_protocol &http_protocol::request_line(const char *method, const char *url,
                                           const char *version)
{
    require(is_token(method), "method", method);
    require(is_safe_target(url), "url", url);
    require(is_version(version), "version", version);

    clear();
    start_line_ = std::string(method) + " " + url + " " + version + "\r\n";
    return *this;
}

http_protocol &http_protocol::status_line(int status_code, const char *reason,
                                          const char *version)
{
    require(status_code >= 100 && status_code <= 999, "status code",
            std::to_string(status_code));
    require(is_version(version), "version", version);

    clear();
    if (!reason)
    {
        reason = default_reason(status_code);
    }
    require(is_safe_text(reason), "reason", reason);

    start_line_ = std::string(version) + " " + std::to_string(status_code);
    if (*reason)
    {
        start_line_ += " ";
        start_line_ += reason;
    }
    start_line_ += "\r\n";
    return *this;
}

http_protocol &http_protocol::header(const std::string &field, const std::string &value,
                                     bool replace)
{
    require(is_token(field), "header field", field);
    require(is_safe_text(value), "header value", value);

    if (replace)
    {
        headers_.erase(
            std::remove_if(headers_.begin(), headers_.end(),
                           [&field](const std::pair<std::string, std::string> &h) {
                               return iequals(h.first, field.c_str());
                           }),
            headers_.end());
    }
    headers_.emplace_back(field, value);
    return *this;
}

http_protocol &http_protocol::body(const std::string &body)
{
    body_ = body;
    has_body_ = true;
    return *this;
}

std::string http_protocol::build() const
{
    std::string text;
    text.reserve(start_line_.size() + body_.size() + headers_.size() * 40 + 4);
    text += start_line_;
    for (const auto &h : headers_)
    {
        text += h.first;
        text += ": ";
        text += h.second;
        text += "\r\n";
    }
    if (has_body_ && !has_header(headers_, "Content-Length"))
    {
        text += "Content-Length: ";
        text += std::to_string(body_.size());
        text += "\r\n";
    }
    text += "\r\n"; // 头部与消息体之间的空行
    text += body_;
    return text;
}

void http_protocol::clear()
{
    start_line_.clear();
    headers_.clear();
    body_.clear();
    has_body_ = false;
}
