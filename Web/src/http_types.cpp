#include "http_types.h"

#include <algorithm>
#include <cctype>
#include <fstream>
#include <iterator>

#include "http_protocol.h"

// 匿名命名空间: 相当于 C 的 static, 把这些辅助函数限定在本文件内,
// 外部不可见. 避免和其他 .cpp 中的同名函数冲突
namespace
{

    // RFC 7230 规定: HTTP 头部字段名是大小写不敏感的.
    // 比如 "Content-Type" 和 "content-type" 是同一个头部.
    // 这个函数逐字节比较, 但都转成小写后再比.
    // static_cast<unsigned char> 是为了防止负值 char 传给 tolower 导致 UB
    bool iequals(const std::string &a, const std::string &b)
    {
        if (a.size() != b.size())
            return false;
        for (size_t i = 0; i < a.size(); ++i)
        {
            if (std::tolower(static_cast<unsigned char>(a[i])) !=
                std::tolower(static_cast<unsigned char>(b[i])))
                return false;
        }
        return true;
    }

    // 检查 headers 列表中是否已存在某个头部(大小写不敏感).
    // 用于 build() 时判断是否需要自动补 Content-Length,
    // 避免重复添加.
    bool has_header(const http_response::header_list &headers, const std::string &name)
    {
        return std::any_of(headers.begin(), headers.end(),
                           [&](const auto &h)
                           { return iequals(h.first, name); });
    }

    // 按文件扩展名推断 MIME 类型 (Content-Type 的值).
    // 比如 "hello.html" → "text/html; charset=utf-8"
    // 不认识的扩展名统一返回 "application/octet-stream" (二进制流),
    // 浏览器会当作下载处理而不是尝试渲染.

} // namespace

// ---- http_request 成员函数 ----

// 从 url 中提取路径部分, 去掉 query string.
// 例如: "/greet?name=coro" → "/greet"
//       "/index.html"      → "/index.html" (无 query, 原样返回)
std::string http_request::path() const
{
    auto q = url.find('?');
    return q == std::string::npos ? url : url.substr(0, q);
}

// path() 的零分配版本: 直接取 url 上的 string_view, 不构造新字符串。
// 路由热路径用它, 避免 path() 每次调用都 find+substr 堆分配。
// 返回的视图指向 url 内部, 仅在 req 存活期间有效。
std::string_view http_request::path_view() const
{
    auto q = url.find('?');
    return q == std::string::npos ? std::string_view(url)
                                  : std::string_view(url.data(), q);
}

// 提取 query string, 不含 '?' 本身.
// 例如: "/greet?name=coro" → "name=coro"
//       "/index.html"      → "" (无 query, 返回空串)
std::string http_request::query() const
{
    auto q = url.find('?');
    return q == std::string::npos ? std::string() : url.substr(q + 1);
}

// 按字段名取头部值, 大小写不敏感.
// 例如: header("Content-Type") 能匹配到 "content-type: text/html".
// 不存在时返回 static 空串的引用(避免返回临时对象的引用导致 UB).
const std::string &http_request::header(const std::string &name) const
{
    for (const auto &h : headers)
    {
        if (iequals(h.first, name))
            return h.second;
    }
    static const std::string empty; // 函数内 static: 生命周期到程序结束, 引用安全
    return empty;
}

// 按名称取动态路由参数 (router 在分发时捕获并填充到 params)。
// 例如: 路由 /user/:id 匹配 /user/42 后, param("id") 返回 "42"。
// 参数通常只有 0~2 个, 线性扫描足够; 不存在时返回 static 空串引用。
const std::string &http_request::param(const std::string &name) const
{
    for (const auto &[k, v] : params)
    {
        if (k == name)
            return v;
    }
    static const std::string empty; // 同 header(): 静态空串引用安全
    return empty;
}

// ---- http_response 工厂方法 ----
// 工厂方法: 静态方法, 返回一个已经设置好 Content-Type 的响应对象.
// 用法示例: return http_response::text("hello");
//         return http_response::json("{\"code\":0}", 200);

// 纯文本响应, Content-Type 带 charset=utf-8, 浏览器会按 UTF-8 解码
http_response http_response::text(std::string body, int status)
{
    http_response r;
    r.status = status;
    r.header("Content-Type", "text/plain; charset=utf-8");
    r.body = std::move(body); // move 避免拷贝, body 是值传递进来的副本
    return r;
}

// JSON 响应, 浏览器/客户端会根据 Content-Type 自动按 JSON 解析
http_response http_response::json(std::string body, int status)
{
    http_response r;
    r.status = status;
    r.header("Content-Type", "application/json");
    r.body = std::move(body);
    return r;
}

// HTML 响应, 浏览器会渲染为网页
http_response http_response::html(std::string body, int status)
{
    http_response r;
    r.status = status;
    r.header("Content-Type", "text/html; charset=utf-8");
    r.body = std::move(body);
    return r;
}

// 错误响应: 纯文本 + 指定状态码. 尾部加 \n 让命令行 curl 显示更友好
http_response http_response::error(int status, const std::string &message)
{
    return text(message + "\n", status);
}

// 读取本地文件作为响应体. 注意: 这是同步 IO (遗留便捷接口);
// 异步路径用 coro::fs::read_all (router.h 的静态文件服务已切换到它),
// 需要本接口时建议套 coro::to_thread 避免阻塞事件循环.
http_response http_response::file(const std::string &path)
{
    // binary + ate: 二进制模式 (避免 Windows \r\n 转换导致 Content-Length
    // 与实际字节数不一致), 打开即定位到文件尾 (取大小)
    std::ifstream f(path, std::ios::binary | std::ios::ate);
    if (!f)
        return http_response::error(404, "file not found: " + path);
    // 定长 + 单次 read: 比 istreambuf_iterator 逐字节快约一个数量级
    std::string body;
    auto size = f.tellg();
    if (size > 0)
    {
        body.resize((size_t)size);
        f.seekg(0);
        f.read(body.data(), (std::streamsize)body.size());
        body.resize((size_t)f.gcount()); // 实际读取数 (防御并发截断)
    }
    http_response r;
    r.header("Content-Type", mime_type(path)); // 按扩展名推断 MIME
    r.body = std::move(body);
    return r;
}

// 添加响应头. replace=true 时会先删除所有同名头部(大小写不敏感),
// 再添加新的. 比如设置 Content-Type 时不希望出现两个重复的.
// replace=false(默认) 则直接追加, 允许同名头部共存(如 Set-Cookie).
void http_response::header(const std::string &name, const std::string &value, bool replace)
{
    if (replace)
    {
        // erase-remove 惯用法: remove_if 把要删除的元素移到末尾并返回新尾,
        // erase 再真正删除. iequals 实现大小写不敏感匹配.
        headers.erase(std::remove_if(headers.begin(), headers.end(),
                                     [&](const auto &h)
                                     { return iequals(h.first, name); }),
                      headers.end());
    }
    headers.emplace_back(name, value); // 在尾部插入新头部
}

// 把响应序列化为完整的 HTTP 报文字符串, 可以直接 write 到 socket.
// 输出格式示例:
//   HTTP/1.1 200 OK\r\n
//   Content-Type: text/plain\r\n
//   Content-Length: 13\r\n
//   \r\n
//   Hello, World!
//
// 关键: 强制添加 Content-Length.
//   keep-alive 模式下, 客户端需要靠 Content-Length 来判断响应体何时结束.
//   如果没有 Content-Length, 客户端不知道什么时候响应接收完毕,
//   会一直等待直到超时. 即使 body 为空也要发 Content-Length: 0.
std::string http_response::build() const
{
    // http_protocol 是底层序列化器(在 thirdparty/http 中),
    // 提供链式 API: status_line() → header() → body() → build()
    http_protocol p;
    p.status_line(status); // 如 "HTTP/1.1 200 OK\r\n"
    for (const auto &h : headers)
    {
        p.header(h.first, h.second); // 每个头部: "Name: Value\r\n"
    }
    // 如果用户没手动设置 Content-Length, 自动补上
    if (!has_header(headers, "Content-Length"))
    {
        p.header("Content-Length", std::to_string(body.size()));
    }
    p.body(body); // 先输出 "\r\n"(头部与主体的分隔线), 再输出 body
    return p.build(); // 拼接成完整报文
}
std::string mime_type(const std::string &path)
{
    auto dot = path.find_last_of('.');
    std::string ext = dot == std::string::npos ? "" : path.substr(dot + 1);
    if (ext == "html" || ext == "htm")
        return "text/html; charset=utf-8";
    if (ext == "css")
        return "text/css";
    if (ext == "js")
        return "application/javascript";
    if (ext == "json")
        return "application/json";
    if (ext == "png")
        return "image/png";
    if (ext == "jpg" || ext == "jpeg")
        return "image/jpeg";
    if (ext == "gif")
        return "image/gif";
    if (ext == "svg")
        return "image/svg+xml";
    if (ext == "txt")
        return "text/plain; charset=utf-8";
    if (ext == "ico")
        return "image/x-icon";
    return "application/octet-stream"; // 默认: 二进制流
}
