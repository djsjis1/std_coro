#pragma once

#include <functional>
#include <map>
#include <string>

#include "llhttp.h"

// llhttp 的 C++ 封装:流式喂入数据,自动累积解析结果(method/url/headers/body)。
// 支持一条连接上连续解析多条消息(keep-alive)。
class http_parse {
public:
    using void_callback = std::function<void()>;
    using data_callback = std::function<void(const char *, size_t)>;

    // type: HTTP_REQUEST / HTTP_RESPONSE / HTTP_BOTH
    explicit http_parse(llhttp_type_t type = HTTP_REQUEST);

    // parser->data 指向本对象,禁止拷贝
    http_parse(const http_parse &) = delete;
    http_parse &operator=(const http_parse &) = delete;

    // 增量喂入数据,可多次调用。出错返回 false,错误信息见 error()
    bool feed(const char *data, size_t len);

    // 解析一条完整消息:先重置解析器,再 feed(一次性传入整条消息)
    bool feed_all(const char *data, size_t len);

    // 重置解析器状态(不清空已累积的结果)
    void reset();

    // 最近一次解析错误的描述
    const std::string &error() const { return error_; }

    // 按字段名取头部,大小写不敏感(参考 RFC 7230);不存在返回空串
    const std::string &header(const std::string &field) const;

    // 当前连接是否应复用(消息解析完成后可调用)
    bool keep_alive() const;

    // ---- 解析结果(每条新消息开始时自动清空) ----
    std::string http_method;                          // 请求方法,仅解析请求时有效
    std::string http_url;                             // 请求 URL,仅解析请求时有效
    std::string http_version;                         // 协议版本,如 "1.1"
    int status_code = 0;                              // 状态码,仅解析响应时有效
    std::string http_body;                            // 消息体
    std::map<std::string, std::string> http_headers;  // 头部,字段名按接收原样保存

    // 消息体上限(字节),超出则中止解析;0 表示不限制(参考 cpp-httplib 的做法)
    size_t body_limit = 8 * 1024 * 1024;

    // 请求目标和头部上限。HTTP 头部在 message_complete 之前持续累积,
    // 因此必须在回调阶段拒绝, 不能等整条请求解析完再检查。
    size_t url_limit = 8 * 1024;
    size_t header_bytes_limit = 64 * 1024;
    size_t header_count_limit = 100;

    // ---- 可选的用户回调,按需赋值 ----
    void_callback message_begin;                       // 消息开始(结果已清空)
    void_callback message_complete;                    // 消息完整结束
    data_callback url;                                 // URL 数据块
    data_callback body;                                // 消息体数据块
    data_callback header_field;                        // 头部字段数据块
    data_callback header_value;                        // 头部值数据块

private:
    void setup_callbacks();
    void clear_result();

    llhttp_t parser_;
    llhttp_settings_t settings_;
    llhttp_type_t type_;
    std::string current_field_;   // 正在接收的头部字段名
    std::string current_value_;   // 正在接收的头部值
    size_t header_bytes_ = 0;     // 当前消息已接收的字段名和值字节数
    std::string error_;
    static const std::string empty_string_;
    size_t header_count_ = 0; // 当前消息已完成的头部字段数 (包含重复字段)
};
