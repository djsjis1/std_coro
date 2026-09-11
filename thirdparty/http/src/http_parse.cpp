#include "http_parse.h"

#include <cctype>

namespace {

// llhttp_t::data 中取回封装对象
inline http_parse *self(llhttp_t *parser)
{
    return static_cast<http_parse *>(parser->data);
}

// 头部字段名按 RFC 7230 大小写不敏感
bool field_equals(const std::string &a, const std::string &b)
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

} // namespace

const std::string http_parse::empty_string_;

const std::string &http_parse::header(const std::string &field) const
{
    for (const auto &h : http_headers)
    {
        if (field_equals(h.first, field))
            return h.second;
    }
    return empty_string_;
}

bool http_parse::keep_alive() const
{
    return llhttp_should_keep_alive(&parser_) != 0;
}

http_parse::http_parse(llhttp_type_t type) : type_(type)
{
    setup_callbacks();
    reset();
}

void http_parse::setup_callbacks()
{
    llhttp_settings_init(&settings_);

    settings_.on_message_begin = [](llhttp_t *parser) -> int {
        http_parse *p = self(parser);
        p->clear_result();
        if (p->message_begin)
        {
            p->message_begin();
        }
        return 0;
    };

    settings_.on_url = [](llhttp_t *parser, const char *at, size_t length) -> int {
        http_parse *p = self(parser);
        p->http_url.append(at, length);
        if (p->url)
        {
            p->url(at, length);
        }
        return 0;
    };

    // 头部字段/值可能分多个数据块到达,只做追加;
    // 在 *_complete 回调中才提交到 http_headers
    settings_.on_header_field = [](llhttp_t *parser, const char *at, size_t length) -> int {
        http_parse *p = self(parser);
        p->current_field_.append(at, length);
        if (p->header_field)
        {
            p->header_field(at, length);
        }
        return 0;
    };

    settings_.on_header_value = [](llhttp_t *parser, const char *at, size_t length) -> int {
        http_parse *p = self(parser);
        p->current_value_.append(at, length);
        if (p->header_value)
        {
            p->header_value(at, length);
        }
        return 0;
    };

    settings_.on_header_value_complete = [](llhttp_t *parser) -> int {
        http_parse *p = self(parser);
        p->http_headers[p->current_field_] = p->current_value_;
        p->current_field_.clear();
        p->current_value_.clear();
        return 0;
    };

    settings_.on_body = [](llhttp_t *parser, const char *at, size_t length) -> int {
        http_parse *p = self(parser);
        // 防止超大消息体打爆内存,超限则中止解析
        if (p->body_limit && p->http_body.size() + length > p->body_limit)
        {
            llhttp_set_error_reason(parser, "body limit exceeded");
            return -1;
        }
        p->http_body.append(at, length);
        if (p->body)
        {
            p->body(at, length);
        }
        return 0;
    };

    settings_.on_message_complete = [](llhttp_t *parser) -> int {
        http_parse *p = self(parser);
        p->http_version = std::to_string(llhttp_get_http_major(parser)) + "." +
                          std::to_string(llhttp_get_http_minor(parser));
        if (llhttp_get_type(parser) == HTTP_REQUEST)
        {
            // 请求行此时已解析完毕,method 可用
            p->http_method = llhttp_method_name(static_cast<llhttp_method_t>(parser->method));
        }
        else if (llhttp_get_type(parser) == HTTP_RESPONSE)
        {
            p->status_code = llhttp_get_status_code(parser);
        }
        if (p->message_complete)
        {
            p->message_complete();
        }
        return 0;
    };
}

void http_parse::reset()
{
    error_.clear();
    current_field_.clear();
    current_value_.clear();
    llhttp_init(&parser_, type_, &settings_);
    parser_.data = this;
}

bool http_parse::feed(const char *data, size_t len)
{
    error_.clear();
    llhttp_errno_t err = llhttp_execute(&parser_, data, len);
    if (err == HPE_OK)
    {
        return true;
    }

    error_ = std::string(llhttp_errno_name(err)) + ": " + llhttp_get_error_reason(&parser_);
    // 出错后解析器进入错误态,重置以便接收下一条消息
    reset();
    return false;
}

bool http_parse::feed_all(const char *data, size_t len)
{
    reset();
    return feed(data, len);
}

void http_parse::clear_result()
{
    http_method.clear();
    http_url.clear();
    http_version.clear();
    http_body.clear();
    http_headers.clear();
    status_code = 0;
}
