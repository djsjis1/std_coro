#include "http_parse.h"

#include <stdexcept>

namespace {

http_parse* self(llhttp_t* parser) { return static_cast<http_parse*>(parser->data); }

char ascii_lower(char c) { return c >= 'A' && c <= 'Z' ? static_cast<char>(c + ('a' - 'A')) : c; }

bool field_equals(const std::string& a, const std::string& b) {
    if (a.size() != b.size())
        return false;
    for (size_t i = 0; i < a.size(); ++i)
        if (ascii_lower(a[i]) != ascii_lower(b[i]))
            return false;
    return true;
}

bool exceeds(size_t used, size_t added, size_t limit) {
    return limit != 0 && (used > limit || added > limit - used);
}

} // namespace

const std::string http_parse::empty_string_;

const std::string& http_parse::header(const std::string& field) const {
    for (const auto& h : http_headers)
        if (field_equals(h.first, field))
            return h.second;
    return empty_string_;
}

bool http_parse::keep_alive() const { return completed_ && llhttp_should_keep_alive(&parser_) != 0; }

http_parse::http_parse(llhttp_type_t type) : type_(type) {
    setup_callbacks();
    reset();
}

void http_parse::setup_callbacks() {
    llhttp_settings_init(&settings_);
    settings_.on_message_begin = [](llhttp_t* parser) -> int {
        auto* p = self(parser);
        return p->callback([&] {
            p->clear_result();
            if (p->message_begin)
                p->message_begin();
            return 0;
        });
    };
    settings_.on_url = [](llhttp_t* parser, const char* at, size_t length) -> int {
        auto* p = self(parser);
        return p->callback([&] {
            if (exceeds(p->http_url.size(), length, p->url_limit)) {
                llhttp_set_error_reason(parser, "url limit exceeded");
                return -1;
            }
            p->http_url.append(at, length);
            if (p->url)
                p->url(at, length);
            return 0;
        });
    };
    settings_.on_header_field = [](llhttp_t* parser, const char* at, size_t length) -> int {
        auto* p = self(parser);
        return p->callback([&] {
            if (exceeds(p->header_bytes_, length, p->header_bytes_limit)) {
                llhttp_set_error_reason(parser, "header bytes limit exceeded");
                return -1;
            }
            p->header_bytes_ += length;
            p->current_field_.append(at, length);
            if (p->header_field)
                p->header_field(at, length);
            return 0;
        });
    };
    settings_.on_header_value = [](llhttp_t* parser, const char* at, size_t length) -> int {
        auto* p = self(parser);
        return p->callback([&] {
            if (exceeds(p->header_bytes_, length, p->header_bytes_limit)) {
                llhttp_set_error_reason(parser, "header bytes limit exceeded");
                return -1;
            }
            p->header_bytes_ += length;
            p->current_value_.append(at, length);
            if (p->header_value)
                p->header_value(at, length);
            return 0;
        });
    };
    settings_.on_header_value_complete = [](llhttp_t* parser) -> int {
        auto* p = self(parser);
        return p->callback([&] {
            if (p->header_count_limit && p->header_count_ >= p->header_count_limit) {
                llhttp_set_error_reason(parser, "header count limit exceeded");
                return -1;
            }
            ++p->header_count_;
            if (parser->flags & F_TRAILING) {
                p->trailer_fields.emplace_back(p->current_field_, p->current_value_);
            } else {
                p->http_headers[p->current_field_] = p->current_value_;
                p->header_fields.emplace_back(p->current_field_, p->current_value_);
            }
            p->current_field_.clear();
            p->current_value_.clear();
            return 0;
        });
    };
    settings_.on_headers_complete = [](llhttp_t* parser) -> int {
        return llhttp_get_type(parser) == HTTP_RESPONSE && self(parser)->response_to_head_ ? 1 : 0;
    };
    settings_.on_body = [](llhttp_t* parser, const char* at, size_t length) -> int {
        auto* p = self(parser);
        return p->callback([&] {
            if (exceeds(p->http_body.size(), length, p->body_limit)) {
                llhttp_set_error_reason(parser, "body limit exceeded");
                return -1;
            }
            p->http_body.append(at, length);
            if (p->body)
                p->body(at, length);
            return 0;
        });
    };
    settings_.on_message_complete = [](llhttp_t* parser) -> int {
        auto* p = self(parser);
        return p->callback([&] {
            p->http_version = std::to_string(llhttp_get_http_major(parser)) + "." +
                              std::to_string(llhttp_get_http_minor(parser));
            if (llhttp_get_type(parser) == HTTP_REQUEST)
                p->http_method = llhttp_method_name(static_cast<llhttp_method_t>(parser->method));
            else
                p->status_code = llhttp_get_status_code(parser);
            p->completed_ = true;
            if (p->message_complete)
                p->message_complete();
            return p->pause_after_message_ ? static_cast<int>(HPE_PAUSED) : 0;
        });
    };
}

void http_parse::reset() {
    error_.clear();
    callback_error_ = nullptr;
    current_field_.clear();
    current_value_.clear();
    header_bytes_ = 0;
    header_count_ = 0;
    consumed_bytes_ = 0;
    completed_ = false;
    paused_ = false;
    llhttp_init(&parser_, type_, &settings_);
    parser_.data = this;
}

bool http_parse::accept_result(llhttp_errno_t error) {
    if (callback_error_)
        std::rethrow_exception(callback_error_);
    if (error == HPE_OK || (error == HPE_PAUSED && completed_)) {
        paused_ = error == HPE_PAUSED;
        return true;
    }
    const char* reason = llhttp_get_error_reason(&parser_);
    error_ = std::string(llhttp_errno_name(error)) + ": " + (reason ? reason : "HTTP parse failed");
    // Preserve both diagnostic and error state until the caller explicitly resets.
    return false;
}

bool http_parse::feed(const char* data, size_t len) {
    if (callback_error_)
        std::rethrow_exception(callback_error_);
    if (!data && len != 0)
        throw std::invalid_argument("http_parse::feed requires a buffer");
    consumed_bytes_ = 0;
    if (paused_)
        return true;
    if (!error_.empty())
        return false;
    if (len == 0)
        return true;
    const auto error = llhttp_execute(&parser_, data, len);
    const auto* position = llhttp_get_error_pos(&parser_);
    consumed_bytes_ = error == HPE_OK ? len : (position ? static_cast<size_t>(position - data) : 0);
    return accept_result(error);
}

bool http_parse::finish() {
    if (!error_.empty())
        return false;
    if (paused_ && completed_)
        return true;
    return accept_result(llhttp_finish(&parser_));
}

void http_parse::resume() {
    if (paused_) {
        llhttp_resume(&parser_);
        paused_ = false;
        completed_ = false;
    }
}

bool http_parse::feed_all(const char* data, size_t len) {
    reset();
    return feed(data, len);
}

void http_parse::clear_result() {
    http_method.clear();
    http_url.clear();
    http_version.clear();
    http_body.clear();
    http_headers.clear();
    header_fields.clear();
    trailer_fields.clear();
    status_code = 0;
    completed_ = false;
    header_bytes_ = 0;
    header_count_ = 0;
    current_field_.clear();
    current_value_.clear();
}
