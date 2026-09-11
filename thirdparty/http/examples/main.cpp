#include <iostream>
#include <stdexcept>

#include "http_parse.h"
#include "http_protocol.h"

int main()
{
    // ---- 1. 一站式生成(推荐用法) ----
    std::string get_text = http_protocol::request(
        "GET", "/", {{"Host", "example.com"}, {"User-Agent", "demo"}});

    std::string json_text = http_protocol::json(
        "POST", "/api/login", R"({"user":"alice","pwd":"123456"})",
        {{"Host", "example.com"}});

    std::string form_text = http_protocol::form(
        "POST", "/search", {{"q", "c++ http"}, {"page", "1"}});

    std::string resp_text = http_protocol::response(
        200, {{"Content-Type", "text/plain"}}, "hello world");

    // 链式细控:临时对象直接链,一行到底
    std::string chain_text = http_protocol()
        .request_line("PUT", "/api/user")
        .header("Host", "example.com")
        .body("update")
        .build();

    std::cout << "===== GET =====\n" << get_text
              << "===== JSON =====\n" << json_text
              << "===== FORM =====\n" << form_text
              << "===== RESPONSE =====\n" << resp_text
              << "===== CHAIN =====\n" << chain_text;

    // ---- 2. 非法输入会被校验拦截(防 CRLF 头注入) ----
    try
    {
        http_protocol::request("GET", "/", {{"Host\r\nX-Injected", "evil"}});
    }
    catch (const std::invalid_argument &e)
    {
        std::cout << "rejected: " << e.what() << "\n";
    }

    // ---- 3. 生成 + 解析回环验证 ----
    http_parse parser(HTTP_REQUEST);
    if (!parser.feed_all(json_text.data(), json_text.size()))
    {
        std::cerr << "parse failed: " << parser.error() << "\n";
        return 1;
    }
    std::cout << "===== parsed JSON request =====\n"
              << "method:  " << parser.http_method << "\n"
              << "url:     " << parser.http_url << "\n"
              << "version: " << parser.http_version << "\n"
              << "keep-alive: " << std::boolalpha << parser.keep_alive() << "\n"
              << "content-type: " << parser.header("content-type") << "\n"
              << "body:    " << parser.http_body << "\n";

    http_parse rparser(HTTP_RESPONSE);
    if (!rparser.feed_all(resp_text.data(), resp_text.size()))
    {
        std::cerr << "parse failed: " << rparser.error() << "\n";
        return 1;
    }
    std::cout << "===== parsed response =====\n"
              << "status: " << rparser.status_code << "\n"
              << "body:   " << rparser.http_body << "\n";

    return 0;
}
