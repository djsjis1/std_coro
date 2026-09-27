// stream_demo.cpp — coro::stream_reader / stream_writer 示例 (计划 M4a)
//
// 演示三件事:
//   1. 在真实 TcpStream 上按行读 (read_line 把短读与跨包边界都吸收掉);
//   2. write_all 处理短写, 把整段一次性交付;
//   3. 读满定长头: 用 read_exactly 拿到 body_len 指定的字节数, 不足时是
//      IncompleteStreamError (与对端正常 EOF、IO 错误、取消各自可区分)。
#include <coro/coro.hpp>
#include <coro/net.hpp>
#include <coro/pipe.hpp>
#include <coro/stream.hpp>

#include <cstring>
#include <iostream>
#include <string>

using namespace std::chrono_literals;

/// 极简单行协议: "NAME <word>\n" -> "OK <len>\n"
coro::Task<> server(coro::net::TcpListener* listener) {
    auto conn = co_await listener->accept();
    if (!conn.valid())
        co_return;
    coro::stream_reader r(conn);
    coro::stream_writer w(conn);
    while (auto line = co_await r.read_line()) {
        if (line->empty())
            break;
        std::string reply = "OK " + std::to_string(line->size()) + "\n";
        if (!co_await w.write_all(reply))
            break; // 对端不可写: 明确退出, 不死循环
    }
    co_return;
}

coro::Task<> client(unsigned short port, std::string* out) {
    auto sock = co_await coro::net::TcpStream::connect("127.0.0.1", port);
    if (!sock.valid()) {
        *out = "connect-failed";
        co_return;
    }
    coro::stream_writer w(sock);
    coro::stream_reader r(sock);

    (void)co_await w.write_all("hello stream world\n");
    (void)co_await w.write_all("\n"); // 空行让服务端结束循环

    if (auto line = co_await r.read_line())
        *out = *line;
    else
        *out = "eof";
    co_return;
}

/// 把"2 字节长度头 + 正文"写进管道; 结束时写端析构关闭, 读端据此区分 EOF 与错误
coro::Task<> write_frame(coro::pipe::PipeEnd w) {
    const std::string body = "payload";
    std::string blob;
    blob += static_cast<char>(0);                  // 长度头高字节
    blob += static_cast<char>(body.size() & 0xff); // 长度头低字节: 等于正文真实长度
    blob += body;
    (void)co_await w.write(blob.data(), blob.size());
    co_return;
}

/// 定长头 + 正文: 演示 read_exactly 与 IncompleteStreamError
coro::Task<> framing_demo(std::string* body, bool* incomplete) {
    // 用一条本地 pipe 充当字节源: PipeEnd 的 read/write 就是 awaitable<int>,
    // 因此它直接满足 AsyncReadable/AsyncWritable, 不需要任何适配。
    auto ends = coro::pipe::pair(4096);
    // 命名协程而不是临时 lambda 协程: 后者的闭包在全表达式结束就销毁, 而协程可能在
    // 那之后才从挂起点恢复 —— 恢复时读到的就是悬空捕获 (本仓库踩过多次的坑)。
    auto spawner = coro::spawn(write_frame(std::move(ends.second)));

    coro::stream_reader r(ends.first);
    char head[2];
    co_await r.read_exactly(head, 2); // 读满 2 字节的长度头
    const std::size_t want =
        (static_cast<std::size_t>(static_cast<unsigned char>(head[0])) << 8) | static_cast<unsigned char>(head[1]);
    try {
        std::string buf(want, '\0');
        co_await r.read_exactly(buf.data(), want);
        *body = buf;
        *incomplete = false;
    } catch (const coro::IncompleteStreamError&) {
        *incomplete = true; // 对端在凑够之前关掉了
    }
    co_await std::move(spawner);
    co_return;
}

int main() {
    std::string reply;
    std::string body;
    bool incomplete = false;
    coro::run([&]() -> coro::Task<> {
        coro::net::TcpListener listener;
        unsigned short port = 0;
        for (unsigned short candidate = 19720; candidate < 19740; ++candidate) {
            if (listener.bind_listen("127.0.0.1", candidate)) {
                port = candidate;
                break;
            }
        }
        if (port == 0) {
            reply = "bind-failed";
            co_return;
        }
        auto svc = coro::spawn(server(&listener));
        co_await client(port, &reply);
        co_await std::move(svc);
        listener.close();
        co_await framing_demo(&body, &incomplete);
    }());

    std::cout << "line-reply: " << reply << std::endl;
    std::cout << "framing body: " << body << " incomplete=" << (incomplete ? 1 : 0) << std::endl;
    // "hello stream world" 共 18 字节
    if (reply != "OK 18") {
        std::cerr << "unexpected reply" << std::endl;
        return 1;
    }
    if (body != "payload" || incomplete) {
        std::cerr << "framing demo mismatch" << std::endl;
        return 2;
    }
    std::cout << "ALL STREAM CASES PASSED" << std::endl;
    return 0;
}
