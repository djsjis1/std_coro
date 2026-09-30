// 本地 TLS 往返: example_tls <localhost-cert.pem> <private-key.pem>
// 证书需包含 DNS:localhost; 客户端显式信任该证书, 保持主机名验证开启。
#include <coro/coro.hpp>
#include <coro/net.hpp>
#include <coro/stream.hpp>
#include <coro/tls.hpp>

#include <chrono>
#include <exception>
#include <iostream>
#include <stdexcept>
#include <string>
#include <utility>

using namespace std::chrono_literals;

namespace {

    coro::Task<> serve_one(coro::net::TcpListener& listener, const coro::tls::TlsContext& context) {
        auto socket = co_await listener.accept();
        if (!socket.valid())
            throw std::runtime_error("accept failed");
        coro::tls::TlsStream stream(std::move(socket), context, "");
        if (!co_await stream.handshake())
            throw std::runtime_error(stream.last_error());
        coro::stream_reader reader(stream);
        coro::stream_writer writer(stream);
        auto line = co_await reader.read_line();
        if (!line || *line != "tls-ping")
            throw std::runtime_error("server: expected tls-ping");
        if (!co_await writer.write_line("tls-pong"))
            throw std::runtime_error("server: write failed");
        if (!co_await stream.shutdown())
            throw std::runtime_error("server: incomplete close_notify: " + stream.last_error());
    }

    coro::Task<> exchange(std::string certificate, std::string private_key) {
        // Context 先于 stream 构造并活到会话结束; 私钥只交给服务端。
        coro::tls::TlsContext server_context(coro::tls::TlsContext::role::server);
        if (!server_context.use_certificate_file(certificate) ||
            !server_context.use_private_key_file(private_key))
            throw std::runtime_error("cannot load server certificate/private key");
        coro::tls::TlsContext client_context(coro::tls::TlsContext::role::client);
        if (!client_context.load_verify_file(certificate))
            throw std::runtime_error("cannot load client trust anchor");

        coro::net::TcpListener listener;
        if (!listener.bind_listen("127.0.0.1", 0))
            throw std::runtime_error("listen failed");
        auto server = coro::spawn(serve_one(listener, server_context));
        auto socket = co_await coro::net::TcpStream::connect("127.0.0.1", listener.local_port());
        if (!socket.valid())
            throw std::runtime_error("connect failed");
        coro::tls::TlsStream stream(std::move(socket), client_context, "localhost");
        if (!co_await stream.handshake())
            throw std::runtime_error(stream.last_error());
        std::cout << "Verified localhost, negotiated " << stream.negotiated_version() << '\n';

        coro::stream_writer writer(stream);
        coro::stream_reader reader(stream);
        if (!co_await writer.write_line("tls-ping"))
            throw std::runtime_error("client: write failed");
        auto line = co_await reader.read_line();
        if (!line || *line != "tls-pong")
            throw std::runtime_error("client: expected tls-pong");
        // false 表示关闭交换不完整, 调用方必须丢弃连接; 此例由 RAII 关闭 socket。
        if (!co_await stream.shutdown())
            throw std::runtime_error("client: incomplete close_notify: " + stream.last_error());
        co_await std::move(server);
        std::cout << "tls-ping -> tls-pong; close_notify completed on both peers\n";
    }

} // namespace

int main(int argc, char** argv) {
    if (argc != 3) {
        std::cerr << "Usage: example_tls <localhost-cert.pem> <private-key.pem>\n";
        return 2;
    }
    try {
        coro::run(coro::wait_for(exchange(argv[1], argv[2]), 10s));
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "TLS demo failed: " << error.what() << '\n';
        return 1;
    }
}
