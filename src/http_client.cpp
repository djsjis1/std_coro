#include <coro/http_client.hpp>
#include "detail/http_transport.hpp"

#include <coro/dns.hpp>
#include <coro/pool.hpp>
#include <coro/stream.hpp>
#include <coro/tls.hpp>
#include <coro/wait.hpp>
#include <http_parse.h>
#include <http_protocol.h>

#include <algorithm>
#include <charconv>
#include <map>
#include <optional>
#include <system_error>
#include <tuple>

namespace coro {
namespace {

char ascii_lower(char c) { return c >= 'A' && c <= 'Z' ? static_cast<char>(c + ('a' - 'A')) : c; }

bool equal_field(std::string_view a, std::string_view b) noexcept {
    if (a.size() != b.size())
        return false;
    for (std::size_t i = 0; i < a.size(); ++i)
        if (ascii_lower(a[i]) != ascii_lower(b[i]))
            return false;
    return true;
}

struct Endpoint {
    std::string host;
    std::string authority;
    std::string target;
    unsigned short port;
    bool secure;
};

Endpoint parse_url(std::string_view url) {
    if (url.size() > 8192)
        throw std::invalid_argument("HTTP URL exceeds 8192 bytes");
    for (unsigned char c : url)
        if (c <= 0x20 || c >= 0x7f || c == '\\')
            throw std::invalid_argument("HTTP URL must use ASCII and percent-encoded path/query bytes");
    const auto split = url.find("://");
    if (split == std::string_view::npos)
        throw std::invalid_argument("HTTP URL requires an explicit http:// or https:// scheme");
    const auto scheme = url.substr(0, split);
    if (!equal_field(scheme, "http") && !equal_field(scheme, "https"))
        throw std::invalid_argument("unsupported HTTP URL scheme");
    Endpoint result;
    result.secure = equal_field(scheme, "https");
    result.port = result.secure ? 443 : 80;
    auto remainder = url.substr(split + 3);
    const auto end = remainder.find_first_of("/?#");
    auto authority = remainder.substr(0, end);
    if (authority.empty() || authority.find_first_of("@[]") != std::string_view::npos)
        throw std::invalid_argument("HTTP URL requires a DNS/IPv4 host without userinfo; IPv6 is not supported yet");
    const auto colon = authority.find(':');
    auto host = authority.substr(0, colon);
    if (host.empty())
        throw std::invalid_argument("HTTP URL host is empty");
    for (char c : host)
        if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '.'))
            throw std::invalid_argument("invalid HTTP URL host");
    result.host.assign(host);
    std::transform(result.host.begin(), result.host.end(), result.host.begin(), ascii_lower);
    if (colon != std::string_view::npos) {
        const auto port_text = authority.substr(colon + 1);
        unsigned int port = 0;
        const auto parsed = std::from_chars(port_text.data(), port_text.data() + port_text.size(), port);
        if (parsed.ec != std::errc{} || parsed.ptr != port_text.data() + port_text.size() || port == 0 || port > 65535)
            throw std::invalid_argument("invalid HTTP URL port");
        result.port = static_cast<unsigned short>(port);
    }
    result.authority = result.host;
    if (result.port != (result.secure ? 443 : 80))
        result.authority += ':' + std::to_string(result.port);
    auto target = end == std::string_view::npos ? std::string_view{} : remainder.substr(end);
    target = target.substr(0, target.find('#')); // Fragments never go on the wire.
    result.target = target.empty() || target.front() == '?' ? "/" + std::string(target) : std::string(target);
    return result;
}

std::string serialize(const HttpRequest& request, const Endpoint& endpoint) {
    if (request.method.find('\0') != std::string::npos || request.method == "CONNECT")
        throw std::invalid_argument("invalid or unsupported HTTP method");
    http_protocol builder;
    builder.request_line(request.method.c_str(), endpoint.target.c_str());
    builder.header("Host", endpoint.authority);
    builder.header("Connection", request.keep_alive ? "keep-alive" : "close");
    for (const auto& [name, value] : request.headers) {
        for (auto owned : {"Host", "Content-Length", "Transfer-Encoding", "Connection", "Expect", "Upgrade",
                           "Trailer", "Proxy-Connection"})
            if (equal_field(name, owned))
                throw std::invalid_argument("HTTP client manages header: " + name);
        builder.header(name, value);
    }
    builder.body(request.body);
    return builder.build();
}

struct ClientTLS {
#if defined(CORO_HAS_TLS) && CORO_HAS_TLS
    tls::TlsContext context{tls::TlsContext::role::client};
    explicit ClientTLS(const std::string& bundle) {
        const bool loaded = bundle.empty() ? context.load_system_trust() : context.load_verify_file(bundle);
        if (!loaded)
            throw HttpError("cannot configure TLS trust anchors");
        context.set_alpn({"http/1.1"});
    }
#else
    explicit ClientTLS(const std::string&) { throw HttpError("this HTTP client was built without TLS"); }
#endif
};

struct Connection {
    detail::HttpTransport transport;
    std::chrono::steady_clock::time_point last_used;
};

Task<std::unique_ptr<Connection>> connect_to(Endpoint endpoint, std::shared_ptr<ClientTLS> context) {
    auto addresses = co_await net::resolve(endpoint.host, std::to_string(endpoint.port));
    auto connection = std::make_unique<Connection>();
    std::error_code last_error = std::make_error_code(std::errc::host_unreachable);
    for (auto& address : addresses) {
        auto socket = co_await net::TcpStream::connect(address.address.c_str(), address.port);
        if (socket.valid()) {
            connection->transport.socket = std::move(socket);
            break;
        }
        last_error = std::error_code(io::last_error(), std::system_category());
    }
    if (!connection->transport.socket.valid())
        throw std::system_error(last_error, "HTTP connect failed");
    if (endpoint.secure) {
#if defined(CORO_HAS_TLS) && CORO_HAS_TLS
        connection->transport.tls_stream.emplace(std::move(connection->transport.socket), context->context, endpoint.host);
        if (!co_await connection->transport.tls_stream->handshake())
            throw HttpError(connection->transport.tls_stream->last_error());
        const auto protocol = connection->transport.tls_stream->negotiated_alpn();
        if (!protocol.empty() && protocol != "http/1.1")
            throw HttpError("peer selected an unsupported ALPN protocol");
#else
        (void)context;
        throw HttpError("this HTTP client was built without TLS");
#endif
    }
    connection->last_used = std::chrono::steady_clock::now();
    co_return connection;
}

Task<HttpResponse> read_response(detail::HttpTransport& connection, bool head, const HttpClient::Options& options) {
    http_parse parser(HTTP_RESPONSE);
    parser.pause_after_message(true);
    parser.response_to_head(head);
    parser.body_limit = options.max_body_bytes;
    parser.header_bytes_limit = options.max_header_bytes;
    parser.header_count_limit = options.max_header_count;
    char buffer[16384];
    std::size_t wire_bytes = 0;
    unsigned int informational = 0;
    for (;;) {
        const int count = co_await connection.read(buffer, sizeof(buffer));
        if (count < 0)
            throw std::system_error(connection.last_error_code(), "HTTP response read failed");
        if (count == 0) {
            if (!parser.finish() || !parser.completed() || parser.status_code < 200)
                throw HttpError("incomplete HTTP response at EOF: " + parser.error());
            break;
        }
        const auto received = static_cast<std::size_t>(count);
        if (received > options.max_response_wire_bytes - wire_bytes)
            throw HttpError("HTTP response wire limit exceeded");
        wire_bytes += received;
        std::size_t offset = 0;
        while (offset < received) {
            if (!parser.feed(buffer + offset, received - offset))
                throw HttpError("HTTP response parse failed: " + parser.error());
            const auto consumed = parser.consumed_bytes();
            if (consumed == 0)
                throw HttpError("HTTP parser made no progress");
            offset += consumed;
            if (parser.completed()) {
                if (parser.status_code == 101)
                    throw HttpError("HTTP protocol upgrades are not supported");
                if (parser.status_code < 200) {
                    if (++informational > 8)
                        throw HttpError("too many informational HTTP responses");
                    parser.resume();
                } else {
                    if (offset != received)
                        throw HttpError("unexpected bytes after the HTTP response");
                    break;
                }
            }
        }
        if (parser.completed())
            break;
    }
    HttpResponse response;
    response.status = parser.status_code;
    response.version = std::move(parser.http_version);
    response.headers = std::move(parser.header_fields);
    response.trailers = std::move(parser.trailer_fields);
    response.body = std::move(parser.http_body);
    response.keep_alive = parser.keep_alive();
    co_return response;
}

} // namespace

struct HttpClient::impl {
    using pool_type = Pool<std::unique_ptr<Connection>>;
    Options options;
    bool closed = false;
    EventLoop* owner = &EventLoop::get();
    std::shared_ptr<ClientTLS> tls;
    std::map<std::tuple<bool, std::string, unsigned short>, std::shared_ptr<pool_type>> pools;

    explicit impl(Options opts) : options(std::move(opts)) {}

    void check_loop() const {
        if (&EventLoop::get() != owner)
            throw std::logic_error("HttpClient must be used on its owning event loop");
    }

    std::shared_ptr<pool_type> pool_for(const Endpoint& endpoint) {
        const auto key = std::make_tuple(endpoint.secure, endpoint.host, endpoint.port);
        if (auto found = pools.find(key); found != pools.end())
            return found->second;
        if (pools.size() == options.max_origins) {
            const auto idle = std::find_if(pools.begin(), pools.end(), [](const auto& item) {
                return item.second->in_use_count() == 0 && item.second->waiting_count() == 0;
            });
            if (idle == pools.end())
                throw HttpError("HTTP origin limit reached while all pools are in use");
            idle->second->close();
            pools.erase(idle);
        }
        if (endpoint.secure && !tls)
            tls = std::make_shared<ClientTLS>(options.ca_bundle);
        auto pool = std::make_shared<pool_type>([endpoint, context = tls] { return connect_to(endpoint, context); },
            pool_type::options{options.max_connections_per_origin, options.max_idle_per_origin});
        pools.emplace(key, pool);
        return pool;
    }
};

HttpClient::HttpClient() : HttpClient(Options{}) {}

HttpClient::HttpClient(Options options) {
    if (options.request_timeout.count() < 0 || options.shutdown_timeout.count() <= 0 ||
        options.idle_timeout.count() <= 0 || options.max_origins == 0 ||
        options.max_connections_per_origin == 0 || options.max_body_bytes == 0 ||
        options.max_header_bytes == 0 || options.max_header_count == 0 || options.max_response_wire_bytes == 0)
        throw std::invalid_argument("invalid HTTP client limits or timeouts");
    impl_ = std::make_shared<impl>(std::move(options));
}

HttpClient::~HttpClient() { close(); }
HttpClient::HttpClient(HttpClient&&) noexcept = default;
HttpClient& HttpClient::operator=(HttpClient&& other) noexcept {
    if (this != &other) {
        close();
        impl_ = std::move(other.impl_);
    }
    return *this;
}

bool HttpClient::closed() const noexcept { return !impl_ || impl_->closed; }

bool HttpClient::supports_tls() noexcept {
#if defined(CORO_HAS_TLS) && CORO_HAS_TLS
    return true;
#else
    return false;
#endif
}

void HttpClient::close() {
    if (!impl_ || impl_->closed)
        return;
    impl_->check_loop();
    impl_->closed = true;
    for (auto& [key, pool] : impl_->pools)
        pool->close();
    impl_->pools.clear();
}

Task<HttpResponse> HttpClient::request(HttpRequest request) {
    if (closed())
        throw HttpError("HTTP client is closed");
    impl_->check_loop();
    auto task = perform(impl_, std::move(request));
    if (impl_->options.request_timeout.count() > 0)
        return wait_for(std::move(task), impl_->options.request_timeout);
    return task;
}

Task<HttpResponse> HttpClient::get(std::string url) {
    HttpRequest request;
    request.url = std::move(url);
    return this->request(std::move(request));
}

Task<HttpResponse> HttpClient::perform(std::shared_ptr<impl> state, HttpRequest request) {
    state->check_loop();
    if (state->closed)
        throw HttpError("HTTP client is closed");
    const auto endpoint = parse_url(request.url);
    if (endpoint.secure && !supports_tls())
        throw HttpError("HTTPS requires a TLS-enabled build of coro::http_client");
    const auto wire = serialize(request, endpoint); // Validate before opening a connection.
    auto pool = state->pool_for(endpoint);
    PoolLease<std::unique_ptr<Connection>> lease;
    for (;;) {
        lease = co_await pool->acquire();
        if (!lease)
            throw HttpError("HTTP connection pool is closed");
        if ((*lease)->transport.reusable() && std::chrono::steady_clock::now() - (*lease)->last_used < state->options.idle_timeout)
            break;
        lease.discard();
    }
    struct lease_guard {
        PoolLease<std::unique_ptr<Connection>>& lease;
        bool reusable = false;
        ~lease_guard() { if (!reusable) lease.discard(); }
    } guard{lease};
    auto& connection = (*lease)->transport;
    stream_writer writer(connection);
    if (!co_await writer.write_all(wire.data(), wire.size()))
        throw HttpError("connection closed during HTTP request write");
    auto response = co_await read_response(connection, request.method == "HEAD", state->options);
    if (request.keep_alive && response.keep_alive && connection.reusable() && !state->closed) {
        (*lease)->last_used = std::chrono::steady_clock::now();
        guard.reusable = true;
    } else {
        co_await connection.shutdown(state->options.shutdown_timeout);
    }
    co_return response;
}

} // namespace coro
