#include <coro/http_client.hpp>
#include "detail/http_transport.hpp"

#include <coro/dns.hpp>
#include <coro/exceptions.hpp>
#include <coro/pool.hpp>
#include <coro/stream.hpp>
#include <coro/sync.hpp>
#include <coro/tls.hpp>
#include <coro/wait.hpp>
#include <http_parse.h>
#include <http_protocol.h>

#include <algorithm>
#include <atomic>
#include <charconv>
#include <map>
#include <optional>
#include <system_error>
#include <tuple>

namespace coro {
    namespace {

        char ascii_lower(char c) {
            return c >= 'A' && c <= 'Z' ? static_cast<char>(c + ('a' - 'A')) : c;
        }

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
                throw std::invalid_argument(
                    "HTTP URL requires a DNS/IPv4 host without userinfo; IPv6 is not supported yet");
            const auto colon = authority.find(':');
            auto host = authority.substr(0, colon);
            if (host.empty())
                throw std::invalid_argument("HTTP URL host is empty");
            for (char c : host)
                if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' ||
                      c == '.'))
                    throw std::invalid_argument("invalid HTTP URL host");
            result.host.assign(host);
            std::transform(result.host.begin(), result.host.end(), result.host.begin(), ascii_lower);
            if (colon != std::string_view::npos) {
                const auto port_text = authority.substr(colon + 1);
                unsigned int port = 0;
                const auto parsed = std::from_chars(port_text.data(), port_text.data() + port_text.size(), port);
                if (parsed.ec != std::errc{} || parsed.ptr != port_text.data() + port_text.size() || port == 0 ||
                    port > 65535)
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
                connection->transport.tls_stream.emplace(std::move(connection->transport.socket), context->context,
                                                         endpoint.host);
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

        Task<HttpResponse> read_response(detail::HttpTransport& connection, bool head,
                                         const HttpClient::Options& options) {
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
        std::atomic<bool> closed{false};
        EventLoop* owner = &EventLoop::get();
        std::shared_ptr<ClientTLS> tls;
        std::map<std::tuple<bool, std::string, unsigned short>, std::shared_ptr<pool_type>> pools;
        /// 在途请求计数: aclose() 的屏障依据。只在本 loop 线程增减。
        std::size_t in_flight = 0;
        /// 在途计数归零时触发, 唤醒 aclose() 等待者 (Event 本身可跨线程 set)。
        std::shared_ptr<Event> idle_event;

        explicit impl(Options opts) : options(std::move(opts)) {}

        void check_loop() const {
            if (&EventLoop::get() != owner)
                throw std::logic_error("HttpClient must be used on its owning event loop");
        }

        /// 关闭全部连接池 (只能在 owner loop 线程调用)
        void close_pools() {
            for (auto& [key, pool] : pools)
                pool->close();
            pools.clear();
        }

        /// 一个流结束/被丢弃时归还背压名额
        void release_inflight() {
            if (in_flight > 0 && --in_flight == 0 && idle_event)
                idle_event->set();
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
            auto pool = std::make_shared<pool_type>(
                [endpoint, context = tls] { return connect_to(endpoint, context); },
                pool_type::options{options.max_connections_per_origin, options.max_idle_per_origin});
            pools.emplace(key, pool);
            return pool;
        }
    };

    HttpClient::HttpClient() : HttpClient(Options{}) {}

    HttpClient::HttpClient(Options options) {
        if (options.request_timeout.count() < 0 || options.shutdown_timeout.count() <= 0 ||
            options.idle_timeout.count() <= 0 || options.max_origins == 0 || options.max_connections_per_origin == 0 ||
            options.max_body_bytes == 0 || options.max_request_wire_bytes == 0 || options.max_header_bytes == 0 ||
            options.max_header_count == 0 || options.max_response_wire_bytes == 0)
            throw std::invalid_argument("invalid HTTP client limits or timeouts");
        impl_ = std::make_shared<impl>(std::move(options));
    }

    HttpClient::~HttpClient() {
        // 析构不得抛异常: close() 的跨线程路径只置标记并投递清理。
        try {
            close();
        } catch (...) {
        }
    }
    HttpClient::HttpClient(HttpClient&&) noexcept = default;
    HttpClient& HttpClient::operator=(HttpClient&& other) noexcept {
        if (this != &other) {
            close();
            impl_ = std::move(other.impl_);
        }
        return *this;
    }

    bool HttpClient::closed() const noexcept {
        return !impl_ || impl_->closed.load(std::memory_order_acquire);
    }

    bool HttpClient::supports_tls() noexcept {
#if defined(CORO_HAS_TLS) && CORO_HAS_TLS
        return true;
#else
        return false;
#endif
    }

    void HttpClient::close() {
        if (!impl_)
            return;
        auto state = impl_;
        if (state->closed.exchange(true, std::memory_order_acq_rel))
            return;
        if (&EventLoop::get() == state->owner) {
            state->close_pools();
            return;
        }
        // 跨线程 (典型: 在别的线程析构): 只置标记并把清理投递回 owner。
        // 如果 owner loop 已经不再运行, shared_ptr<impl> 析构时 Pool 也会关闭,
        // 因此这里不阻塞、不抛异常。
        try {
            state->owner->dispatch([state] { state->close_pools(); });
        } catch (...) {
        }
    }

    Task<void> HttpClient::aclose() {
        if (!impl_)
            co_return;
        auto state = impl_;
        state->check_loop();
        close(); // 只发起: 拒绝新请求 + 丢弃 idle 连接
        if (!state->idle_event)
            state->idle_event = std::make_shared<Event>();
        // in_flight 只在本 loop 线程递减 (协程收尾): 挂起等待 set() 唤醒后
        // 重查条件即可, clear 与 wait 之间不存在需要双检的窗口。
        while (state->in_flight > 0) {
            state->idle_event->clear();
            co_await state->idle_event->wait();
        }
        co_return;
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
        if (state->closed.load(std::memory_order_acquire))
            throw HttpError("HTTP client is closed");
        // 在途总量背压: 超过上限直接拒绝, 不静默无限排队。计数与限额检查同步,
        // 因为 perform 只在 owner loop 线程上推进 (co_await 不跨线程)。
        if (state->options.max_total_requests != 0 && state->in_flight >= state->options.max_total_requests)
            throw OverloadError("HTTP client request limit reached");
        struct inflight_scope {
            impl* state;
            ~inflight_scope() {
                if (--state->in_flight == 0 && state->idle_event)
                    state->idle_event->set();
            }
        };
        ++state->in_flight;
        inflight_scope inflight{state.get()};
        const auto endpoint = parse_url(request.url);
        if (endpoint.secure && !supports_tls())
            throw HttpError("HTTPS requires a TLS-enabled build of coro::http_client");
        const auto wire = serialize(request, endpoint); // Validate before opening a connection.
        if (wire.size() > state->options.max_request_wire_bytes)
            throw HttpError("HTTP request exceeds the configured wire size limit");
        auto pool = state->pool_for(endpoint);
        PoolLease<std::unique_ptr<Connection>> lease;
        for (;;) {
            lease = co_await pool->acquire();
            if (!lease)
                throw HttpError("HTTP connection pool is closed");
            if ((*lease)->transport.reusable() &&
                std::chrono::steady_clock::now() - (*lease)->last_used < state->options.idle_timeout)
                break;
            lease.discard();
        }
        struct lease_guard {
            PoolLease<std::unique_ptr<Connection>>& lease;
            bool reusable = false;
            ~lease_guard() {
                if (!reusable)
                    lease.discard();
            }
        } guard{lease};
        auto& connection = (*lease)->transport;
        stream_writer writer(connection);
        if (!co_await writer.write_all(wire.data(), wire.size()))
            throw HttpError("connection closed during HTTP request write");
        auto response = co_await read_response(connection, request.method == "HEAD", state->options);
        if (request.keep_alive && response.keep_alive && connection.reusable() &&
            !state->closed.load(std::memory_order_acquire)) {
            (*lease)->last_used = std::chrono::steady_clock::now();
            guard.reusable = true;
        } else {
            co_await connection.shutdown(state->options.shutdown_timeout);
        }
        co_return response;
    }

    // ============================================================================
    // 流式响应 (E05)
    // ============================================================================

    struct HttpResponseStream::impl {
        using pool_type = Pool<std::unique_ptr<Connection>>;

        std::shared_ptr<HttpClient::impl> client;
        PoolLease<std::unique_ptr<Connection>> lease;
        http_parse parser{HTTP_RESPONSE};
        std::deque<std::string> chunks;
        HttpHeaders headers;
        HttpHeaders trailers;
        std::size_t body_bytes = 0;
        std::size_t wire_bytes = 0;
        std::size_t max_body = 0;
        std::size_t max_wire = 0;
        int status_code = 0;
        bool want_keep_alive = false;
        bool headers_ready = false;
        bool done = false;
        bool finished = false;

        ~impl() {
            // 未读完就销毁 = 连接状态未知: 丢弃而不是放回池
            if (!finished && lease)
                lease.discard();
            if (client)
                client->release_inflight();
        }

        /// 收尾: 可复用则归还连接, 否则丢弃。幂等。
        void finish(bool reusable) {
            if (finished)
                return;
            finished = true;
            done = true;
            if (!lease)
                return;
            const bool pool_it = reusable && want_keep_alive && parser.keep_alive() && (*lease)->transport.reusable() &&
                                 client && !client->closed.load(std::memory_order_acquire);
            if (pool_it) {
                (*lease)->last_used = std::chrono::steady_clock::now();
                lease = PoolLease<std::unique_ptr<Connection>>{}; // 析构即归还到池
            } else {
                lease.discard();
                lease = PoolLease<std::unique_ptr<Connection>>{};
            }
        }
    };

    namespace {

        Task<std::optional<std::string>> read_stream_chunk(std::shared_ptr<HttpResponseStream::impl> s) {
            for (;;) {
                if (!s->chunks.empty()) {
                    auto chunk = std::move(s->chunks.front());
                    s->chunks.pop_front();
                    co_return chunk;
                }
                if (s->done)
                    co_return std::nullopt;
                char buffer[16384];
                const int count = co_await (*s->lease)->transport.read(buffer, sizeof(buffer));
                if (count < 0) {
                    // 先取证再收尾: finish(false) 会把 lease 重置为空,
                    // 之后解引用只会抛 "lease used after release", 真实 I/O 错误码丢失。
                    const auto ec = (*s->lease)->transport.last_error_code();
                    s->finish(false);
                    throw std::system_error(ec, "HTTP stream read failed");
                }
                if (count == 0) {
                    if (!s->parser.finish() || !s->parser.completed())
                        throw HttpError("incomplete HTTP response body at EOF: " + s->parser.error());
                    s->trailers = std::move(s->parser.trailer_fields);
                    s->finish(true);
                    co_return std::nullopt;
                }
                s->wire_bytes += static_cast<std::size_t>(count);
                if (s->wire_bytes > s->max_wire)
                    throw HttpError("HTTP response wire limit exceeded");
                if (!s->parser.feed(buffer, static_cast<std::size_t>(count)))
                    throw HttpError("HTTP response parse failed: " + s->parser.error());
                // 流式消费不需要在解析器里保留已交付的正文副本
                s->parser.http_body.clear();
                if (s->body_bytes > s->max_body)
                    throw HttpError("HTTP response body limit exceeded");
                if (s->parser.completed()) {
                    s->trailers = std::move(s->parser.trailer_fields);
                    s->finish(true);
                    if (s->chunks.empty())
                        co_return std::nullopt;
                }
            }
        }

    } // namespace

    HttpResponseStream::~HttpResponseStream() = default;
    HttpResponseStream::HttpResponseStream(HttpResponseStream&&) noexcept = default;
    HttpResponseStream& HttpResponseStream::operator=(HttpResponseStream&&) noexcept = default;

    bool HttpResponseStream::valid() const noexcept {
        return impl_ != nullptr;
    }
    int HttpResponseStream::status() const noexcept {
        return impl_ ? impl_->status_code : 0;
    }
    const HttpHeaders& HttpResponseStream::headers() const {
        static const HttpHeaders empty;
        return impl_ ? impl_->headers : empty;
    }
    const HttpHeaders& HttpResponseStream::trailers() const {
        static const HttpHeaders empty;
        return impl_ ? impl_->trailers : empty;
    }
    bool HttpResponseStream::keep_alive() const noexcept {
        return impl_ && impl_->parser.keep_alive();
    }

    Task<std::optional<std::string>> HttpResponseStream::read_chunk() {
        if (!impl_)
            co_return std::nullopt;
        co_return co_await read_stream_chunk(impl_);
    }

    Task<void> HttpResponseStream::aclose() {
        if (!impl_) {
            co_return;
        }
        if (impl_->lease) {
            // 未读完: 连接上可能还有未消费的字节, 不能复用
            co_await (*impl_->lease)->transport.shutdown(std::chrono::milliseconds(500));
            impl_->finish(false);
        }
        impl_.reset();
        co_return;
    }

    namespace {

        /// stream_open 的注解见 HttpClient::stream_open 定义处。
        /// (辅助函数 parse_url/serialize/ClientTLS 等仍是本命名空间的内部实现。)

    } // namespace

    /// stream() 的建连 + 请求写出 + 响应头阶段。
    /// 独立协程: 让 stream() 能用 wait_for 给这一阶段加上 request_timeout ——
    /// 只建连不回头的对端会让无超时的实现永久挂起, 且流对象持有 in_flight
    /// 名额直到销毁, 挂死的流积累后整个 client 会 OverloadError (自我 DoS)。
    /// 超时/取消展开时, 本协程帧内的 s (shared_ptr) 与 inflight 守卫析构,
    /// 连接被丢弃、名额被归还。body 阶段的读超时由调用方控制 (read_chunk)。
    Task<HttpResponseStream> HttpClient::stream_open(std::shared_ptr<HttpClient::impl> state, HttpRequest request) {
        if (state->closed.load(std::memory_order_acquire))
            throw HttpError("HTTP client is closed");
        if (state->options.max_total_requests != 0 && state->in_flight >= state->options.max_total_requests)
            throw OverloadError("HTTP client request limit reached");
        ++state->in_flight;
        struct guard {
            HttpClient::impl* state;
            bool armed = true;
            ~guard() {
                if (armed)
                    state->release_inflight();
            }
        } inflight{state.get()};

        const auto endpoint = parse_url(request.url);
        if (endpoint.secure && !HttpClient::supports_tls())
            throw HttpError("HTTPS requires a TLS-enabled build of coro::http_client");
        const auto wire = serialize(request, endpoint);
        if (wire.size() > state->options.max_request_wire_bytes)
            throw HttpError("HTTP request exceeds the configured wire size limit");
        auto pool = state->pool_for(endpoint);

        auto s = std::make_shared<HttpResponseStream::impl>();
        s->client = state;
        // 名额所有权自此移交给 s: 无论成功/超时/异常, 都由 ~impl 恰好归还一次。
        // (守卫只在 s 接管之前的失败路径上兜底 —— 两边都保留会双重归还,
        //  在多请求在途时偷走其他请求的名额。)
        inflight.armed = false;
        s->max_body = state->options.max_body_bytes;
        s->max_wire = state->options.max_response_wire_bytes;
        s->want_keep_alive = request.keep_alive;
        s->parser.body_limit = state->options.max_body_bytes;
        s->parser.header_bytes_limit = state->options.max_header_bytes;
        s->parser.header_count_limit = state->options.max_header_count;
        // pause_after_message 必须开启: llhttp 在消息完成时会清空 flags,
        // 从而丢失 keep-alive 判定所需的 F_CONTENT_LENGTH/F_CHUNKED。暂停模式下
        // 我们在暂停处自己停手, flags 保留, parser.keep_alive() 才是可信的。
        s->parser.pause_after_message(true);
        s->parser.headers_complete = [raw = s.get()] { raw->headers_ready = true; };
        s->parser.body = [raw = s.get()](const char* at, std::size_t len) {
            raw->body_bytes += len;
            if (raw->body_bytes <= raw->max_body)
                raw->chunks.emplace_back(at, len);
        };

        for (;;) {
            s->lease = co_await pool->acquire();
            if (!s->lease)
                throw HttpError("HTTP connection pool is closed");
            if ((*s->lease)->transport.reusable() &&
                std::chrono::steady_clock::now() - (*s->lease)->last_used < state->options.idle_timeout)
                break;
            s->lease.discard();
        }
        {
            stream_writer writer((*s->lease)->transport);
            if (!co_await writer.write_all(wire.data(), wire.size()))
                throw HttpError("connection closed during HTTP request write");
        }

        // 读到头部完成 (或整个小响应一次到位) 再交出流
        while (!s->headers_ready && !s->parser.completed()) {
            char buffer[16384];
            const int count = co_await (*s->lease)->transport.read(buffer, sizeof(buffer));
            if (count < 0)
                throw std::system_error((*s->lease)->transport.last_error_code(), "HTTP response read failed");
            if (count == 0) {
                if (!s->parser.finish() || !s->parser.completed())
                    throw HttpError("incomplete HTTP response headers at EOF: " + s->parser.error());
                break;
            }
            s->wire_bytes += static_cast<std::size_t>(count);
            if (s->wire_bytes > s->max_wire)
                throw HttpError("HTTP response wire limit exceeded");
            if (!s->parser.feed(buffer, static_cast<std::size_t>(count)))
                throw HttpError("HTTP response parse failed: " + s->parser.error());
        }
        s->parser.http_body.clear();
        if (s->parser.status_code == 101)
            throw HttpError("HTTP protocol upgrades are not supported");
        if (s->parser.status_code < 200)
            throw HttpError("informational responses are not supported by stream()");
        s->status_code = s->parser.status_code;
        s->headers = std::move(s->parser.header_fields);
        if (s->parser.completed()) {
            s->trailers = std::move(s->parser.trailer_fields);
            s->finish(true);
        }
        co_return HttpResponseStream(std::move(s));
    }

    Task<HttpResponseStream> HttpClient::stream(HttpRequest request) {
        if (closed())
            throw HttpError("HTTP client is closed");
        impl_->check_loop();
        // 建连到头部就绪受 request_timeout 约束 (与 request() 对齐, 见 stream_open 注释)。
        auto task = stream_open(impl_, std::move(request));
        if (impl_->options.request_timeout.count() > 0)
            return wait_for(std::move(task), impl_->options.request_timeout);
        return task;
    }

} // namespace coro
