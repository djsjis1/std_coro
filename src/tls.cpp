#include <coro/tls.hpp>

#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>

#include <algorithm>
#include <climits>
#include <cstring>
#include <stdexcept>
#include <utility>

namespace {

    // SSL 的 app_data 指向会话自己的协议快照, 不借用 TlsContext 的地址。
    int select_alpn(SSL* ssl, const unsigned char** out, unsigned char* outlen,
                    const unsigned char* in, unsigned int inlen, void*) {
        auto* protocols = static_cast<const std::vector<std::string>*>(SSL_get_app_data(ssl));
        if (!protocols || protocols->empty())
            return SSL_TLSEXT_ERR_NOACK;
        for (const auto& protocol : *protocols) {
            unsigned int offset = 0;
            while (offset < inlen) {
                const unsigned int size = in[offset++];
                if (size == 0 || size > inlen - offset)
                    return SSL_TLSEXT_ERR_ALERT_FATAL;
                if (size == protocol.size() && std::memcmp(in + offset, protocol.data(), size) == 0) {
                    *out = in + offset;
                    *outlen = static_cast<unsigned char>(size);
                    return SSL_TLSEXT_ERR_OK;
                }
                offset += size;
            }
        }
        return SSL_TLSEXT_ERR_ALERT_FATAL;
    }

} // namespace

namespace coro::tls {

    std::string TlsError::describe(unsigned long error) {
        if (error == 0)
            return "no OpenSSL error recorded";
        char buffer[256];
        ERR_error_string_n(error, buffer, sizeof(buffer));
        return buffer;
    }

    struct TlsContext::impl {
        SSL_CTX* ctx = nullptr;
        ~impl() { SSL_CTX_free(ctx); }
    };

    TlsContext::TlsContext(role r) : impl_(std::make_unique<impl>()), role_(r) {
        ERR_clear_error();
        impl_->ctx = SSL_CTX_new(r == role::client ? TLS_client_method() : TLS_server_method());
        if (!impl_->ctx)
            throw TlsError("SSL_CTX_new failed", ERR_get_error());
        if (SSL_CTX_set_min_proto_version(impl_->ctx, TLS1_2_VERSION) != 1)
            throw TlsError("cannot set minimum TLS version", ERR_get_error());
        SSL_CTX_set_mode(impl_->ctx, SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);
        SSL_CTX_set_verify_depth(impl_->ctx, verify_depth_);
        if (r == role::client)
            SSL_CTX_set_verify(impl_->ctx, SSL_VERIFY_PEER, nullptr);
        else
            SSL_CTX_set_alpn_select_cb(impl_->ctx, select_alpn, nullptr);
    }

    TlsContext::~TlsContext() = default;
    TlsContext::TlsContext(TlsContext&&) noexcept = default;
    TlsContext& TlsContext::operator=(TlsContext&&) noexcept = default;

    bool TlsContext::load_system_trust() {
        return impl_ && SSL_CTX_set_default_verify_paths(impl_->ctx) == 1;
    }

    bool TlsContext::load_verify_file(const std::string& path) {
        return impl_ && SSL_CTX_load_verify_locations(impl_->ctx, path.c_str(), nullptr) == 1;
    }

    bool TlsContext::use_certificate_file(const std::string& path) {
        return impl_ && SSL_CTX_use_certificate_chain_file(impl_->ctx, path.c_str()) == 1;
    }

    bool TlsContext::use_private_key_file(const std::string& path) {
        return impl_ && SSL_CTX_use_PrivateKey_file(impl_->ctx, path.c_str(), SSL_FILETYPE_PEM) == 1 &&
               SSL_CTX_check_private_key(impl_->ctx) == 1;
    }

    bool TlsContext::set_min_version(protocol_version version) {
        return impl_ && SSL_CTX_set_min_proto_version(
                            impl_->ctx, version == protocol_version::tls13 ? TLS1_3_VERSION : TLS1_2_VERSION) == 1;
    }

    void TlsContext::set_verify_depth(int depth) {
        if (depth < 0)
            throw std::invalid_argument("TLS verify depth must be non-negative");
        if (!impl_)
            throw std::logic_error("TlsContext was moved from");
        SSL_CTX_set_verify_depth(impl_->ctx, depth);
        verify_depth_ = depth;
    }

    void TlsContext::set_alpn(std::vector<std::string> protocols) {
        std::size_t size = 0;
        for (const auto& protocol : protocols) {
            if (protocol.empty() || protocol.size() > 255)
                throw std::invalid_argument("ALPN protocol length must be in [1, 255]");
            size += protocol.size() + 1;
            if (size > 65535)
                throw std::invalid_argument("ALPN protocol list is too large");
        }
        alpn_ = std::move(protocols);
    }

    struct TlsStream::impl {
        net::TcpStream sock;
        SSL* ssl = nullptr;
        BIO* rbio = nullptr;
        BIO* wbio = nullptr;
        TlsContext::role role = TlsContext::role::client;
        std::vector<std::string> alpn;
        bool handshake_done = false;
        bool broken = false;
        bool closing = false;
        bool closed = false;
        bool peer_closed = false;
        bool busy = false;

        ~impl() {
            if (ssl)
                SSL_free(ssl); // SSL_set_bio 已接管两个 BIO。
            else {
                BIO_free(rbio);
                BIO_free(wbio);
            }
        }

        // 取消和直接销毁挂起帧都会执行析构。未正常结束的操作污染会话, 不能继续复用。
        struct operation {
            impl& state;
            bool finished = false;
            explicit operation(impl& s) : state(s) {
                if (state.busy)
                    throw std::logic_error("TlsStream operations must not overlap");
                state.busy = true;
            }
            ~operation() {
                state.busy = false;
                if (!finished)
                    state.broken = true;
            }
        };
    };

    TlsStream::TlsStream(net::TcpStream socket, const TlsContext& context, std::string hostname)
        : impl_(std::make_unique<impl>()) {
        if (!context.impl_)
            throw std::invalid_argument("TlsStream requires a valid TlsContext");
        if (hostname.find('\0') != std::string::npos ||
            (context.role_ == TlsContext::role::client && context.hostname_verification_ && hostname.empty()))
            throw std::invalid_argument("TLS client requires a non-empty hostname without embedded NUL");
        impl_->sock = std::move(socket);
        impl_->role = context.role_;
        impl_->alpn = context.alpn_;
        ERR_clear_error();
        impl_->rbio = BIO_new(BIO_s_mem());
        impl_->wbio = BIO_new(BIO_s_mem());
        if (!impl_->rbio || !impl_->wbio)
            throw TlsError("BIO_new failed", ERR_get_error());
        impl_->ssl = SSL_new(context.impl_->ctx); // SSL 持有 SSL_CTX 引用, 不借用 Context 外壳。
        if (!impl_->ssl)
            throw TlsError("SSL_new failed", ERR_get_error());
        SSL_set_bio(impl_->ssl, impl_->rbio, impl_->wbio);
        if (SSL_set_app_data(impl_->ssl, &impl_->alpn) != 1)
            throw TlsError("SSL_set_app_data failed", ERR_get_error());
        if (impl_->role == TlsContext::role::client) {
            SSL_set_connect_state(impl_->ssl);
            if (context.hostname_verification_ && SSL_set1_host(impl_->ssl, hostname.c_str()) != 1)
                throw TlsError("SSL_set1_host failed", ERR_get_error());
            if (!hostname.empty()) {
                auto* ip = a2i_IPADDRESS(hostname.c_str());
                if (ip)
                    ASN1_OCTET_STRING_free(ip);
                else {
                    ERR_clear_error(); // 非 IP 是普通 DNS 主机名, 清理解析探测的错误。
                    if (SSL_set_tlsext_host_name(impl_->ssl, hostname.c_str()) != 1)
                        throw TlsError("cannot set TLS SNI", ERR_get_error());
                }
            }
            std::string wire;
            for (const auto& protocol : impl_->alpn) {
                wire.push_back(static_cast<char>(protocol.size()));
                wire += protocol;
            }
            if (!wire.empty() && SSL_set_alpn_protos(impl_->ssl,
                                    reinterpret_cast<const unsigned char*>(wire.data()),
                                    static_cast<unsigned int>(wire.size())) != 0)
                throw TlsError("cannot set ALPN protocols", ERR_get_error());
        } else {
            SSL_set_accept_state(impl_->ssl);
        }
    }

    TlsStream::~TlsStream() = default;
    TlsStream::TlsStream(TlsStream&&) noexcept = default;
    TlsStream& TlsStream::operator=(TlsStream&&) noexcept = default;

    void TlsStream::set_error(const char* where, unsigned long error) {
        if (impl_)
            impl_->broken = true;
        last_error_ = where;
        if (error)
            last_error_ += ": " + TlsError::describe(error);
        last_error_code_ = std::make_error_code(std::errc::protocol_error);
#ifdef _WIN32
        io::set_error(ERROR_INVALID_DATA);
#else
        io::set_error(EPROTO);
#endif
    }

    bool TlsStream::socket_valid() const noexcept { return impl_ && impl_->sock.valid(); }

    bool TlsStream::reusable() const noexcept {
        return socket_valid() && impl_->handshake_done && !impl_->broken && !impl_->closing &&
               !impl_->closed && !impl_->peer_closed && !impl_->busy;
    }

    Task<int> TlsStream::pull_from_socket() {
        char buffer[16 * 1024];
        const int count = co_await impl_->sock.read(buffer, sizeof(buffer));
        if (count < 0) {
            set_error("socket read failed", 0);
            co_return -1;
        }
        if (count > 0 && BIO_write(impl_->rbio, buffer, count) != count) {
            set_error("BIO_write failed", ERR_get_error());
            co_return -1;
        }
        co_return count;
    }

    Task<bool> TlsStream::flush_to_socket() {
        char buffer[16 * 1024];
        while (BIO_ctrl_pending(impl_->wbio) != 0) {
            const int count = BIO_read(impl_->wbio, buffer, sizeof(buffer));
            if (count <= 0) {
                set_error("BIO_read failed", ERR_get_error());
                co_return false;
            }
            for (int offset = 0; offset < count;) {
                const int written = co_await impl_->sock.write(buffer + offset, count - offset);
                if (written <= 0) {
                    set_error("socket write failed", 0);
                    co_return false;
                }
                offset += written;
            }
        }
        co_return true;
    }

    Task<bool> TlsStream::handshake() {
        if (!impl_ || impl_->broken || impl_->closing) {
            set_error("handshake on an unusable TLS stream", 0);
            co_return false;
        }
        impl::operation operation(*impl_);
        if (impl_->handshake_done) {
            operation.finished = true;
            co_return true;
        }
        for (;;) {
            // OpenSSL 错误队列属于线程, 其他协程也可能用它。调用前清空,
            // 调用后立即取分类与错误码, 绝不跨 co_await 再读 SSL_get_error。
            ERR_clear_error();
            const int rc = SSL_do_handshake(impl_->ssl);
            const int reason = rc == 1 ? SSL_ERROR_NONE : SSL_get_error(impl_->ssl, rc);
            const unsigned long error = ERR_get_error();
            if (reason != SSL_ERROR_NONE && reason != SSL_ERROR_WANT_READ && reason != SSL_ERROR_WANT_WRITE) {
                set_error("TLS handshake failed", error);
                co_return false;
            }
            if (!co_await flush_to_socket())
                co_return false;
            if (rc == 1) {
                if (impl_->role == TlsContext::role::client && SSL_get_verify_result(impl_->ssl) != X509_V_OK) {
                    set_error("peer certificate verification failed", 0);
                    co_return false;
                }
                impl_->handshake_done = true;
                last_error_.clear();
                last_error_code_.clear();
                operation.finished = true;
                co_return true;
            }
            if (reason == SSL_ERROR_WANT_READ && co_await pull_from_socket() <= 0) {
                set_error("peer closed during TLS handshake", 0);
                co_return false;
            }
        }
    }

    Task<int> TlsStream::read(char* buffer, std::size_t size) {
        if (!impl_ || !impl_->handshake_done || impl_->broken || impl_->closing) {
            set_error("read on an unusable TLS stream", 0);
            co_return -1;
        }
        impl::operation operation(*impl_);
        if (size == 0 || impl_->peer_closed) {
            operation.finished = true;
            co_return 0;
        }
        if (!buffer) {
            set_error("null TLS read buffer", 0);
            co_return -1;
        }
        const int amount = static_cast<int>(std::min(size, static_cast<std::size_t>(INT_MAX)));
        for (;;) {
            ERR_clear_error();
            const int rc = SSL_read(impl_->ssl, buffer, amount);
            const int reason = rc > 0 ? SSL_ERROR_NONE : SSL_get_error(impl_->ssl, rc);
            const unsigned long error = ERR_get_error();
            if (reason == SSL_ERROR_ZERO_RETURN) {
                impl_->peer_closed = true;
                operation.finished = true;
                co_return 0;
            }
            if (reason != SSL_ERROR_NONE && reason != SSL_ERROR_WANT_READ && reason != SSL_ERROR_WANT_WRITE) {
                set_error("TLS read failed", error);
                co_return -1;
            }
            if (!co_await flush_to_socket())
                co_return -1;
            if (rc > 0) {
                operation.finished = true;
                co_return rc;
            }
            if (reason == SSL_ERROR_WANT_READ && co_await pull_from_socket() <= 0) {
                set_error("peer closed without close_notify", 0);
                co_return -1;
            }
        }
    }

    Task<int> TlsStream::write(const char* buffer, std::size_t size) {
        if (!impl_ || !impl_->handshake_done || impl_->broken || impl_->closing || impl_->peer_closed) {
            set_error("write on an unusable TLS stream", 0);
            co_return -1;
        }
        impl::operation operation(*impl_);
        if (size == 0) {
            operation.finished = true;
            co_return 0;
        }
        if (!buffer) {
            set_error("null TLS write buffer", 0);
            co_return -1;
        }
        const int amount = static_cast<int>(std::min(size, static_cast<std::size_t>(INT_MAX)));
        for (;;) {
            ERR_clear_error();
            const int rc = SSL_write(impl_->ssl, buffer, amount);
            const int reason = rc > 0 ? SSL_ERROR_NONE : SSL_get_error(impl_->ssl, rc);
            const unsigned long error = ERR_get_error();
            if (reason != SSL_ERROR_NONE && reason != SSL_ERROR_WANT_READ && reason != SSL_ERROR_WANT_WRITE) {
                set_error("TLS write failed", error);
                co_return -1;
            }
            if (!co_await flush_to_socket())
                co_return -1;
            if (rc > 0) {
                operation.finished = true;
                co_return rc;
            }
            if (reason == SSL_ERROR_WANT_READ && co_await pull_from_socket() <= 0) {
                set_error("peer closed during TLS write", 0);
                co_return -1;
            }
        }
    }

    Task<bool> TlsStream::shutdown() {
        if (!impl_ || impl_->broken || !impl_->handshake_done) {
            set_error("shutdown on an unusable TLS stream", 0);
            co_return false;
        }
        impl::operation operation(*impl_);
        impl_->closing = true;
        if (impl_->closed) {
            operation.finished = true;
            co_return true;
        }
        for (;;) {
            ERR_clear_error();
            const int rc = SSL_shutdown(impl_->ssl);
            const int reason = rc < 0 ? SSL_get_error(impl_->ssl, rc) : SSL_ERROR_NONE;
            const unsigned long error = ERR_get_error();
            if (reason != SSL_ERROR_NONE && reason != SSL_ERROR_WANT_READ && reason != SSL_ERROR_WANT_WRITE) {
                set_error("TLS shutdown failed", error);
                co_return false;
            }
            if (!co_await flush_to_socket())
                co_return false;
            if (rc == 1) {
                impl_->closed = true;
                impl_->peer_closed = true;
                operation.finished = true;
                co_return true;
            }
            // 第一次 rc==0 只表示本端已发送; 先再次驱动 SSL 处理 BIO 中已有记录,
            // 只有 WANT_READ 才向 socket 要更多字节, 避免已有 close_notify 时死等。
            if (reason == SSL_ERROR_WANT_READ && co_await pull_from_socket() <= 0) {
                set_error("peer closed without completing close_notify", 0);
                co_return false;
            }
        }
    }

    bool TlsStream::peer_verified() const {
        if (!impl_ || !impl_->handshake_done || impl_->broken || impl_->role != TlsContext::role::client)
            return false;
        return SSL_get0_peer_certificate(impl_->ssl) != nullptr && SSL_get_verify_result(impl_->ssl) == X509_V_OK;
    }

    std::string TlsStream::negotiated_version() const {
        if (!impl_ || !impl_->handshake_done)
            return {};
        const char* version = SSL_get_version(impl_->ssl);
        return version ? version : "";
    }

    std::string TlsStream::negotiated_alpn() const {
        if (!impl_ || !impl_->handshake_done)
            return {};
        const unsigned char* protocol = nullptr;
        unsigned int size = 0;
        SSL_get0_alpn_selected(impl_->ssl, &protocol, &size);
        return protocol && size ? std::string(reinterpret_cast<const char*>(protocol), size) : std::string{};
    }

} // namespace coro::tls
