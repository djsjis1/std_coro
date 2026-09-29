// tls.ipp — coro::tls 的实现体 (由 tls.hpp 在 CORO_HAS_TLS 为真时 include)
//
// 这个文件是唯一 include OpenSSL 头的位置。tls.hpp 的公共 API 里没有任何 OpenSSL 类型,
// 因此消费者不需要 OpenSSL 的头搜索路径, 也不会看到 SSL*/BIO*/X509* 的布局。
//
// 隧道模型 (memory BIO):
//   OpenSSL  <->  rbio (内存, 我们往里喂 socket 收到的字节)
//   OpenSSL  ->   wbio (内存, 我们把里面攒的字节写进 socket)
// 于是 SSL_do_handshake / SSL_read / SSL_write 永远不直接阻塞: 它们要么 WANT_READ
// (需要更多字节) 要么 WANT_WRITE (先把 wbio 排空)。两种情况都 co_await 已有 socket
// awaiter, 因此握手/读写天然可取消、可被超时打断, 且不会占住事件循环线程。
#pragma once

#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>
#include <openssl/x509_vfy.h>

#include <cstring>

namespace coro {
    namespace tls {

        inline std::string TlsError::describe(unsigned long e) {
            if (e == 0)
                return "no openssl error recorded";
            char buf[256];
            ::ERR_error_string_n(e, buf, sizeof(buf));
            return std::string(buf);
        }

        // ---------------------------------------------------------------- context
        struct TlsContext::impl {
            SSL_CTX* ctx = nullptr;
            ~impl() {
                if (ctx)
                    ::SSL_CTX_free(ctx);
            }
        };

        inline TlsContext::TlsContext(role r) : role_(r), impl_(std::make_unique<impl>()) {
            const SSL_METHOD* m = (r == role::client) ? ::TLS_client_method() : ::TLS_server_method();
            impl_->ctx = ::SSL_CTX_new(m);
            if (!impl_->ctx)
                throw TlsError("SSL_CTX_new failed", ::ERR_get_error());

            // 默认最低 TLS 1.2: SSLv3/TLS1.0/1.1 一律不接受。
            ::SSL_CTX_set_min_proto_version(impl_->ctx, TLS1_2_VERSION);
            // 允许同一 CTX 派生多个会话, 且 socket 写可以分段 (底层可能短写)。
            ::SSL_CTX_set_mode(impl_->ctx, SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);
            if (r == role::client) {
                // 客户端强制验证证书链; 没有信任锚时握手会失败, 而不是静默通过。
                ::SSL_CTX_set_verify(impl_->ctx, SSL_VERIFY_PEER, nullptr);
                ::SSL_CTX_set_verify_depth(impl_->ctx, verify_depth_);
            }
        }

        inline TlsContext::~TlsContext() = default;
        inline TlsContext::TlsContext(TlsContext&&) noexcept = default;
        inline TlsContext& TlsContext::operator=(TlsContext&&) noexcept = default;

        inline bool TlsContext::load_system_trust() {
            // SSL_CTX_set_default_verify_paths 会读 OpenSSL 编译期路径与 SSL_CERT_FILE/
            // SSL_CERT_DIR。仓库内构建的 OpenSSL 若没配系统路径, 这里返回 0 是**正确**
            // 的失败信号 —— 调用方必须因此拒绝连接, 而不是改用"不验证"。
            return ::SSL_CTX_set_default_verify_paths(impl_->ctx) == 1;
        }

        inline bool TlsContext::load_verify_file(const std::string& path) {
            return ::SSL_CTX_load_verify_locations(impl_->ctx, path.c_str(), nullptr) == 1;
        }

        inline bool TlsContext::use_certificate_file(const std::string& path) {
            return ::SSL_CTX_use_certificate_chain_file(impl_->ctx, path.c_str()) == 1;
        }

        inline bool TlsContext::use_private_key_file(const std::string& path) {
            return ::SSL_CTX_use_PrivateKey_file(impl_->ctx, path.c_str(), SSL_FILETYPE_PEM) == 1 &&
                   ::SSL_CTX_check_private_key(impl_->ctx) == 1;
        }

        inline bool TlsContext::set_min_version(protocol_version v) {
            const int ver = (v == protocol_version::tls13) ? TLS1_3_VERSION : TLS1_2_VERSION;
            return ::SSL_CTX_set_min_proto_version(impl_->ctx, ver) == 1;
        }

        // ---------------------------------------------------------------- stream
        struct TlsStream::impl {
            net::TcpStream sock;
            const TlsContext* ctx = nullptr;
            SSL* ssl = nullptr;
            BIO* rbio = nullptr;
            BIO* wbio = nullptr;
            std::string hostname;
            bool handshake_done = false;
            bool broken = false; // 任何协议错误/未完成交换后置真: 不得回池

            impl() = default;
            ~impl() {
                if (ssl)
                    ::SSL_free(ssl); // 接管了 rbio/wbio (SSL_set_bio 的所有权约定)
                else {
                    if (rbio)
                        ::BIO_free(rbio);
                    if (wbio)
                        ::BIO_free(wbio);
                }
            }

            impl(impl&&) = delete; // 由 unique_ptr 持有, 不需要自身可移动
        };

        inline TlsStream::TlsStream(net::TcpStream socket, const TlsContext& ctx, std::string hostname)
            : impl_(std::make_unique<impl>()) {
            impl_->sock = std::move(socket);
            impl_->ctx = &ctx;
            impl_->hostname = std::move(hostname);

            impl_->rbio = ::BIO_new(BIO_s_mem());
            impl_->wbio = ::BIO_new(BIO_s_mem());
            if (!impl_->rbio || !impl_->wbio) {
                set_error("BIO_new", ::ERR_get_error());
                return;
            }
            impl_->ssl = ::SSL_new(ctx.raw_impl().ctx);
            if (!impl_->ssl) {
                set_error("SSL_new", ::ERR_get_error());
                return;
            }
            // 先把两个内存 BIO 挂上再设置角色: 之后每次 WANT_* 都由我们搬运字节。
            ::SSL_set_bio(impl_->ssl, impl_->rbio, impl_->wbio);

            if (ctx.get_role() == TlsContext::role::client) {
                ::SSL_set_connect_state(impl_->ssl);
                if (ctx.hostname_verification() && !impl_->hostname.empty()) {
                    // 主机名校验走 X509_check_host 的 RFC6125 规则 (含通配符)
                    ::SSL_set1_host(impl_->ssl, impl_->hostname.c_str());
                }
                if (!ctx.hostname_verification())
                    ::SSL_set_hostflags(impl_->ssl, 0);
                const auto& alpn = ctx.get_alpn();
                if (!alpn.empty()) {
                    // wire format: 每协议一个 1 字节长度前缀 + 名字, 串起来
                    std::string wire;
                    for (const auto& p : alpn) {
                        if (p.empty() || p.size() > 255)
                            continue;
                        wire.push_back(static_cast<char>(p.size()));
                        wire += p;
                    }
                    ::SSL_set_alpn_protos(impl_->ssl, reinterpret_cast<const unsigned char*>(wire.data()),
                                          static_cast<unsigned>(wire.size()));
                }
            } else {
                ::SSL_set_accept_state(impl_->ssl);
            }
        }

        inline TlsStream::~TlsStream() = default;
        inline TlsStream::TlsStream(TlsStream&&) noexcept = default;
        inline TlsStream& TlsStream::operator=(TlsStream&&) noexcept = default;

        inline void TlsStream::set_error(const char* where, unsigned long ssl_err) {
            impl_->broken = true;
            std::string msg = where;
            if (ssl_err)
                msg += ": " + TlsError::describe(ssl_err);
            else {
                unsigned long e = ::ERR_get_error();
                msg += e ? (": " + TlsError::describe(e)) : ": 无 openssl 错误记录 (对端可能直接关闭连接)";
            }
            last_error_ = msg;
        }

        inline bool TlsStream::socket_valid() const noexcept {
            return impl_->sock.valid();
        }

        /// socket -> rbio。返回本次拿到的字节数; 0 = 对端关闭; -1 = 错误。
        inline Task<int> TlsStream::pull_from_socket() {
            char tmp[16 * 1024];
            const int n = co_await impl_->sock.read(tmp, sizeof(tmp));
            if (n < 0) {
                set_error("socket read", 0);
                co_return -1;
            }
            if (n == 0)
                co_return 0; // EOF: 由调用方判断是否缺 close_notify
            if (::BIO_write(impl_->rbio, tmp, n) <= 0) {
                set_error("BIO_write", ::ERR_get_error());
                co_return -1;
            }
            co_return n;
        }

        /// wbio -> socket, 排空为止 (处理短写)。
        inline Task<bool> TlsStream::flush_to_socket() {
            char tmp[16 * 1024];
            for (;;) {
                int pending = BIO_pending(impl_->wbio);
                if (pending <= 0)
                    co_return true;
                const int want = pending < static_cast<int>(sizeof(tmp)) ? pending : static_cast<int>(sizeof(tmp));
                int got = ::BIO_read(impl_->wbio, tmp, want);
                if (got <= 0) {
                    set_error("BIO_read", ::ERR_get_error());
                    co_return false;
                }
                int off = 0;
                while (off < got) {
                    const int w = co_await impl_->sock.write(tmp + off, static_cast<std::size_t>(got - off));
                    if (w <= 0) {
                        set_error("socket write", 0);
                        co_return false;
                    }
                    off += w;
                }
            }
        }

        inline Task<bool> TlsStream::handshake() {
            if (!impl_->ssl) {
                set_error("handshake: SSL 未建立", 0);
                co_return false;
            }
            // 循环上限是防御性的: 正常握手几轮就结束, 转这么多圈说明对端在喂无意义数据。
            for (int round = 0; round < 64; ++round) {
                const int rc = ::SSL_do_handshake(impl_->ssl);
                if (rc == 1) {
                    if (!co_await flush_to_socket())
                        co_return false;
                    // 客户端还要确认证书验证结果通过 (SSL_get_verify_result 是会话级)
                    if (impl_->ctx->hostname_verification() && ::SSL_get_verify_result(impl_->ssl) != X509_V_OK) {
                        set_error("证书验证失败", 0);
                        co_return false;
                    }
                    impl_->handshake_done = true;
                    last_error_.clear();
                    co_return true;
                }
                const unsigned long e = ::SSL_get_error(impl_->ssl, rc);
                if (e != SSL_ERROR_WANT_READ && e != SSL_ERROR_WANT_WRITE) {
                    set_error("handshake", e);
                    co_return false;
                }
                // 先把对端要的字节发出去 (ClientHello / 证书应答), 再补读
                if (!co_await flush_to_socket())
                    co_return false;
                if (e == SSL_ERROR_WANT_READ) {
                    const int n = co_await pull_from_socket();
                    if (n <= 0) {
                        set_error("握手期间对端关闭", 0);
                        co_return false;
                    }
                }
            }
            set_error("握手轮次超限 (对端行为异常)", 0);
            co_return false;
        }

        inline Task<int> TlsStream::read(char* buf, std::size_t len) {
            if (!impl_->handshake_done) {
                set_error("read before handshake", 0);
                co_return -1;
            }
            for (int round = 0; round < 256; ++round) {
                const int rc = ::SSL_read(impl_->ssl, buf, static_cast<int>(len));
                if (rc > 0)
                    co_return rc;
                const unsigned long e = ::SSL_get_error(impl_->ssl, rc);
                if (e == SSL_ERROR_ZERO_RETURN) {
                    // 收到对端 close_notify: 这是干净 EOF, 不算 broken
                    impl_->handshake_done = false;
                    co_return 0;
                }
                if (e == SSL_ERROR_WANT_READ) {
                    if (!co_await flush_to_socket()) // 可能同时有要发的记录
                        co_return -1;
                    const int n = co_await pull_from_socket();
                    if (n < 0)
                        co_return -1;
                    if (n == 0) {
                        set_error("对端未发 close_notify 就关闭 (截断攻击风险, 按错误处理)", 0);
                        co_return -1;
                    }
                    continue;
                }
                if (e == SSL_ERROR_WANT_WRITE) {
                    if (!co_await flush_to_socket())
                        co_return -1;
                    continue;
                }
                set_error("SSL_read", e);
                co_return -1;
            }
            set_error("SSL_read 轮次超限", 0);
            co_return -1;
        }

        inline Task<int> TlsStream::write(const char* buf, std::size_t len) {
            if (!impl_->handshake_done) {
                set_error("write before handshake", 0);
                co_return -1;
            }
            const int rc = ::SSL_write(impl_->ssl, buf, static_cast<int>(len));
            if (rc <= 0) {
                const unsigned long e = ::SSL_get_error(impl_->ssl, rc);
                if (e == SSL_ERROR_WANT_WRITE) {
                    if (!co_await flush_to_socket())
                        co_return -1;
                    // 排空后重试一次: 明文记录还在 OpenSSL 内部
                    const int again = ::SSL_write(impl_->ssl, buf, static_cast<int>(len));
                    if (again <= 0) {
                        set_error("SSL_write 重试", ::SSL_get_error(impl_->ssl, again));
                        co_return -1;
                    }
                    if (!co_await flush_to_socket())
                        co_return -1;
                    co_return again;
                }
                if (e == SSL_ERROR_WANT_READ) {
                    const int n = co_await pull_from_socket();
                    if (n <= 0) {
                        set_error("写时需要对端数据但连接已结束", 0);
                        co_return -1;
                    }
                    const int again = ::SSL_write(impl_->ssl, buf, static_cast<int>(len));
                    if (again <= 0) {
                        set_error("SSL_write 重试", ::SSL_get_error(impl_->ssl, again));
                        co_return -1;
                    }
                    if (!co_await flush_to_socket())
                        co_return -1;
                    co_return again;
                }
                set_error("SSL_write", e);
                co_return -1;
            }
            if (!co_await flush_to_socket())
                co_return -1;
            co_return rc;
        }

        inline Task<bool> TlsStream::shutdown() {
            if (!impl_->ssl)
                co_return false;
            if (impl_->broken) {
                last_error_ = "shutdown: 会话已因先前的错误作废";
                co_return false;
            }
            for (int round = 0; round < 4; ++round) {
                const int rc = ::SSL_shutdown(impl_->ssl);
                if (!co_await flush_to_socket()) {
                    last_error_ = "shutdown: 发出 close_notify 后写 socket 失败";
                    co_return false;
                }
                if (rc == 1)
                    co_return true; // 双向交换完成
                if (rc < 0) {
                    const unsigned long e = ::SSL_get_error(impl_->ssl, rc);
                    if (e == SSL_ERROR_ZERO_RETURN)
                        co_return true; // 对端告警已被这次调用消化
                    if (e != SSL_ERROR_WANT_READ) {
                        last_error_ = "shutdown: SSL_shutdown 返回错误 " + TlsError::describe(e);
                        co_return false;
                    }
                }
                // 关键顺序: SSL_get_shutdown 的 RECEIVED 位**只在 OpenSSL 处理该记录时**
                // 更新 (SSL_read / SSL_shutdown)。刚 BIO_write 进 rbio 的告警还没被处理,
                // 此刻查标志必然为空 —— 上一轮就是在这里把已经完成的交换误判成失败。
                if (::SSL_get_shutdown(impl_->ssl) & SSL_RECEIVED_SHUTDOWN)
                    co_return true;
                const int n = co_await pull_from_socket();
                if (n < 0) {
                    last_error_ = "shutdown: 等待对端 close_notify 时读失败";
                    co_return false;
                }
                if (n == 0) {
                    // 对端已关闭连接: 若告警其实已在缓冲区里, 让它被处理一次再判定
                    ::SSL_shutdown(impl_->ssl);
                    if (::SSL_get_shutdown(impl_->ssl) & SSL_RECEIVED_SHUTDOWN)
                        co_return true;
                    impl_->broken = true;
                    last_error_ = "shutdown: 对端未发 close_notify 就关闭 (截断风险, 连接不可复用)";
                    co_return false;
                }
            }
            impl_->broken = true;
            last_error_ = "shutdown: 交换轮次超限, 未收到对端 close_notify";
            co_return false;
        }

        inline bool TlsStream::peer_verified() const {
            if (!impl_->ssl)
                return false;
            if (impl_->ctx->get_role() != TlsContext::role::client)
                return true; // 服务端默认不要求客户端证书 (v1)
            return ::SSL_get_verify_result(impl_->ssl) == X509_V_OK;
        }

        inline std::string TlsStream::negotiated_version() const {
            if (!impl_->ssl)
                return std::string();
            const char* v = ::SSL_get_version(impl_->ssl);
            return v ? std::string(v) : std::string();
        }

        inline std::string TlsStream::negotiated_alpn() const {
            if (!impl_->ssl)
                return std::string();
            const unsigned char* out = nullptr;
            unsigned int len = 0;
            ::SSL_get0_alpn_selected(impl_->ssl, &out, &len);
            if (!out || len == 0)
                return std::string();
            return std::string(reinterpret_cast<const char*>(out), len);
        }

    } // namespace tls
} // namespace coro
