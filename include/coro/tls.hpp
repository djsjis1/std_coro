#pragma once

#include "exceptions.hpp"
#include "net.hpp"
#include "task.hpp"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

// TLS 能力由根 CMake 单点派生 (CORO_ENABLE_TLS + 仓库内 OpenSSL 源码存在)。
// 关闭时本头编译为空: 消费者不需要 OpenSSL 头文件, 也不会被要求链接 libssl。
#if defined(CORO_HAS_TLS) && CORO_HAS_TLS

#include "stream.hpp"

// ============================================================================
// coro::tls — TLS 会话 (计划 M5, OpenSSL 底座)
// ============================================================================
//
// 设计要点:
//   1. **公共 API 不出现任何 OpenSSL 类型** (SSL_CTX*/SSL*/BIO*/X509*)。所有句柄藏在
//      PIMPL 里, 因此消费者只 include 本头即可, 不需要 OpenSSL 的头搜索路径。实现体在
//      同目录的 `tls.ipp`: header-only 只能把 include 边界推到"私有实现文件", 这是唯一
//      诚实的做法 —— 既保住接口边界, 又不引入第二套编译单元。
//   2. **不做阻塞式 SSL_connect/SSL_accept**, 也不用 BIO_s_connect。原因是本库的 socket
//      只认协程 awaiter: 用阻塞 BIO 会让事件循环线程卡在系统调用上 (一个慢握手拖死整个
//      loop)。这里用 memory BIO 做隧道: OpenSSL 想要字节就从 socket 读, 要发就写 socket,
//      每一步都 co_await, 因此握手天然可取消、可超时。
//   3. **AsyncReadable / AsyncWritable 兼容** stream.hpp 的概念: TlsStream 提供
//      `co_await read(buf, n)` 与 `co_await write(buf, n)`, 返回约定与 TcpStream 一致
//      (>=0 字节数, 0 = 干净 EOF, -1 = 错误且 io::last_error() 已设)。于是
//      stream_reader/stream_writer 直接能在 TLS 上跑, 不必再造一层协议缓冲。
//   4. **安全默认**: 校验证书链 + 主机名; 信任库缺失时 `load_system_trust()` 返回失败
//      而不是"跳过校验"。要放宽必须显式调用 insecure_* 入口。
//
// 用法 (客户端):
//   coro::tls::TlsContext ctx(coro::tls::TlsContext::role::client);
//   if (!ctx.load_system_trust()) throw ...;         // 信任来源缺失必须失败
//   auto sock = co_await coro::net::TcpStream::connect(ip, 443);
//   coro::tls::TlsStream tls(std::move(sock), ctx, "example.com");
//   if (!co_await tls.handshake()) throw ...;
//   coro::stream_writer w(tls);
//   co_await w.write_line("GET / HTTP/1.0\r\n");
// ============================================================================

namespace coro {
    namespace tls {

        /// TLS 层错误: 携带 OpenSSL 错误码, 便于区分"对端拒绝""证书问题""协议错误"。
        class TlsError : public std::runtime_error {
          public:
            TlsError(const std::string& what, unsigned long openssl_error)
                : std::runtime_error(what + " (openssl error " + std::to_string(openssl_error) + ')'),
                  code(describe(openssl_error)), raw(openssl_error) {}
            std::string code;  ///< ERR_error_string 的可读形式
            unsigned long raw; ///< 原始 ERR_get_error 值

            /// 把 ERR_get_error 的值转成人话。public: TlsStream 记录 last_error 时也要用。
            static std::string describe(unsigned long e); // 定义在 tls.ipp
        };

        /// SSL_CTX 的持有者。可被多个会话共享 (会话票据、信任库都挂在它上面)。
        class TlsContext {
          public:
            enum class role { client, server };

            explicit TlsContext(role r);
            ~TlsContext();

            TlsContext(TlsContext&&) noexcept;
            TlsContext& operator=(TlsContext&&) noexcept;
            TlsContext(const TlsContext&) = delete;
            TlsContext& operator=(const TlsContext&) = delete;

            /// 加载系统信任库作为验证锚点。失败返回 false (不静默降级成"不验证")。
            bool load_system_trust();
            /// 用指定 CA bundle 文件作为验证锚点 (PEM)。
            bool load_verify_file(const std::string& path);
            /// 服务端: 加载证书链与私钥 (PEM)。两者都成功才算成功。
            bool use_certificate_file(const std::string& path);
            bool use_private_key_file(const std::string& path);

            /// 最低协议版本, 默认 TLS 1.2 (13 与 12)。传 0x0304 这类 SSL 版本常量时
            /// 由实现层转换; 这里用枚举避免把 OpenSSL 宏暴露在公共头。
            enum class protocol_version { tls12, tls13 };
            bool set_min_version(protocol_version v);

            /// 主机名校验开关 (默认开)。关掉它等于放弃身份验证, 只该出现在测试里。
            void set_hostname_verification(bool enable) { hostname_verification_ = enable; }
            bool hostname_verification() const noexcept { return hostname_verification_; }

            /// ALPN 协议列表 (按优先级)。空 = 不协商。
            void set_alpn(std::vector<std::string> protocols) { alpn_ = std::move(protocols); }

            /// 验证深度 (默认 9, 与 OpenSSL 一致)。
            void set_verify_depth(int depth) { verify_depth_ = depth; }

            role get_role() const noexcept { return role_; }
            int get_verify_depth() const noexcept { return verify_depth_; }
            const std::vector<std::string>& get_alpn() const noexcept { return alpn_; }

            struct impl;
            /// 只给 TlsStream 的实现体用 (detail/tls.ipp), 不在公共 API 里暴露 OpenSSL 类型;
            /// const 限定是必需的: 会话共享 context, 却只读它的 SSL_CTX。
            impl& raw_impl() const noexcept { return *impl_; }

          private:
            std::unique_ptr<impl> impl_;
            role role_;
            bool hostname_verification_ = true;
            int verify_depth_ = 9;
            std::vector<std::string> alpn_;
        };

        /// 一次 TLS 会话: 拥有一个 TcpStream + 借用 ctx。
        class TlsStream {
          public:
            TlsStream(net::TcpStream socket, const TlsContext& ctx, std::string hostname);
            ~TlsStream();

            TlsStream(TlsStream&&) noexcept;
            TlsStream& operator=(TlsStream&&) noexcept;
            TlsStream(const TlsStream&) = delete;
            TlsStream& operator=(const TlsStream&) = delete;

            /// 握手。返回 true = 完成且对端已通过验证; false = 失败 (原因见 last_error())。
            /// 可被取消 (CancelledError) 或由内部 deadline 结束; 失败后连接不可复用。
            Task<bool> handshake();

            /// ---- AsyncReadable: co_await tls.read(buf, n) ----
            /// 刻意写成协程成员而不是手写 awaiter: 概念只要求"可 co_await 且返回 int",
            /// 而协程成员自动继承框架的取消注入 (cancel_check_awaiter) 与异常传播, 不必
            /// 再自己维护挂起/摘链/取消兜底 —— 那些正是本项目反复出过错的地方。
            /// >=0 = 明文字节数; 0 = 收到 close_notify (干净 EOF); -1 = 错误
            Task<int> read(char* buf, std::size_t len);

            /// ---- AsyncWritable: co_await tls.write(buf, n) ----
            /// >=0 = 已交付给 TLS 层的字节数; -1 = 错误
            Task<int> write(const char* buf, std::size_t len);

            /// 双向 close_notify 交换。返回 true = 对端也回了 close_notify (连接可安全关闭)。
            /// 未完成交换的会话不得回连接池 —— 否则下一个使用者会读到上一段残留。
            Task<bool> shutdown();

            /// 对端证书是否已通过验证 (握手成功后才有意义)。
            bool peer_verified() const;
            /// 协商到的协议版本与 ALPN, 供日志/测试断言。
            std::string negotiated_version() const;
            std::string negotiated_alpn() const;

            /// 最近一次失败的描述 (空 = 无错误)。
            const std::string& last_error() const noexcept { return last_error_; }

            /// 底层 socket 是否仍有效。
            bool socket_valid() const noexcept;

            struct impl;
            impl& raw_impl() noexcept { return *impl_; }

          private:
            // 隧道原语 (定义在 tls.ipp, 由 awaiter 与 handshake 调用)
            Task<int> pull_from_socket(); // socket -> 网络侧 BIO; <0 表示错误/EOF
            Task<bool> flush_to_socket(); // 网络侧 BIO -> socket
            void set_error(const char* where, unsigned long ssl_err);

            std::unique_ptr<impl> impl_;
            std::string last_error_;
        };

    } // namespace tls
} // namespace coro

// 实现体: 只有开启 TLS 时才存在, 里面才 include OpenSSL 头。
#include "detail/tls.ipp"

#endif // CORO_HAS_TLS
