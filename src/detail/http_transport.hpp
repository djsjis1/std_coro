#pragma once

#include <coro/net.hpp>
#include <coro/tls.hpp>
#include <coro/wait.hpp>

#include <chrono>
#include <optional>
#include <system_error>

namespace coro::detail {

struct HttpTransport {
    net::TcpStream socket;
#if defined(CORO_HAS_TLS) && CORO_HAS_TLS
    std::optional<tls::TlsStream> tls_stream;
#endif
    std::error_code error;

    std::error_code last_error_code() const noexcept { return error; }

    Task<int> read(char* data, std::size_t size) {
#if defined(CORO_HAS_TLS) && CORO_HAS_TLS
        if (tls_stream) {
            const int result = co_await tls_stream->read(data, size);
            if (result < 0)
                error = tls_stream->last_error_code();
            co_return result;
        }
#endif
        const int result = co_await socket.read(data, size);
        if (result < 0)
            error = std::error_code(io::last_error(), std::system_category());
        co_return result;
    }

    Task<int> write(const char* data, std::size_t size) {
#if defined(CORO_HAS_TLS) && CORO_HAS_TLS
        if (tls_stream) {
            const int result = co_await tls_stream->write(data, size);
            if (result < 0)
                error = tls_stream->last_error_code();
            co_return result;
        }
#endif
        const int result = co_await socket.write(data, size);
        if (result < 0)
            error = std::error_code(io::last_error(), std::system_category());
        co_return result;
    }

    bool reusable() const noexcept {
#if defined(CORO_HAS_TLS) && CORO_HAS_TLS
        if (tls_stream)
            return !error && tls_stream->reusable();
#endif
        return !error && socket.valid();
    }

    Task<> shutdown(std::chrono::milliseconds timeout) {
#if defined(CORO_HAS_TLS) && CORO_HAS_TLS
        if (tls_stream) {
            try {
                (void)co_await wait_for(tls_stream->shutdown(), timeout);
            } catch (const CancelledError&) {
                throw;
            } catch (const std::exception&) {
                // A complete HTTP response may still be returned. The lease is
                // discarded unconditionally, including on failed close_notify.
            }
        }
#else
        (void)timeout;
#endif
        co_return;
    }
};


} // namespace coro::detail
