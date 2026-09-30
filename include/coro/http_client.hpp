#pragma once

#include "task.hpp"
#include "http.hpp"

#include <chrono>
#include <cstddef>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#if defined(CORO_HAS_HTTP_CLIENT) && CORO_HAS_HTTP_CLIENT
namespace coro {

    struct HttpRequest {
        std::string method = "GET";
        std::string url;
        HttpHeaders headers;
        std::string body;
        bool keep_alive = true;
    };

    /// HTTP/1.x client, local to one event loop. HTTP and optional HTTPS share
    /// protocol handling; pools are isolated by scheme, host, port and client TLS configuration.
    /// No automatic retries, redirects, proxying, decompression or protocol upgrades.
    class HttpClient {
      public:
        struct Options {
            std::chrono::milliseconds request_timeout{30000}; // Includes queueing/DNS/connect/body; 0 disables.
            std::chrono::milliseconds shutdown_timeout{1000};
            std::chrono::milliseconds idle_timeout{30000};
            std::size_t max_connections_per_origin = 8;
            std::size_t max_idle_per_origin = 4; // 0 disables connection caching.
            std::size_t max_origins = 64;
            std::size_t max_body_bytes = 8 * 1024 * 1024;
            std::size_t max_header_bytes = 64 * 1024;
            std::size_t max_header_count = 100;
            std::size_t max_response_wire_bytes = 16 * 1024 * 1024;
            std::string ca_bundle; // Empty: OpenSSL default trust paths. Certificate/hostname checks stay enabled.
        };

        HttpClient();
        explicit HttpClient(Options options);
        ~HttpClient();
        HttpClient(HttpClient&&) noexcept;
        HttpClient& operator=(HttpClient&&) noexcept;
        HttpClient(const HttpClient&) = delete;
        HttpClient& operator=(const HttpClient&) = delete;

        /// Owns the request before returning a lazy Task. Host, Content-Length,
        /// Transfer-Encoding, Connection and upgrade/expectation headers are managed by the client.
        Task<HttpResponse> request(HttpRequest request);
        Task<HttpResponse> get(std::string url);

        /// Reject new work, wake pool waiters and discard idle connections.
        /// In-flight requests retain their resources and finish under their own deadline.
        void close();
        bool closed() const noexcept;
        static bool supports_tls() noexcept;

      private:
        struct impl;
        static Task<HttpResponse> perform(std::shared_ptr<impl> state, HttpRequest request);
        std::shared_ptr<impl> impl_;
    };

} // namespace coro
#endif
