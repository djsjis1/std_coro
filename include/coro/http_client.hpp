#pragma once

#include "exceptions.hpp"
#include "task.hpp"
#include "http.hpp"

#include <chrono>
#include <cstddef>
#include <memory>
#include <optional>
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
    class HttpResponseStream {
      public:
        struct impl;

        HttpResponseStream() = default;
        ~HttpResponseStream();
        HttpResponseStream(HttpResponseStream&&) noexcept;
        HttpResponseStream& operator=(HttpResponseStream&&) noexcept;
        HttpResponseStream(const HttpResponseStream&) = delete;
        HttpResponseStream& operator=(const HttpResponseStream&) = delete;

        bool valid() const noexcept;
        int status() const noexcept;
        const HttpHeaders& headers() const;
        const HttpHeaders& trailers() const;
        bool keep_alive() const noexcept;

        /// 读下一块正文 (大小由内核/解析器决定)。nullopt = 正常结束。
        /// 流对象必须活到本次 read_chunk 完成; 未读完就销毁 = 丢弃该连接。
        Task<std::optional<std::string>> read_chunk();

        /// 丢弃剩余正文并关闭连接 (提前退出/取消路径)。可在读完后重复调用。
        Task<void> aclose();

      private:
        friend class HttpClient;
        explicit HttpResponseStream(std::shared_ptr<impl> state) : impl_(std::move(state)) {}
        std::shared_ptr<impl> impl_;
    };

    class HttpClient {
      public:
        struct Options {
            std::chrono::milliseconds request_timeout{30000}; // Includes queueing/DNS/connect/body; 0 disables.
            std::chrono::milliseconds shutdown_timeout{1000};
            std::chrono::milliseconds idle_timeout{30000};
            std::size_t max_connections_per_origin = 8;
            std::size_t max_idle_per_origin = 4; // 0 disables connection caching.
            std::size_t max_origins = 64;
            /// 同时在途请求总数上限 (0 = 不限)。超过即报 OverloadError,
            /// 不静默无限排队 —— 连接数有限并不等于等待队列内存有限。
            std::size_t max_total_requests = 256;
            std::size_t max_body_bytes = 8 * 1024 * 1024;
            /// 发送侧 wire (请求行 + 头 + 体) 的独立上限, 避免响应有限而发送无界。
            std::size_t max_request_wire_bytes = 16 * 1024 * 1024;
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

        /// 流式请求: 头部返回后即可逐块读正文 (适合大响应/SSE/日志流)。
        /// `max_body_bytes` 仍作为累计上限, 超出立刻报错。
        /// `request_timeout` 覆盖建连到响应头就绪的阶段 (超时抛 TimeoutError);
        /// 正文读取由调用方控制节奏, 不受该超时约束。
        Task<HttpResponseStream> stream(HttpRequest request);

        /// Reject new work, wake pool waiters and discard idle connections.
        /// In-flight requests retain their resources and finish under their own deadline.
        /// 可从任意线程调用: 非本 loop 线程时只置关闭标记并投递清理, 不抛异常
        /// (析构函数也是这条路径, 不能因 check_loop 失败而 terminate)。
        void close();
        bool closed() const noexcept;

        /// 关闭并等待所有在途请求退出 (可 co_await 的收尾屏障)。
        /// 必须在所属 EventLoop 线程调用。close() 只发起, aclose() 才表示完成。
        Task<void> aclose();

        static bool supports_tls() noexcept;

        /// 实现细节类型 (对外只暴露名称, 定义在 .cpp): 流式响应需要持有它
        struct impl;

      private:
        static Task<HttpResponse> perform(std::shared_ptr<impl> state, HttpRequest request);
        /// stream() 的建连 + 写请求 + 等响应头阶段 (独立协程便于整体限时)
        static Task<HttpResponseStream> stream_open(std::shared_ptr<impl> state, HttpRequest request);
        std::shared_ptr<impl> impl_;
    };

} // namespace coro
#endif
