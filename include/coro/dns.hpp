#pragma once

#include "exceptions.hpp"
#include "net.hpp"
#include "task.hpp"
#include "thread.hpp"

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

// 系统头按平台分流: coro_header_check 会把每个公共头单独编成一个 TU,
// 在 Windows 上写 <netdb.h> 会直接把那个作业编炸 (Windows 走 ws2tcpip.h)。
#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#endif

#if defined(_WIN32) || (defined(__linux__) && defined(CORO_HAS_URING) && CORO_HAS_URING)
#define CORO_HAS_DNS 1
#else
#define CORO_HAS_DNS 0
#endif

// ============================================================================
// coro::net — 异步 DNS 解析与 endpoint 适配 (计划 M4b)
// ============================================================================
//
// 为什么这样实现: 本仓库的约束是"不引入 c-ares、不加新的通用运行时"。而 getaddrinfo
// 是同步阻塞调用 (可能查 DNS、读 /etc/hosts、走 nsswitch), 直接在事件循环线程上调用
// 会把整个 loop 卡住 —— 一个慢 DNS 服务器就能让所有连接停摆。
// 所以把它交给**已有的** to_thread 线程池卸载: 不新增依赖, 也不新增第二套调度器。
//
// 用法:
//   auto eps = co_await coro::net::resolve("example.com", "80");
//   if (eps.empty()) ...                      // 无结果 (不等于错误)
//   auto sock = co_await coro::net::connect(eps[0]);
//
// 范围 (v1, 有意划小并写明):
//   - 只返回 **IPv4** 端点。net.hpp 的 connect 目前只构造 sockaddr_in, 返回 v6 地址
//     等于给调用方一个连不上的东西; 等 net.hpp 支持 sockaddr_in6 再放开。
//   - 不做解析结果缓存, 也不做 happy-eyeballs 排序: 那属于连接池/HTTP Client 层的策略。
//   - 不做反向解析 (getnameinfo)。
// ============================================================================

namespace coro {
    namespace detail {
        /// DNS 解析失败: 携带 getaddrinfo 的返回码, 调用方要能区分"查不到"与"网络故障"。
        class DnsResolutionError : public std::runtime_error {
          public:
            DnsResolutionError(const std::string& host, int code, const char* detail)
                : std::runtime_error("resolve \"" + host + "\" failed: " + detail), code(code) {}
            int code; ///< getaddrinfo 返回码 (EAI_NONAME/EAI_AGAIN/...)
        };
    } // namespace detail

    namespace net {
#if CORO_HAS_DNS
        /// 一个可用于连接的解析结果
        struct resolved_endpoint {
            std::string address;        ///< 点分十进制 IPv4
            uint16_t port = 0;          ///< 主机字节序
            std::string canonical_name; ///< 服务给的规范名 (可能为空)
        };

        /// 解析主机名/服务名为可连接的 IPv4 端点列表。不阻塞事件循环。
        /// 失败抛 detail::DnsResolutionError; 查不到地址返回空 vector (不算错误)。
        inline Task<std::vector<resolved_endpoint>> resolve(std::string host, std::string service = "") {
            co_return co_await to_thread([host, service]() {
                // 结果按值带回循环线程: sockaddr 是 POD, 但 vector 跨线程搬运才安全
                std::vector<resolved_endpoint> out;
                addrinfo hints{};
                hints.ai_family = AF_INET; // v1 只取 v4 (见文件头范围说明)
                hints.ai_socktype = SOCK_STREAM;
                addrinfo* raw = nullptr;
                const int rc = ::getaddrinfo(host.c_str(), service.empty() ? nullptr : service.c_str(), &hints, &raw);
                if (rc != 0) {
                    // 在 worker 线程里构造异常对象没问题: to_thread 会把它作为结果传回并
                    // 由 await_resume 在循环线程重新抛出。
                    throw detail::DnsResolutionError(host, rc, gai_strerror(rc));
                }
                char peer[INET_ADDRSTRLEN] = {0};
                for (addrinfo* it = raw; it != nullptr; it = it->ai_next) {
                    if (it->ai_family != AF_INET || it->ai_addrlen < sizeof(sockaddr_in))
                        continue;
                    sockaddr_in sa{};
                    std::memcpy(&sa, it->ai_addr, sizeof(sa));
                    resolved_endpoint ep;
                    if (::inet_ntop(AF_INET, &sa.sin_addr, peer, sizeof(peer)) != nullptr)
                        ep.address = peer;
                    ep.port = static_cast<uint16_t>(ntohs(sa.sin_port));
                    if (it->ai_canonname != nullptr)
                        ep.canonical_name = it->ai_canonname;
                    out.push_back(std::move(ep));
                }
                ::freeaddrinfo(raw); // 必须在同一线程释放, 且不能漏
                return out;
            });
        }

        /// 按解析结果建立连接: 把 endpoint 适配回 net.hpp 的 (ip, port) 接口, 调用方不必
        /// 自己拆字段。ip 为空视为无效结果。
        inline Task<TcpStream> connect(const resolved_endpoint& ep) {
            co_return co_await TcpStream::connect(ep.address.c_str(), ep.port);
        }
#endif // CORO_HAS_DNS
    } // namespace net
} // namespace coro
