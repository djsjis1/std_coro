#pragma once

// net_common.hpp — coro::net 的平台无关公共件
// 给谁用: net.hpp / net_iocp.hpp / net_uring.hpp 三个头共用, 只放"两个平台实现
// 都要用且无平台语义差异"的最小件; 具体 socket / IOCP / io_uring 代码留在各平台头。

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#elif defined(__linux__)
#include <arpa/inet.h>
#include <sys/socket.h>
#endif

namespace coro {
    namespace net {
        namespace detail_net {
            /// 点分十进制 IPv4 → in_addr。失败返回 false。
            /// 不用 inet_addr: 它把"非法输入"与 255.255.255.255 混为同一个
            /// INADDR_NONE 返回值, 且已被 Winsock 标为弃用 (MSVC /W4 报 C4996,
            /// 开 CORO_WERROR 就直接编不过)。
            inline bool parse_ipv4(const char* ip, in_addr& out) noexcept {
                if (ip == nullptr)
                    return false;
                return ::inet_pton(AF_INET, ip, &out) == 1; // Windows/Linux 同名同签名
            }
        } // namespace detail_net
    } // namespace net
} // namespace coro
