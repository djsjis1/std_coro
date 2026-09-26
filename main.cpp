#include <coro/coro.hpp>
#include <coro/sleep.hpp>
#include <tcp_udp/udp_server.hpp>
#include <map>
#include <cstring>
#include <chrono>
using coro::Task;
using namespace std::chrono_literals;

// ============================================================================
// UDP 分片重组: 按客户端地址维护会话缓冲区
// ============================================================================
//
// UDP 虽然保留消息边界, 但应用层协议可能把一条逻辑消息拆成多个数据报发送
// (例如超过 MTU 时, 或自定义分片协议)。
//
// 常见重组策略 (按协议选择):
//   1. 长度前缀: 前 N 字节 = 整条消息长度, 攒够长度后交付
//   2. 分隔符:   遇到特定字节序列 (如 '\n' 或 '\0') 视为一条消息结束
//   3. 序号+总数: 每个分片带 (seq, total), 收齐 total 个后按序拼装
//
// 下面以 "2 字节大端长度前缀 + 载荷" 为例演示策略 1。
// ============================================================================

// 会话超时时间: 超过此时间无数据到达, 视为"连接断开"
static constexpr auto SESSION_TIMEOUT = 30s;
// 清理协程的扫描间隔
static constexpr auto CLEANUP_INTERVAL = 10s;

// 客户端会话: 标识一个 (IP, port) 对应的连接状态
struct ClientSession {
    std::vector<char> buffer;                          // 累积的原始字节, 尚未凑够完整消息
    std::chrono::steady_clock::time_point last_active; // 最后一次收到数据的时间
};

// 用 (ip, port) 作为会话 key
struct SockAddrKey {
    uint32_t ip;
    uint16_t port;
    bool operator<(const SockAddrKey& o) const { return ip < o.ip || (ip == o.ip && port < o.port); }
};

static SockAddrKey make_key(const sockaddr_in& addr) {
    return {addr.sin_addr.s_addr, addr.sin_port};
}

// 从 buffer 头部解析 2 字节大端长度前缀, 返回消息总长 (含前缀本身)
// buffer 不足 2 字节时返回 0, 表示还需要更多数据
static size_t peek_message_length(const std::vector<char>& buf) {
    if (buf.size() < 2)
        return 0;
    auto hi = static_cast<uint8_t>(buf[0]);
    auto lo = static_cast<uint8_t>(buf[1]);
    return static_cast<size_t>((hi << 8) | lo) + 2; // +2 是前缀本身
}

Task<> main_task() {
    // 会话表: 每个客户端地址对应一个累积缓冲区
    std::map<SockAddrKey, ClientSession> sessions;

    coro::UdpServer server;

    // ------------------------------------------------------------------
    // 后台清理协程: 定期扫描 sessions, 踢掉超时的会话释放内存
    // ------------------------------------------------------------------
    auto cleanup = [&sessions]() -> Task<> {
        while (true) {
            co_await coro::sleep(CLEANUP_INTERVAL); // 每 10s 扫一次

            auto now = std::chrono::steady_clock::now();
            for (auto it = sessions.begin(); it != sessions.end();) {
                if (now - it->second.last_active > SESSION_TIMEOUT) {
                    // 超时: 可选地通知应用层 (如日志记录)
                    std::cout << "[cleanup] session expired, removing" << std::endl;
                    it = sessions.erase(it); // 释放该会话的 buffer 内存
                } else {
                    ++it;
                }
            }
        }
    };
    spawn(cleanup()).detach(); // 启动后台清理, detach 自持有

    // ------------------------------------------------------------------
    // handler: 每收到一个 UDP 数据报调用一次
    // ------------------------------------------------------------------
    server.set_handler([&sessions, &server](const char* data, size_t size, const sockaddr_in& addr) -> Task<> {
        auto key = make_key(addr);
        auto& session = sessions[key];

        // 更新活跃时间 (每次收到数据都刷新)
        session.last_active = std::chrono::steady_clock::now();

        // 1) 追加本次数据报到会话缓冲区
        session.buffer.insert(session.buffer.end(), data, data + size);

        // 2) 循环尝试从缓冲区中提取完整消息 (可能一次收到多个完整消息)
        while (true) {
            size_t msg_len = peek_message_length(session.buffer);
            if (msg_len == 0 || session.buffer.size() < msg_len)
                break; // 数据不够, 等下一个数据报

            // 3) 提取一条完整消息
            std::string payload(session.buffer.begin() + 2, session.buffer.begin() + msg_len);
            session.buffer.erase(session.buffer.begin(), session.buffer.begin() + msg_len);

            // 4) 处理这条完整消息
            std::cout << "[" << inet_ntoa(addr.sin_addr) << ":" << ntohs(addr.sin_port) << "] complete message ("
                      << payload.size() << " bytes): " << payload << std::endl;
        }

        co_return;
    });

    server.set_error_handler([](const std::string& error) { std::cout << "Error: " << error << std::endl; });

    server.run_forever("0.0.0.0", 8080);

    co_return;
}

int main() {
#ifdef _WIN32
    SetConsoleOutputCP(65001);
    SetConsoleCP(65001);
#endif
    coro::run(main_task());
    return 0;
}
