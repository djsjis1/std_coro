// ============================================================================
// main.cpp — coro 协程服务端: start/end 流式协议解析
// ============================================================================
//
// 协议格式:
//   消息 = "start" + 载荷 + "end"
//
//   例: startpingend
//       startecho:helloend
//       start{"cmd":"login"}end
//
// TCP 是流协议, 数据可能分片到达:
//   第1次 read: "star"
//   第2次 read: "tping"
//   第3次 read: "endstart"
//   第4次 read: "echo:helloend"
//
// StreamParser 负责累积缓冲、提取完整消息。
//
// 构建并运行:
//   cmake --build build --config Debug --target coro_practice
//   ./build/Debug/coro_practice
//
// 手动测试 (网络调试助手):
//   连接 127.0.0.1:8800
//   发送: startpingend
//   收到: startpongend
// ============================================================================

#include <coro/coro.hpp>
#include <coro/net.hpp>

#ifdef _WIN32
#include <windows.h>
#endif

#include <chrono>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>

using namespace coro;
using namespace std::chrono_literals;

// ============================================================================
// StreamParser — start/end 流式协议解析器
// ============================================================================
//
// 协议: start<payload>end
//
//   feed() 喂入原始数据, 返回已解析出的完整消息列表。
//   内部维护累积缓冲, 自动处理:
//     - 数据分片 (start 和 end 分布在不同 read 中)
//     - 粘包 (一次 read 包含多条消息)
//     - 半标记 ("star" 跨 read 边界)
//
class StreamParser {
  public:
    StreamParser(const char* begin_mark, const char* end_mark) : begin_(begin_mark), end_(end_mark) {}

    /// 喂入原始数据, 返回已解析出的完整消息 (载荷部分, 不含标记)
    std::vector<std::string> feed(const char* data, size_t len) {
        buf_.append(data, len);
        std::vector<std::string> messages;

        while (true) {
            // 1. 查找起始标记
            auto pos = buf_.find(begin_);
            if (pos == std::string::npos) {
                // 没找到 start: 保留末尾可能是不完整的 start 标记
                size_t keep = begin_.size() - 1;
                if (buf_.size() > keep)
                    buf_.erase(0, buf_.size() - keep);
                break;
            }

            // 丢弃 start 之前的垃圾数据
            if (pos > 0)
                buf_.erase(0, pos);

            // 2. 查找结束标记 (在 start 之后)
            auto end_pos = buf_.find(end_, begin_.size());
            if (end_pos == std::string::npos)
                break; // end 还没到, 等更多数据

            // 3. 提取载荷
            messages.push_back(buf_.substr(begin_.size(), end_pos - begin_.size()));

            // 4. 移除已解析的消息
            buf_.erase(0, end_pos + end_.size());
        }

        return messages;
    }

    void clear() { buf_.clear(); }
    size_t buffered() const { return buf_.size(); }

  private:
    std::string begin_;
    std::string end_;
    std::string buf_;
};

// 把响应包装成协议帧: start<response>end
static std::string make_frame(const std::string& response) {
    return "start" + response + "end";
}

// ============================================================================
// 协议处理: 根据载荷命令生成响应
// ============================================================================
// 支持的命令:
//   ping          → 回复 pong
//   time          → 回复当前时间戳
//   echo:内容     → 回复 "内容"
//   help          → 回复命令列表
//   json:{...}    → 回复 JSON ACK
//   其他          → 回复 ERR:unknown command
//
static std::string handle_command(const std::string& payload) {
    if (payload == "ping")
        return "pong";
    if (payload == "time") {
        auto now = std::chrono::system_clock::now();
        auto t = std::chrono::system_clock::to_time_t(now);
        char timebuf[64];
        std::strftime(timebuf, sizeof(timebuf), "%Y-%m-%d %H:%M:%S", std::localtime(&t));
        return std::string("time:") + timebuf;
    }
    if (payload.size() >= 5 && payload.substr(0, 5) == "echo:")
        return payload.substr(5);
    if (payload == "help")
        return "commands: ping | time | echo:<msg> | help | json:{...}";
    if (payload.size() >= 5 && payload.substr(0, 5) == "json:")
        return "ack:" + payload.substr(5);
    return "ERR:unknown command '" + payload + "'";
}

// ============================================================================
// TCP 连接处理协程: start/end 协议解析, 根据命令返回响应
// ============================================================================
Task<> handle_connection(net::TcpStream conn) {
    StreamParser parser("start", "end");
    char buf[4096];
    int msg_count = 0;

    std::printf("  [server] ========== 新连接建立 ==========\n");
    fflush(stdout);

    while (true) {
        int n = co_await conn.read(buf, sizeof(buf));
        if (n <= 0) {
            std::printf("  [server] 连接关闭 (n=%d)\n", n);
            fflush(stdout);
            break;
        }

        std::printf("  [server] 收到 %d 字节: \"%.*s\"\n", n, n, buf);
        fflush(stdout);

        // 喂入解析器, 可能提取出 0/1/多条完整消息
        auto messages = parser.feed(buf, static_cast<size_t>(n));
        std::printf("  [server] 解析出 %zu 条消息 (缓冲 %zu 字节)\n", messages.size(), parser.buffered());
        fflush(stdout);

        for (auto& msg : messages) {
            ++msg_count;
            std::printf("  [server] [#%d] 解析: \"%s\"\n", msg_count, msg.c_str());
            fflush(stdout);

            // 根据命令生成响应, 包装成帧发回
            std::string response = handle_command(msg);
            std::string frame = make_frame(response);

            std::printf("  [server] [#%d] 回复: \"%s\"\n", msg_count, response.c_str());
            fflush(stdout);
            co_await conn.write(frame.data(), frame.size());
        }
    }

    std::printf("  [server] 连接结束, 共处理 %d 条消息\n", msg_count);
    fflush(stdout);
}

// ============================================================================
// TCP 测试客户端: 发送 start/end 格式命令, 验证协议响应
// ============================================================================
Task<> tcp_test_client() {
    co_await sleep(200ms); // 等服务端就绪

    std::printf("\n[client] 连接 127.0.0.1:8800...\n");
    fflush(stdout);

    auto conn = co_await net::TcpStream::connect("127.0.0.1", 8800);
    if (!conn.valid()) {
        std::printf("[client] 连接失败!\n");
        fflush(stdout);
        co_return;
    }
    std::printf("[client] 已连接!\n\n");
    fflush(stdout);

    // 辅助: 发送一条协议消息并打印响应
    auto send_cmd = [&](const char* payload) -> Task<> {
        std::string frame = make_frame(payload);
        co_await conn.write(frame.data(), frame.size());
        std::printf("  >>> %s\n", payload);

        char rbuf[1024];
        int n = co_await conn.read(rbuf, sizeof(rbuf));
        if (n > 0) {
            // 用解析器提取响应
            StreamParser rp("start", "end");
            auto msgs = rp.feed(rbuf, static_cast<size_t>(n));
            for (auto& m : msgs)
                std::printf("  <<< %s\n", m.c_str());
        } else {
            std::printf("  <<< (无数据 n=%d)\n", n);
        }
        fflush(stdout);
    };

    // ---- 测试 1: ping → pong ----
    std::printf("=== 测试 1: ping ===\n");
    co_await send_cmd("ping");
    std::printf("\n");

    // ---- 测试 2: time → 时间戳 ----
    std::printf("=== 测试 2: time ===\n");
    co_await send_cmd("time");
    std::printf("\n");

    // ---- 测试 3: echo:内容 → 返回内容 ----
    std::printf("=== 测试 3: echo ===\n");
    co_await send_cmd("echo:你好，协程服务端！");
    std::printf("\n");

    // ---- 测试 4: help → 命令列表 ----
    std::printf("=== 测试 4: help ===\n");
    co_await send_cmd("help");
    std::printf("\n");

    // ---- 测试 5: JSON 载荷 → ACK ----
    std::printf("=== 测试 5: json ===\n");
    co_await send_cmd("json:{\"cmd\":\"login\",\"user\":\"admin\"}");
    std::printf("\n");

    // ---- 测试 6: 未知命令 → ERR ----
    std::printf("=== 测试 6: 未知命令 ===\n");
    co_await send_cmd("foobar");
    std::printf("\n");

    // ---- 测试 7: 粘包 (两条消息一次发) ----
    std::printf("=== 测试 7: 粘包 ===\n");
    {
        std::string combined = make_frame("ping") + make_frame("echo:粘包测试");
        co_await conn.write(combined.data(), combined.size());
        std::printf("  >>> [startpingend + startecho:粘包测试end] (两条一次发)\n");

        char rbuf[1024];
        int n = co_await conn.read(rbuf, sizeof(rbuf));
        if (n > 0) {
            StreamParser rp("start", "end");
            auto msgs = rp.feed(rbuf, static_cast<size_t>(n));
            for (auto& m : msgs)
                std::printf("  <<< %s\n", m.c_str());
        }
        fflush(stdout);
    }
    std::printf("\n");

    // ---- 测试 8: 分片发送 ----
    std::printf("=== 测试 8: 分片发送 ===\n");
    {
        const char* p1 = "star";
        const char* p2 = "t分片消息end";
        co_await conn.write(p1, strlen(p1));
        std::printf("  [片段1] \"%s\"\n", p1);
        co_await sleep(50ms);
        co_await conn.write(p2, strlen(p2));
        std::printf("  [片段2] \"%s\"\n", p2);

        char rbuf[1024];
        int n = co_await conn.read(rbuf, sizeof(rbuf));
        if (n > 0) {
            StreamParser rp("start", "end");
            auto msgs = rp.feed(rbuf, static_cast<size_t>(n));
            for (auto& m : msgs)
                std::printf("  <<< %s\n", m.c_str());
        }
        fflush(stdout);
    }
    std::printf("\n");

    conn.close();
    std::printf("[client] 全部测试完成!\n");
    fflush(stdout);
}

// ============================================================================
// 主协程
// ============================================================================
Task<> main_task() {
    std::printf("=== coro start/end 协议服务端 ===\n");
    std::printf("协议: start<命令>end → start<响应>end\n");
    std::printf("监听: 127.0.0.1:8800\n\n");
    fflush(stdout);

    // ---- 启动 TCP 服务端 ----
    net::TcpListener listener;
    if (!listener.bind_listen("127.0.0.1", 8800)) {
        std::printf("bind 失败!\n");
        co_return;
    }
    std::printf("[server] 监听中...\n");
    fflush(stdout);

    // 存储 handler 任务, 防止 Task 析构销毁协程
    std::vector<Task<>> handlers;

    // ---- 第一阶段: 自动测试 ----
    {
        auto client = spawn(tcp_test_client());

        // accept 测试客户端的连接
        auto conn = co_await listener.accept();
        if (conn.valid()) {
            handlers.push_back(spawn(handle_connection(std::move(conn))));
        }

        // 等待测试客户端完成
        co_await std::move(client);
    }

    // ---- 第二阶段: 等待手动连接 ----
    co_await sleep(1s);

    std::printf("\n");
    std::printf("========================================\n");
    std::printf("  自动测试完成! 服务端持续运行中...\n");
    std::printf("  请用网络调试助手连接 127.0.0.1:8800\n");
    std::printf("  发送格式: start你的命令end\n");
    std::printf("  例: startpingend\n");
    std::printf("      startecho:helloend\n");
    std::printf("  Ctrl+C 退出\n");
    std::printf("========================================\n\n");
    fflush(stdout);

    // 持续 accept, 等待手动连接
    while (true) {
        // 清理已完成的 handler
        std::erase_if(handlers, [](const Task<>& t) { return t.is_ready(); });

        auto conn = co_await listener.accept();
        if (!conn.valid())
            break;

        std::printf("[server] 接受新连接, 启动 handler\n");
        fflush(stdout);
        handlers.push_back(spawn(handle_connection(std::move(conn))));
    }

    std::printf("\n=== 结束 ===\n");
}

int main() {
#ifdef _WIN32
    SetConsoleOutputCP(65001);
    SetConsoleCP(65001);
#endif
    coro::run(main_task());
    return 0;
}
