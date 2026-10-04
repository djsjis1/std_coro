#pragma once

#include "exceptions.hpp"
#include "task.hpp"

#include <cstddef>
#include <cstring>
#include <cerrno> // 直接用 errno 就必须自己 include: <system_error> 不保证提供它
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <utility>
#include <vector>

// ============================================================================
// coro::stream — 字节流上的 reader / writer 适配层 (计划 M4a)
// ============================================================================
//
// 为什么要有这一层: TcpStream/pipe/未来的 TLS 都只提供"一次 read/write 若干个字节"的
// 原语, 而真实协议需要"读一行""读满定长头""把这段完整写出去"。把这些写在每个使用处
// 会各自漏掉短读、跨缓冲的分隔符、以及"0 字节写入"这三种边界, 而它们恰好是最难复现的 bug。
//
// 用法:
//   coro::net::TcpStream sock = co_await coro::net::TcpStream::connect("127.0.0.1", 8080);
//   coro::stream_reader reader(sock);
//   coro::stream_writer writer(sock);
//   while (auto line = co_await reader.read_line()) handle(*line);
//   co_await writer.write_all("PING\r\n");
//
// 四种结局必须互相可区分 (这是本层的核心合同, 也是测试逐条锁住的东西):
//   - 对端正常结束      -> nullopt (read_*) / false (write_* 无法推进时)
//   - IO 错误           -> 抛 std::system_error, errno 与 io::last_error() 均已填好
//   - 被取消            -> 抛 CancelledError (由 Task 的取消注入负责, 本层不拦)
//   - 要求读满却提前结束 -> 抛 IncompleteStreamError, 并带上 requested/received
//   零进展写入 (write 返回 0) 返回 false, 不继续循环。
//
// 多态方式选模板而不是基类: AsyncReadable/AsyncWritable 只有两个方法, 但每个方法都是
// awaitable —— 用虚函数擦除就得让虚函数返回类型擦除的 awaiter, 代价与复杂度都不划算;
// 直接按 Source 模板化, 零开销, 也不会催生一个"什么都能包"的巨大基类。
// ============================================================================

namespace coro {
    namespace detail {

        /// 源对象的 API 形状要求。真正的"能不能 co_await"留给实例化时诊断 ——
        /// requires 表达式里写不了 co_await, 硬造 trait 只会让错误信息更难读。
        template <typename S>
        concept AsyncReadable = std::is_class_v<S> && requires(S& s, char* buf, std::size_t n) {
            { s.read(buf, n) };
        };

        template <typename S>
        concept AsyncWritable = std::is_class_v<S> && requires(S& s, const char* buf, std::size_t n) {
            { s.write(buf, n) };
        };

        /// 把底层"返回字节数 / 0 / -1"的约定翻译成异常或直接判定, 集中一处免得每个
        /// 调用点各写一遍。取消在上层由 await_transform 注入, 不会走到这里。
        [[noreturn]] inline void throw_io_error(const char* what) {
            const auto error =
                errno ? std::error_code(errno, std::generic_category()) : std::make_error_code(std::errc::io_error);
            throw std::system_error(error, what);
        }

        // 协程形式的 Source 可能跨调度轮次返回, errno 会被同线程其他任务覆盖。
        // 优先读取 Source 自己保存的错误; 不依赖任何具体传输实现。
        template <typename Source> [[noreturn]] void throw_io_error(Source& source, const char* what) {
            if constexpr (requires { std::error_code(source.last_error_code()); }) {
                auto error = std::error_code(source.last_error_code());
                throw std::system_error(error ? error : std::make_error_code(std::errc::io_error), what);
            } else {
                throw_io_error(what);
            }
        }

        /// 缓冲到上限仍找不到分隔符
        [[noreturn]] inline void throw_overflow(std::size_t limit) {
            throw StreamOverflowError(limit);
        }

    } // namespace detail

    /// 缓冲读端。Source 按引用使用: 它必须比 reader 活得久 (与 channel 的 loop-local
    /// 约定一致, reader 不接管所有权, 因此也不负责关闭)。同一 reader 的读取不得重叠。
    template <detail::AsyncReadable Source> class stream_reader {
      public:
        /// buffer_capacity: 初始缓冲大小; max_capacity: 缓冲硬上限 —— 分隔符始终不出现时
        /// 抛 StreamOverflowError, 而不是无界吃内存 (有界背压的前提是上限真实存在)。
        ///
        /// buf_ 的 size() 表示**容量**, 已用长度由 used_ 单独记账。
        /// 曾经用 vector(count) + buf_.size() 当"有效字节数", 结果两个坑同时踩:
        /// 初始就有 NUL 被当成数据, 而扩容的 resize 又把逻辑长度撑大并零初始化新元素,
        /// 实测 reader 全挂 (拿到 NUL 串) 而 writer 全过 —— 因为 writer 不碰缓冲。
        explicit stream_reader(Source& src, std::size_t buffer_capacity = 8192, std::size_t max_capacity = 1u << 20)
            : src_(&src), buf_(initial_capacity(buffer_capacity, max_capacity), '\0'), max_(max_capacity) {}

        stream_reader(const stream_reader&) = delete;
        stream_reader& operator=(const stream_reader&) = delete;

        /// 已缓冲但尚未被消费的字节数 (跨次调用累积, 不会丢数据)
        std::size_t buffered() const noexcept { return used_ - head_; }

        /// 至少读到一个字节; nullopt = 对端正常结束。短读在这里被吸收: 只要拿到 >=1 就返回。
        Task<std::optional<std::size_t>> read_some(char* dst, std::size_t n) {
            if (n == 0)
                co_return std::make_optional<std::size_t>(0);
            if (!dst)
                throw std::invalid_argument("stream_reader::read_some requires a buffer");
            std::size_t got = 0;
            while (got == 0) {
                if (buffered() == 0) {
                    auto filled = co_await fill();
                    if (!filled.has_value())
                        co_return std::nullopt; // EOF: 且缓冲里也没剩东西
                }
                got = take_into(dst, n);
            }
            co_return std::make_optional<std::size_t>(got);
        }

        /// 读满 n 字节。凑不齐就对端结束 → 抛 IncompleteStreamError (与 EOF、错误、取消并列)。
        Task<> read_exactly(char* dst, std::size_t n) {
            if (n != 0 && !dst)
                throw std::invalid_argument("stream_reader::read_exactly requires a buffer");
            std::size_t got = 0;
            while (got < n) {
                if (buffered() == 0) {
                    auto filled = co_await fill();
                    if (!filled.has_value())
                        throw IncompleteStreamError(n, got);
                }
                got += take_into(dst + got, n - got);
            }
            co_return;
        }

        /// 读到 delim 为止 (含 delim 的分隔符本身被吞掉, 不进入返回值)。
        /// nullopt = 对端结束且无残留; 有残留时把残留当作最后一段返回 (不丢尾部数据)。
        Task<std::optional<std::string>> read_until(std::string_view delim) {
            if (delim.empty())
                throw std::invalid_argument("stream_reader::read_until requires a non-empty delimiter");
            return read_until_owned(std::string(delim));
        }

      private:
        static std::size_t initial_capacity(std::size_t requested, std::size_t maximum) {
            if (maximum == 0)
                throw std::invalid_argument("stream_reader max_capacity must be positive");
            if (requested == 0)
                requested = 1;
            return requested < maximum ? requested : maximum;
        }

        /// 按值持有: read_until 以临时 std::string(delim) 转发进来, 惰性 Task 在
        /// 首次 co_await 前不会运行, 临时那时早已析构 —— const& 形参会读到悬空
        /// 内存 (实测分隔符扫描命中脏数据)。cppcheck 的 passedByValue 建议对
        /// 普通函数成立, 对惰性协程入口不成立。
        // cppcheck-suppress passedByValue
        Task<std::optional<std::string>> read_until_owned(std::string delim) {
            for (;;) {
                if (auto hit = scan(delim)) {
                    std::string out = take_bytes(*hit);
                    head_ += delim.size(); // 吞掉分隔符
                    co_return out;
                }
                auto filled = co_await fill();
                if (!filled.has_value()) {
                    if (buffered() == 0)
                        co_return std::nullopt;
                    co_return take_bytes(buffered()); // 尾部没有分隔符, 也要交给调用方
                }
            }
        }

      public:
        /// 读一行 (CR LF / 单独 LF 都算行尾, 行尾不进返回值)。适合 HTTP 头这类文本协议。
        Task<std::optional<std::string>> read_line() {
            for (;;) {
                if (auto nl = find_byte('\n')) {
                    const std::size_t upto = *nl; // LF 之前的字节数 (可能含 CR)
                    std::size_t content = upto;
                    if (content > 0 && buf_[head_ + content - 1] == '\r')
                        --content; // 裁掉 CR: 它不属于行内容
                    std::string out = take_bytes(content);
                    // 必须把"剩下的 CR + LF"整段消费掉。只 += 1 会把 LF 留下,
                    // 下一轮立刻命中偏移 0 的 '\n' → 多吐一个空行 (实测 "one||two||")。
                    head_ += (upto - content) + 1;
                    co_return out;
                }
                auto filled = co_await fill();
                if (!filled.has_value()) {
                    if (buffered() == 0)
                        co_return std::nullopt;
                    // 没有 LF 时末尾 CR 也是数据, 全部返回并消费, 不丢字节或重复返回。
                    co_return take_bytes(buffered());
                }
            }
        }

      private:
        /// 从源再吸一块进缓冲。nullopt = 对端正常结束; IO 错误直接抛。
        ///
        /// 记账规则: buf_ 的 size 就是**容量**, 已用长度另用 used_ 表示。
        /// 之前拿 vector::size() 当"有效字节数", 于是扩容的 resize 会把逻辑长度
        /// 一起撑大 (新元素被零初始化), 实测结果是 "pref" + 1024 个 NUL + 后续内容 ——
        /// 数据被写到偏移 1028 而不是 4。分开记账后, fill 期间不再碰 size。
        Task<std::optional<std::size_t>> fill() {
            if (eof_)
                co_return std::nullopt;
            if (head_ > 0) { // 线性缓冲: 先压缩, 避免每轮 memcpy 抖动
                std::memmove(buf_.data(), buf_.data() + head_, used_ - head_);
                used_ -= head_;
                head_ = 0;
            }
            std::size_t room = buf_.size() - used_;
            if (room == 0) {
                if (buf_.size() >= max_)
                    detail::throw_overflow(max_); // 到顶: 明确失败, 不再无节制增长
                const std::size_t remaining = max_ - buf_.size();
                const std::size_t bigger = buf_.size() + (remaining < 1024 ? remaining : 1024);
                buf_.resize(bigger); // 只扩容量; 新尾部属于"未用"区, 由 used_ 排除在外
                room = buf_.size() - used_;
            }
            int n = co_await src_->read(buf_.data() + used_, room);
            if (n == 0) {
                eof_ = true;
                co_return std::nullopt;
            }
            if (n < 0)
                detail::throw_io_error(*src_, "stream_reader::fill");
            used_ += static_cast<std::size_t>(n);
            co_return std::make_optional<std::size_t>(static_cast<std::size_t>(n));
        }

        std::size_t take_into(char* dst, std::size_t n) {
            std::size_t take = buffered() < n ? buffered() : n;
            std::memcpy(dst, buf_.data() + head_, take);
            head_ += take;
            return take;
        }

        std::string take_bytes(std::size_t n) {
            std::string out(buf_.data() + head_, n);
            head_ += n;
            return out;
        }

        /// 缓冲里第一个 delim 的偏移 (可能跨 fill 边界, 所以每次都整段扫)
        std::optional<std::size_t> scan(std::string_view delim) const {
            if (delim.empty() || buffered() < delim.size())
                return std::nullopt;
            const char* base = buf_.data() + head_;
            std::size_t left = buffered() - delim.size();
            for (std::size_t i = 0; i <= left; ++i) {
                if (std::memcmp(base + i, delim.data(), delim.size()) == 0)
                    return i;
            }
            return std::nullopt;
        }

        std::optional<std::size_t> find_byte(char c) const {
            const char* base = buf_.data() + head_;
            for (std::size_t i = 0; i < buffered(); ++i) {
                if (base[i] == c)
                    return i;
            }
            return std::nullopt;
        }

        Source* src_;
        std::vector<char> buf_; ///< size() 恒为**容量** (不是有效长度)
        std::size_t used_ = 0;  ///< 已填入的字节数 ([0, used_) 有效)
        std::size_t head_ = 0;  ///< 下一个待消费字节的位置
        std::size_t max_;       ///< 缓冲硬上限: fill 到此即报错, 不再无节制增长
        bool eof_ = false;
    };

    /// 写端。只负责一件事: 要么完整写出, 要么以明确的结论失败, 绝不静默循环。
    template <detail::AsyncWritable Source> class stream_writer {
      public:
        explicit stream_writer(Source& src) : src_(&src) {}

        /// 完整写出全部字节。true = 全写完; false = 对端提前结束(写不动了), 已写出的部分
        /// 不回收 —— 调用方需要知道协议层已不一致。IO 错误抛 system_error, 取消抛 CancelledError。
        Task<bool> write_all(const char* data, std::size_t n) {
            if (n != 0 && !data)
                throw std::invalid_argument("stream_writer::write_all requires a buffer");
            std::size_t done = 0;
            while (done < n) {
                int written = co_await src_->write(data + done, n - done);
                if (written == 0)
                    co_return false; // 流已不可写: 再循环就是死循环
                if (written < 0)
                    detail::throw_io_error(*src_, "stream_writer::write_all");
                done += static_cast<std::size_t>(written);
            }
            co_return true;
        }

        /// 字符串重载持有副本, 临时字符串在任务开始前析构也安全。
        /// 零拷贝调用使用 (data, size), 并由调用者保持缓冲有效到任务结束。
        Task<bool> write_all(std::string_view s) { return write_owned(std::string(s)); }

        /// 写一行并带上 LF (文本协议常用; 需要 CRLF 就自己带在字符串里)
        Task<bool> write_line(std::string_view s) {
            std::string line(s);
            line.push_back('\n');
            return write_owned(std::move(line));
        }

      private:
        /// 按值持有: write_all(string_view)/write_line 以临时字符串转发进来,
        /// 惰性 Task 在首次 co_await 前不会运行, const& 形参会读到已析构的
        /// 临时 (实测短写路径返回 false)。契约见上方 write_all 注释。
        // cppcheck-suppress passedByValue
        Task<bool> write_owned(std::string data) { co_return co_await write_all(data.data(), data.size()); }

        Source* src_;
    };

    /// CTAD 指引: 让 coro::stream_reader r(sock) 直接推导 Source
    template <typename S> stream_reader(S&) -> stream_reader<S>;
    template <typename S> stream_reader(S&, std::size_t) -> stream_reader<S>;
    template <typename S> stream_reader(S&, std::size_t, std::size_t) -> stream_reader<S>;
    template <typename S> stream_writer(S&) -> stream_writer<S>;

} // namespace coro
