#include "web_server.h"

#include <http_parse.h> // llhttp 的 C 封装头文件, 提供 http_parse 类(增量解析器)

#include <algorithm>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>

using namespace std::chrono_literals;

// 访问日志互斥锁: 多个 worker 线程会并发打印日志, 而 std::cout 不是线程安全的,
// 所以每次写 cout/cerr 前都要加锁, 否则输出会交错乱掉
static std::mutex g_log_mutex;

// 完整写入: 循环 write 直到所有数据发出 (TCP 可能部分写入)
static coro::Task<bool> write_all(coro::net::TcpStream& conn, const std::string& data) {
    size_t off = 0;
    while (off < data.size()) {
        int n = co_await conn.write(data.data() + off, data.size() - off);
        if (n <= 0)
            co_return false;
        off += n;
    }
    co_return true;
}

static coro::Task<int> read_once(coro::net::TcpStream* conn, char* data, size_t size) {
    co_return co_await conn->read(data, size);
}

static coro::Task<int> read_with_timeout(coro::net::TcpStream& conn, char* data, size_t size,
                                         std::chrono::milliseconds timeout) {
    auto read = read_once(&conn, data, size);
    if (timeout.count() <= 0)
        co_return co_await std::move(read);
    co_return co_await coro::wait_for(std::move(read), timeout);
}

static coro::Task<bool> write_with_timeout(coro::net::TcpStream& conn, const std::string& data,
                                           std::chrono::milliseconds timeout) {
    auto write = write_all(conn, data);
    if (timeout.count() <= 0)
        co_return co_await std::move(write);
    co_return co_await coro::wait_for(std::move(write), timeout);
}

// <=0 表示不限时；两个正值同时存在时取更严格的那个。
static std::chrono::milliseconds stricter_timeout(std::chrono::milliseconds a, std::chrono::milliseconds b) {
    if (a.count() <= 0)
        return b;
    if (b.count() <= 0)
        return a;
    return std::min(a, b);
}

web_server::web_server(size_t workers)
    // workers==0 时自动取 CPU 核心数, 让 worker 线程数匹配硬件能力
    : scheduler_(workers == 0 ? std::thread::hardware_concurrency() : workers) {
    // scheduler_ 构造时即启动所有 worker 线程, 每个 worker 内部跑一个 EventLoop,
    // 析构时自动 join 所有线程, 无需手动管理生命周期
}

bool web_server::listen(const char* ip, unsigned short port) {
    // bind_listen: 内部做了 bind() + listen() 两步, 返回 false 表示端口被占用或地址无效
    if (!listener_.bind_listen(ip, port)) {
        std::lock_guard lock(g_log_mutex);
        std::cerr << "[web] listen failed on " << ip << ":" << port << std::endl;
        return false;
    }
    std::lock_guard lock(g_log_mutex);
    std::cout << "[web] listening on " << ip << ":" << port << " (workers=" << worker_count() << ")" << std::endl;
    return true;
}

void web_server::stop() {
    running_.store(false, std::memory_order_release); // 通知 serve() 且阻止新连接注册
    listener_.close();                                // 关键: 仅设 flag 不够, 因为 accept 正阻塞在 IOCP 上等待新连接.
    // 关闭监听 socket 后, 挂起的 AcceptEx 会立即以错误完成包返回,
    // 这样 accept_noattach() 才会解除挂起, serve() 循环才能检查到 running_==false 并退出

    // 保留 shared_ptr 快照后再解锁并 shutdown，避免持锁调用系统 API，
    // 也保证 worker 同时完成连接、从集合移除时对象仍然存活。
    std::vector<std::shared_ptr<coro::net::TcpStream>> active;
    {
        std::lock_guard lock(connections_mutex_);
        active.assign(connections_.begin(), connections_.end());
    }
    for (auto& conn : active)
        conn->shutdown();
}

coro::Task<> web_server::serve() {
    running_.store(true, std::memory_order_release);
    while (running_.load(std::memory_order_acquire)) {
        // ---- 第一步: 接受新连接 ----
        // accept_noattach(): 这是 Windows IOCP 特有的设计.
        //   普通 accept 会在主线程的 IOCP 上关联 socket, 但 Windows 不允许
        //   把已关联 IOCP 的 socket 再转移到另一个 IOCP.
        //   所以这里先 accept 但不关联任何 IOCP, 留给 worker 线程自己去关联.
        //   (Linux io_uring 没有"关联"概念, 此函数等价于普通 accept)
        auto conn = co_await listener_.accept_noattach();
        // co_await 会挂起当前协程, 直到有新连接到来才恢复执行(不占 CPU)
        if (!conn.valid()) {
            if (!running_.load(std::memory_order_acquire))
                break; // stop() 关闭了监听 socket, accept 失败是预期行为, 正常退出
            std::lock_guard lock(g_log_mutex);
            std::cerr << "[web] accept failed, retrying..." << std::endl;
            co_await coro::sleep(10ms); // 等 10ms 再重试, 避免错误时死循环空转
            continue;
        }

        // ---- 第二步: 把连接分发到 worker 线程池 ----
        // 为什么用工厂模式 (spawn_any)?
        //   1. 协程帧(堆上的 coroutine state)必须在同一个线程上创建和销毁,
        //      否则 MSVC Debug CRT 会断言失败(跨线程 free).
        //   2. Windows 上 socket 必须关联到 worker 线程的 IOCP,
        //      这样 I/O 完成包才会投递到 worker 的端口, 协程才能被正确唤醒.
        //
        // spawn_any() 的行为: 按负载均衡选一个 worker, 在那个 worker 线程上
        // 调用工厂 lambda. 所以 lambda 内部代码运行在 worker 线程上:
        //   - reattach(): 把 socket 关联到当前 worker 的 IOCP (首次关联)
        //   - handle_connection(): 协程帧在 worker 线程创建, 后续 I/O 也在此线程完成
        //
        // 为什么用 shared_ptr?
        //   std::function 要求可拷贝(std::function 内部要 copy), 但 TcpStream
        //   持有 socket 句柄, 是 move-only 的. 用 shared_ptr 包一层就变成可拷贝了.
        auto sp = std::make_shared<coro::net::TcpStream>(std::move(conn));
        if (!register_connection(sp)) {
            sp->shutdown();
            break;
        }
        scheduler_.spawn_any([this, sp]() mutable {
            sp->reattach(); // 在 worker 线程上关联 IOCP
            return handle_connection(std::move(sp));
        });
        // spawn_any 本身不等待 handle_connection 完成, 它只是"发射"一个协程
        // 到 worker 线程, 然后立即返回, serve() 循环继续 accept 下一个连接
    }
    std::lock_guard lock(g_log_mutex);
    std::cout << "[web] accept loop stopped" << std::endl;
}

void web_server::wait_all() {
    scheduler_.wait_all();
}

bool web_server::register_connection(const std::shared_ptr<coro::net::TcpStream>& conn) {
    std::lock_guard lock(connections_mutex_);
    if (!running_.load(std::memory_order_acquire))
        return false;
    connections_.insert(conn);
    return true;
}

void web_server::unregister_connection(const std::shared_ptr<coro::net::TcpStream>& conn) {
    std::lock_guard lock(connections_mutex_);
    connections_.erase(conn);
}

coro::Task<> web_server::handle_connection(std::shared_ptr<coro::net::TcpStream> conn) {
    try {
        co_await process_connection(*conn);
    } catch (...) {
        unregister_connection(conn);
        throw;
    }
    unregister_connection(conn);
}

coro::Task<> web_server::process_connection(coro::net::TcpStream& conn) {
    // ---- 为这个连接创建一个 HTTP 解析器 ----
    // http_parse 是对 llhttp (高性能 C 解析库) 的封装.
    // llhttp 是"增量解析"模式: 你喂给它一段字节, 它解析出尽可能多的完整请求,
    // 通过回调通知你每个请求的字段. 一段 TCP 数据可能包含多条请求(HTTP pipelining).
    http_parse parser(HTTP_REQUEST);
    parser.body_limit = max_body_; // 超过此限制, llhttp 直接报错, 防止恶意大 body 攻击

    // ---- 解析回调: 每当 llhttp 解析完一条完整请求时触发 ----
    // 为什么要立即"快照"到 pending 队列?
    //   因为 llhttp 是增量解析, 同一段 buffer 里可能有 3 条请求.
    //   feed() 过程中会连续触发 3 次 message_complete,
    //   如果不立即保存, 下一次解析会覆盖 parser 内部的字段.
    //   所以每次回调都把当前结果拷贝到 pending 队列里.
    std::deque<http_request> pending; // 双端队列: 支持 O(1) 的头部弹出
    bool request_in_progress = false;
    std::optional<std::chrono::steady_clock::time_point> request_deadline;
    parser.message_begin = [&]() {
        request_in_progress = true;
        const auto timeout = std::chrono::milliseconds(request_timeout_ms_.load(std::memory_order_relaxed));
        if (timeout.count() > 0)
            request_deadline = std::chrono::steady_clock::now() + timeout;
        else
            request_deadline.reset();
    };
    parser.message_complete = [&]() {
        http_request req;
        req.method = std::move(parser.http_method);   // 如 "GET", "POST"
        req.url = std::move(parser.http_url);         // 如 "/greet?name=coro"
        req.version = std::move(parser.http_version); // 如 "1.1"
        req.body = std::move(parser.http_body);       // POST 请求体
        // 头部整表 move (约 10-20 个节点): llhttp 在下一条 message_begin 的
        // clear_result 里会重置容器, move 走后无需保留旧内容 —— 每请求
        // 省一次 map 深拷贝 (每个头部一次节点分配)
        req.headers = std::move(parser.http_headers); // 所有头部键值对
        req.keep_alive = parser.keep_alive();         // HTTP/1.1 默认 true, 除非显式 Connection: close
        pending.push_back(std::move(req));            // 入队, 等下面主循环处理
        request_in_progress = false;
        request_deadline.reset();
    };

    char buf[8192]; // 每次最多读 8KB, 对于大多数请求足够(一个 GET 通常 < 1KB)
    while (true) {
        // ---- 阶段 A: 处理所有已解析完成的请求 ----
        // 一段 TCP 数据可能包含多条请求(pipelining), 所以用 while 循环全部处理完
        while (!pending.empty()) {
            http_request req = std::move(pending.front()); // move 避免拷贝
            pending.pop_front();

            // 打印访问日志(可选, 压测时可关闭以提升性能)
            if (verbose_) {
                std::lock_guard lock(g_log_mutex);
                std::cout << "[web] " << req.method << " " << req.url
                          << " (keep-alive=" << (req.keep_alive ? "yes" : "no") << ")" << std::endl;
            }

            // ---- 路由分发: 根据 method + url 找到对应 handler 并执行 ----
            // dispatch 返回 http_response, co_await 挂起直到 handler 完成
            // (handler 内部可能 co_await sleep / 数据库查询等异步操作)
            http_response resp = co_await dispatch(req);

            // ---- 序列化响应为 HTTP 报文并发送 ----
            // resp.build() 生成类似:
            //   HTTP/1.1 200 OK\r\n
            //   Content-Type: text/plain\r\n
            //   Content-Length: 13\r\n
            //   \r\n
            //   Hello, World!
            std::string wire = resp.build();
            try {
                const auto timeout = std::chrono::milliseconds(write_timeout_ms_.load(std::memory_order_relaxed));
                if (!co_await write_with_timeout(conn, wire, timeout))
                    co_return; // 对端已关闭/写错误时不要继续读取同一连接
            } catch (const coro::TimeoutError&) {
                co_return;
            }

            // HTTP keep-alive: 如果客户端要求关闭连接(Connection: close),
            // 处理完这条请求后就退出, 让连接自然关闭
            if (!req.keep_alive)
                co_return; // 退出整个协程, 连接随之销毁
        }

        // ---- 阶段 B: 从 socket 读下一段数据 ----
        // co_await 挂起当前协程, 等数据到达后由 IOCP 唤醒, 不占 CPU
        auto timeout = std::chrono::milliseconds(idle_timeout_ms_.load(std::memory_order_relaxed));
        if (request_in_progress && request_deadline) {
            const auto now = std::chrono::steady_clock::now();
            if (now >= *request_deadline)
                co_return;
            // 向上取整，避免剩余不到 1ms 被截成 0 后误解为“禁用超时”。
            auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(*request_deadline - now);
            if (remaining.count() == 0)
                remaining = 1ms;
            timeout = stricter_timeout(timeout, remaining);
        }

        int n = 0;
        try {
            n = co_await read_with_timeout(conn, buf, sizeof(buf), timeout);
        } catch (const coro::TimeoutError&) {
            co_return;
        }
        if (n <= 0)
            co_return; // n==0: 对端正常关闭(FIN); n<0: 网络错误. 都直接退出

        // ---- 阶段 C: 增量解析 ----
        // 把读到的字节喂给 llhttp, 它会自动:
        //   1. 解析请求行/头部/体
        //   2. 每当解析完一条完整请求, 触发 message_complete 回调 → 入队 pending
        //   3. 返回 false 表示解析出错(格式非法等)
        if (!parser.feed(buf, (size_t)n)) {
            // 解析失败 → 返回 400 Bad Request 并关闭连接
            {
                std::lock_guard lock(g_log_mutex);
                std::cout << "[web] parse error: " << parser.error() << std::endl;
            }
            http_response resp = http_response::error(400, "bad request");
            std::string wire = resp.build();
            try {
                const auto write_timeout = std::chrono::milliseconds(write_timeout_ms_.load(std::memory_order_relaxed));
                (void)co_await write_with_timeout(conn, wire, write_timeout);
            } catch (const coro::TimeoutError&) {
            }
            co_return; // 解析错误后连接状态不可信, 直接关闭
        }
        // 循环回到阶段 A, 检查 pending 里是否有新解析完的请求
    }
}

coro::Task<http_response> web_server::dispatch(http_request& req) {
    // 异常兜底: handler 是用户写的代码, 可能抛任何异常.
    // 如果不捕获, 异常会逃逸到协程帧的析构, 导致整个连接协程崩溃,
    // 客户端收到不完整的响应. 所以这里统一兜底返回 500.
    try {
        // router_.route() 遍历路由表, 找到匹配的 handler 并执行.
        // co_await 两层: route() 本身是协程, handler 也是协程,
        // 所以需要先 await route 找到 handler, 再 await handler 执行完
        co_return co_await router_.route(req);
    } catch (const coro::CancelledError&) {
        // 客户端断开、服务停止或上层超时产生的取消不是 500。
        // 重新抛出后由连接任务的取消/分离收尾逻辑处理。
        throw;
    } catch (const std::exception& e) {
        // 标准异常(如 runtime_error, invalid_argument 等): 记录日志 + 返回 500
        // 注意: 这里不关闭连接, keep-alive 仍然可用, 下一条请求可以正常处理
        {
            std::lock_guard lock(g_log_mutex);
            std::cerr << "[web] handler exception: " << e.what() << std::endl;
        }
        co_return http_response::error(500, "internal server error");
    } catch (...) {
        // 非标准异常(如 throw 42 / throw "oops"): 同样兜底.
        // catch(...) 必须放在最后一个 catch, 否则它会先匹配到
        {
            std::lock_guard lock(g_log_mutex);
            std::cerr << "[web] handler exception: unknown (non-std)" << std::endl;
        }
        co_return http_response::error(500, "internal server error");
    }
}
