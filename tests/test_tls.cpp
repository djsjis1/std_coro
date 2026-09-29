// test_tls.cpp — coro::tls 的握手、验证、流集成与关闭合同 (计划 M5)
//
// 证书是**一次性的测试专用自签名对** (localhost / 127.0.0.1), 内嵌在本文件里, 运行时
// 写到临时目录。它不属于任何信任链, 绝不能用于真实服务 —— 之所以内嵌而不是调用
// openssl CLI, 是因为测试进程里 spawn CLI 会让用例依赖 PATH 上的工具。
#if defined(CORO_HAS_TLS) && CORO_HAS_TLS

#include <gtest/gtest.h>

#include <coro/coro.hpp>
#include <coro/dns.hpp>
#include <coro/net.hpp>
#include <coro/stream.hpp>
#include <coro/tls.hpp>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <string>
#include <unistd.h>

using namespace std::chrono_literals;

namespace {

    const char* kCertPem = R"PEMCERT(-----BEGIN CERTIFICATE-----
MIIDJTCCAg2gAwIBAgIUNKlCVaA6qxVVJlLRururnTZyLRwwDQYJKoZIhvcNAQEL
BQAwFDESMBAGA1UEAwwJbG9jYWxob3N0MB4XDTI2MDkyOTE0MzcwMloXDTM2MDky
NjE0MzcwMlowFDESMBAGA1UEAwwJbG9jYWxob3N0MIIBIjANBgkqhkiG9w0BAQEF
AAOCAQ8AMIIBCgKCAQEA1qUoMciPlP2eCaAW1OmMMD3lniIMemILLcsVm8BcK0ue
+JbA/BSQ4F5kUy/n4qLz/kscfXj71edAfOOosYoNrHw7GWxR0n336aKlhiXnNXz0
YW0Ts2xoPMQ/CDLCJDcJs85H3UEjt1bN3amhJJlfuUmVAIk9bpvtJuzfnC5HkawW
h8GREDJNnDMtKye+YnWbrfID4R/fBklAfBfp6YktfwMyeDYRyKJMpt0EWcA7q4fP
N3vKUVN910JjDkd4zC/S5d848EHgLLCz5knenp8UEcszak6ekwKTfrtnGUyBTcyc
yfZVLvcZJdBV50vdFWk1D3m86Gy1IKnZclXNmInI2QIDAQABo28wbTAdBgNVHQ4E
FgQU0EhmoB+L/5B4Eo8CJbT86+Vk9I8wHwYDVR0jBBgwFoAU0EhmoB+L/5B4Eo8C
JbT86+Vk9I8wDwYDVR0TAQH/BAUwAwEB/zAaBgNVHREEEzARgglsb2NhbGhvc3SH
BH8AAAEwDQYJKoZIhvcNAQELBQADggEBAB8an3aRNNCzHAqvf0eUsEOKLmYfVKkG
reeRoLmaJvQ3gCGw4CGk6/fpl3jNoV2v7jvFABs/3Fvz+siaHvnaRT4c+i8XialB
fnmYOdOiPvUnBZIZJ+/B+i6Lx6wSoMS1f5mLf8dP1xyGsTCoUTNAcPV4ADslVN9V
5lT3HHinw6ZLAZjN8L5fPxUIllYSVx8Tn/8e1qVuy+BeH7BGryX4ygEv/H3J5/xs
Vq17t1RKbXfTfnDunEW1nQpcGjCpplJOZSXMP8ObOSoA0iZiIDcWQFpG8d/nee4Y
NJ+YBpoS1xy4xKM5F+g6pscnJxt5gzxOYpFs1lYw1x5MHzGrhpBzc2g=
-----END CERTIFICATE-----)PEMCERT";

    const char* kKeyPem = R"PEMKEY(-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDWpSgxyI+U/Z4J
oBbU6YwwPeWeIgx6YgstyxWbwFwrS574lsD8FJDgXmRTL+fiovP+Sxx9ePvV50B8
46ixig2sfDsZbFHSfffpoqWGJec1fPRhbROzbGg8xD8IMsIkNwmzzkfdQSO3Vs3d
qaEkmV+5SZUAiT1um+0m7N+cLkeRrBaHwZEQMk2cMy0rJ75idZut8gPhH98GSUB8
F+npiS1/AzJ4NhHIokym3QRZwDurh883e8pRU33XQmMOR3jML9Ll3zjwQeAssLPm
Sd6enxQRyzNqTp6TApN+u2cZTIFNzJzJ9lUu9xkl0FXnS90VaTUPebzobLUgqdly
Vc2YicjZAgMBAAECggEAOHUci2uriI7lALchCkOb4hyFxwBBs/cR5aujNWJ1W9X6
LHzKqgNmz/1D+jWBkrU15E8xYTdvViYLak4fUwNy8UmD6f35z5nemY90ZkKV/BWv
2ehs5CbBjCo6QhfHxOrNHIRHlsTJdjHY2FMD1bKZ4QyfqpJunCdbS3/GmTf7Dy9u
3LJH/cMWya6OOQxzZDCbGapatiGyrJ6/9wN/fHyTpUg+IwIe903wH2bpzgFGojHU
OHo0rctaIZldDTCXN4D9gMM+QHGJL2sg1DeqL/sOJVLaE2esz0NTRXggz05XMQKN
nN49M672BRSbfhgfxxlo/MadeeTpoevEWoRarunvAQKBgQD8mp8SDPh93GQex9gS
PGgWLekue55dM33vDqbpdc0upEa2TOJ92BU5mD8y0GluVtoHkHtLDqeCIQ0WFimt
3DpQaTk+oWuJRw9C3pAgbtbKlOlFvP3qB5sTNpFDehBhptww/wV51wNug0qISUfz
od9HlKqrgzxEQ9RzD4XOc1+T0QKBgQDZh+TaymPkDcktOWvxKomvhjDdtK9TN266
k1G35Ak34k/YKrqGWEGj7DTVl4zZk2b27t+/n9D2jyhmgSp4KC52fbTffer1QMBM
THpabGo/zKWVT252mjGdMx1sMSSQDa55STSbCk8QPoHfwTAcMY2rNlKGQ64G9pgP
xXEU3jpOiQKBgDq8nXZuC64AfEtn/scmwrE9lbYYSpezbHoU9xzcJozM6CBluli2
0SCmVTO0oH9mtKYDo5etXaf1lXxoROLHjcu62/PDRyURn+vVor/X5hwPCjsMGiK8
CAQssRR2oq6CRTsjYZuWMpcU8lTEbXWqUyfq2tCs4GujNhhXKK77xuKxAoGBAJjD
XmxoBY56P/WKhctvXBHv4xFPenCuVQyhuJmzguXEAN6F40fFYxODJfd53mhNo9l2
F5uy2ETOdEjIHNMVJMwBq6vn+cESH/l1G9e0m0kCpqYcii6wSndjFh6MxGiFsylp
x4+5xZxayUohmW+zPRInq/yuOuCY/GDW/3rwNXJBAoGAHFbog+Ntw2XaRPcT1cjE
unRw0yHDgLaGqZ3NwOyiTp3NatY7RvN3sLyn0CKGPGkLHpWiwovCQlyQrKxeGbwF
0e7s2oPul4cVlDope/J/SOTPAKTO8bAx2hLLmyMDminJzRSFE4foi9XX3JcEUe/K
FnunJq/1n0ciUIJDSIuaerk=
-----END PRIVATE KEY-----)PEMKEY";

    struct pem_files {
        std::string cert_path;
        std::string key_path;
        pem_files() {
            const char* dir = std::getenv("TMPDIR");
            std::string base = dir ? dir : ".";
            cert_path = base + "/coro_tls_test_cert_" + std::to_string(::getpid()) + ".pem";
            key_path = base + "/coro_tls_test_key_" + std::to_string(::getpid()) + ".pem";
            std::ofstream(cert_path) << kCertPem;
            std::ofstream(key_path) << kKeyPem;
        }
        ~pem_files() {
            std::remove(cert_path.c_str());
            std::remove(key_path.c_str());
        }
    };

    int g_port = 0;

    /// 服务端: 接受两次连接 (第一次是"主机名不符"的负例, 第二次是正例)。
    /// 只服务一次会让正例没有对手 —— 这正是初版跑失败的原因, 与库无关。
    coro::Task<> serve(const pem_files* pem, std::atomic<bool>* clean_shutdown, std::atomic<int>* hands) {
        coro::net::TcpListener listener;
        if (!listener.bind_listen("127.0.0.1", g_port))
            co_return;
        g_port = listener.local_port();
        coro::tls::TlsContext ctx(coro::tls::TlsContext::role::server);
        if (!ctx.use_certificate_file(pem->cert_path) || !ctx.use_private_key_file(pem->key_path)) {
            clean_shutdown->store(false);
            co_return; // 证书加载失败: 让下面的断言去报红
        }

        for (int attempt = 0; attempt < 2; ++attempt) {
            auto conn = co_await listener.accept();
            if (!conn.valid())
                co_return;
            hands->fetch_add(1);
            coro::tls::TlsStream tls(std::move(conn), ctx, "");
            if (!co_await tls.handshake())
                continue; // 负例那次客户端会在验证失败后发 alert 并关闭
            coro::stream_reader rd(tls);
            auto line = co_await rd.read_line();
            if (line.has_value() && *line == "tls-ping") {
                coro::stream_writer wr(tls);
                (void)co_await wr.write_line("tls-pong");
                clean_shutdown->store(co_await tls.shutdown());
            }
        }
        listener.close();
        co_return;
    }

    /// 客户端一次往返。expect_ok=false 用于断言"主机名不符必须被拒绝"。
    coro::Task<> attempt_connect(const pem_files* pem, std::string hostname, bool expect_ok, bool* hs_ok,
                                 bool* got_pong, std::string* err, std::string* version) {
        auto sock = co_await coro::net::TcpStream::connect("127.0.0.1", static_cast<unsigned short>(g_port));
        if (!sock.valid()) {
            *err = "connect failed";
            co_return;
        }
        coro::tls::TlsContext ctx(coro::tls::TlsContext::role::client);
        if (!ctx.load_verify_file(pem->cert_path)) { // 自签名证书自己就是信任锚
            *err = "信任锚加载失败";
            co_return;
        }
        coro::tls::TlsStream tls(std::move(sock), ctx, hostname);
        const bool hs = co_await tls.handshake();
        *hs_ok = hs;
        if (hs != expect_ok) {
            *err = "handshake 结果与期望不符: got " + std::to_string(hs ? 1 : 0) + " expect " +
                   std::to_string(expect_ok ? 1 : 0) + " " + tls.last_error();
            co_return;
        }
        if (!expect_ok)
            co_return;
        *version = tls.negotiated_version();
        coro::stream_writer wr(tls);
        (void)co_await wr.write_line("tls-ping");
        coro::stream_reader rd(tls);
        auto line = co_await rd.read_line();
        if (line.has_value() && *line == "tls-pong") {
            *got_pong = true;
            // 客户端必须主动交换 close_notify, 否则服务端的 shutdown 按合同只能报"未完成"
            if (!co_await tls.shutdown())
                *err = "client shutdown: " + tls.last_error();
        } else {
            *err = "read_line 没拿到 tls-pong";
        }
        co_return;
    }

} // namespace

TEST(TlsTest, HandshakeVerifiesHostnameAndStreamsData) {
    pem_files pem;
    // clean 由服务端协程写、主线程读 -> 原子; hs_ok/pong 只在主协程链上顺序访问,
    // 用普通 bool 即可 (声明成 atomic 会和 bool* 参数类型不匹配)。
    std::atomic<bool> clean{false};
    bool pong = false, hs_ok = false;
    std::atomic<int> hands{0};
    std::string err, version;
    g_port = 0;

    coro::run([&]() -> coro::Task<void> {
        auto svc = coro::spawn(serve(&pem, &clean, &hands));
        for (int i = 0; i < 50 && g_port == 0; ++i)
            co_await coro::sleep(2ms);
        if (g_port == 0) {
            err = "服务端没起来";
            co_return; // 交给外层断言报红
        }

        // 负例: 证书 SAN 里只有 localhost/127.0.0.1, 换别的主机名必须被拒绝
        bool dummy_hs = false, dummy_pong = false;
        std::string dummy_err, dummy_ver;
        co_await attempt_connect(&pem, "wrong.example", /*expect_ok=*/false, &dummy_hs, &dummy_pong, &dummy_err,
                                 &dummy_ver);
        EXPECT_FALSE(dummy_hs) << "主机名不符的连接没被拒绝";

        co_await attempt_connect(&pem, "localhost", /*expect_ok=*/true, &hs_ok, &pong, &err, &version);
        co_await std::move(svc);
    }());

    EXPECT_GT(g_port, 0) << "服务端没起来";
    EXPECT_TRUE(hs_ok) << err;
    EXPECT_TRUE(pong) << err;
    EXPECT_EQ(hands.load(), 2) << "服务端应当接到两次连接";
    EXPECT_TRUE(version == "TLSv1.3" || version == "TLSv1.2") << "实际协商: " << version;
    EXPECT_TRUE(clean) << "close_notify 双向交换没完成: " << err;
}

TEST(TlsTest, ContextRejectsMissingTrustSource) {
    // 安全默认: 信任锚加载失败必须返回 false, 而不是"跳过校验"
    coro::tls::TlsContext ctx(coro::tls::TlsContext::role::client);
    EXPECT_FALSE(ctx.load_verify_file("/nonexistent/ca-bundle.pem"));
    EXPECT_TRUE(ctx.set_min_version(coro::tls::TlsContext::protocol_version::tls12));
}

TEST(TlsTest, ReadWriteBeforeHandshakeFails) {
    pem_files pem;
    coro::tls::TlsContext ctx(coro::tls::TlsContext::role::client);
    ASSERT_TRUE(ctx.load_verify_file(pem.cert_path));
    coro::net::TcpStream sock;
    coro::tls::TlsStream tls(std::move(sock), ctx, "localhost");
    char buf[8];
    int read_rc = -2, write_rc = -2;
    coro::run([&]() -> coro::Task<void> {
        read_rc = co_await tls.read(buf, sizeof(buf));
        write_rc = co_await tls.write("x", 1);
    }());
    EXPECT_EQ(read_rc, -1) << "未握手就 read 必须报错, 不能挂死";
    EXPECT_EQ(write_rc, -1) << "未握手就 write 必须报错, 不能挂死";
}

#endif // CORO_HAS_TLS
