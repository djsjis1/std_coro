# TLS 收口与生命周期改动：手动验证计划

2026-09-30：本轮只修改代码与文档，按要求未执行构建、测试、lint 或覆盖率。
以下命令供维护者执行，结果记录到 `quality-status.md`，通过之前不标记为已验证。
Linux 命令在仓库根目录运行，使用支持 io_uring 的目标内核；不同场景使用不同构建目录。

## 1. Linux TLS 构建与功能

准备 CMake、C/C++20 编译器、make、Perl；生成演示证书还需要 openssl 命令行工具。
构建依赖使用仓库内 OpenSSL，不用系统 OpenSSL 开发库替代。

```bash
cmake -S . -B build/tls-review -DCMAKE_BUILD_TYPE=Debug \
  -DCORO_ENABLE_TLS=ON -DCORO_REQUIRE_TLS=ON -DCORO_REQUIRE_URING=ON \
  -DCORO_ENABLE_WEB=ON -DCORO_ENABLE_CONCURRENCY_EXT=ON \
  -DCORO_BUILD_EXAMPLES=ON -DCORO_BUILD_TESTS=ON
cmake --build build/tls-review --parallel 2
./build/tls-review/coro_tests --gtest_filter='TlsTest.*:StreamTest.*:DnsTest.*:UnixTest.*:PoolTest.*'
ctest --test-dir build/tls-review --output-on-failure --timeout 120

openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
  -subj '/CN=localhost' -addext 'subjectAltName=DNS:localhost' \
  -keyout build/tls-review/demo-key.pem -out build/tls-review/demo-cert.pem
./build/tls-review/example_tls build/tls-review/demo-cert.pem build/tls-review/demo-key.pem
```

预期：TLS 用例确实被列出并执行，演示输出已验证 localhost、tls-ping/tls-pong
和双方完成 close_notify；超时或任何错误返回非零。证书仅供本地演示。
再用错误 SAN 的证书运行示例，应返回非零，不能通过关闭校验让它成功。

## 2. 安装、迁移目录、真实 TLS 消费

不要只跑原有 package smoke：它只调用 Task，不能证明 OpenSSL 符号实际可链接。
以下消费者显式实例化 TLS context，并重复调用 find_package 检查接口不重复追加。
首次执行使用未占用的目录；重复执行时更换目录后缀。

```bash
cmake --install build/tls-review --prefix "$PWD/build/tls-install-original"
mv build/tls-install-original build/tls-install-moved
mkdir -p build/tls-consumer-src
cat > build/tls-consumer-src/CMakeLists.txt <<'EOF'
cmake_minimum_required(VERSION 3.20)
project(tls_consumer LANGUAGES CXX)
find_package(coro CONFIG REQUIRED COMPONENTS coro)
get_target_property(before coro::coro INTERFACE_LINK_LIBRARIES)
find_package(coro CONFIG REQUIRED COMPONENTS coro)
get_target_property(after coro::coro INTERFACE_LINK_LIBRARIES)
if(NOT "${before}" STREQUAL "${after}")
  message(FATAL_ERROR "Repeated find_package changed the link interface")
endif()
add_executable(tls_consumer main.cpp)
target_link_libraries(tls_consumer PRIVATE coro::coro)
EOF
cat > build/tls-consumer-src/main.cpp <<'EOF'
#include <coro/tls.hpp>
#if !defined(CORO_HAS_TLS) || !CORO_HAS_TLS
#error Expected TLS-enabled installed package
#endif
int main() {
    coro::tls::TlsContext ctx(coro::tls::TlsContext::role::client);
    return ctx.set_min_version(coro::tls::TlsContext::protocol_version::tls12) ? 0 : 1;
}
EOF
cmake -S build/tls-consumer-src -B build/tls-consumer \
  -DCMAKE_PREFIX_PATH="$PWD/build/tls-install-moved"
cmake --build build/tls-consumer --parallel 2
./build/tls-consumer/tls_consumer
cmake -S tests/core_smoke -B build/tls-core-consumer \
  -DCMAKE_PREFIX_PATH="$PWD/build/tls-install-moved"
cmake --build build/tls-core-consumer --parallel 2
./build/tls-core-consumer/coro_core_smoke
```

验收：没有 `/include`、旧前缀或源码树路径泄漏；TLS 消费者完成链接和运行；
纯核心消费者不带 OpenSSL 链接依赖。另用 `CMAKE_INSTALL_LIBDIR=lib64` 重做一次。
将安装副本的 `libcrypto.a` 临时改名后配置新消费者，应在配置期明确报缺少文件。

再用全新目录仅开启 TLS、关闭 EXAMPLES/TESTS，执行普通 build 和 install；
确认 OpenSSL 仍会构建并安装。负例配置分别验证：

| 开关组合 | 预期 |
|---|---|
| REQUIRE_TLS=ON、ENABLE_TLS=OFF | 配置失败 |
| TLS/REQUIRE_TLS=ON、NATIVE_IO=OFF | 配置失败 |
| Linux TLS/REQUIRE_TLS=ON、URING=OFF | 配置失败 |
| TLS=OFF、REQUIRE_TLS=OFF | 不探测 Perl、不构建 OpenSSL、不注册 example_tls |
| Windows TLS=ON | 仍明确失败，当前未支持 Windows OpenSSL 编排 |

## 3. 核心生命周期回归

建议补到已有 Task、EventLoop 和 Web 生命周期用例中，再跑 ASan/UBSan 与 TSan：

- `Task<int>` 与 `Task<void>` 在所属线程取消未启动任务：立即 ready，取结果抛
  CancelledError；随后移动、再次取消或析构，不得双重释放。
- 跨线程通过 owner loop 的 dispatch 发起取消，保证 Task 外壳活到回调结束；
  不以直接跨线程调用 `Task::cancel()` 作为受支持用法。
- EventLoop 的 dispatch 回调抛异常：`is_running()` 恢复 false，当前 loop/task
  恢复到调用前；随后再次运行可执行新投递的回调。
- 同一 loop 嵌套 run 必须报错；独立 loop 的正常退出也恢复原线程上下文。
- Web 有活动连接、空闲连接和无连接三种情况下，在外部 serve 已完成后析构；
  检查 worker join 早于连接表、路由和统计对象析构。
- 回调桥接 Promise 的 README 示例：回调晚于等待任务取消仍不访问局部悬空对象。

## 4. CI 与 Windows 排障计划

本轮未修改工作流。Linux `build-linux.matrix.include` 增加独立的 GCC 13 Release
TLS 行，命名包含 TLS；显式安装 `perl`，传入 ENABLE_TLS/REQUIRE_TLS=ON，原有行
显式 OFF。ccache key、上传 artifact 名也加入 TLS 标识，避免同编译器同配置冲突。
将第 2 节真实 TLS 消费和迁移验证加入 TLS 行；Windows 所有行继续关闭 TLS。
若使用 `codex/` 分支直接推送触发 CI，需要把该前缀加入 push 分支过滤或通过 PR 触发。

Windows 历史红灯先定位，不能用“没有 Testing 目录”直接断言原因：

1. 打开对应提交的 Actions → Windows Debug/Release → 失败步骤与 annotations。
2. 保存 Configure、Build、Run unit tests 和 Report ctest console output 的输出。
   区分前序构建失败导致 skipped、找不到 ctest、参数错误、进程启动失败和测试失败。
3. 在 VS Developer PowerShell 以 TLS=OFF 复现两配置：

```powershell
cmake -S . -B build/windows-review -DCORO_ENABLE_TLS=OFF -DCORO_ENABLE_WEB=ON -DCORO_BUILD_TESTS=ON -DCORO_BUILD_EXAMPLES=ON
cmake --build build/windows-review --config Debug
if ($LASTEXITCODE -ne 0) { throw 'Debug build failed' }
ctest --test-dir build/windows-review -C Debug --output-on-failure --timeout 120
cmake --build build/windows-review --config Release
if ($LASTEXITCODE -ne 0) { throw 'Release build failed' }
ctest --test-dir build/windows-review -C Release --output-on-failure --timeout 120
```

Windows TLS 后续实现需单独处理 nmake/JOM、VC-WIN64A/VC-WIN32/ARM64 选择、
Debug/Release 输出隔离、MD/MDd CRT 对齐、libssl.lib/libcrypto.lib 的安装和系统链接库。
`tests/test_tls.cpp` 当前使用 unistd/getpid，也需移植。完成这些前不要删 FATAL 或开启 Windows TLS CI。

## 5. lint 与覆盖率基线

格式检查沿用 clang-format 18，但加入 `.ipp`（现有工作流遗漏 TLS 实现文件）：

```bash
find include router examples tests Web/src Web/main.cpp main.cpp \
  \( -name '*.cpp' -o -name '*.hpp' -o -name '*.h' -o -name '*.ipp' \) -print0 | \
  xargs -0 -r clang-format-18 --dry-run --Werror
```

覆盖率单独新建 Debug 目录，开启 TLS/REQUIRE_TLS、URING/REQUIRE_URING、WEB、
CONCURRENCY_EXT 和 TESTS，使用 GCC 13，CXX flags 为 `--coverage -O0 -g`。
构建并执行完整测试后按现有 coverage 作业的 lcov/gcov-13/genhtml 命令生成报告；
报告须单列 stream.hpp、dns.hpp、unix.hpp、pool.hpp、tls.hpp 和 src/tls.cpp。
同时捕获运行前的 initial coverage 并与运行后数据合并，避免未执行文件从分母消失。
记录提交号、编译器、内核、实际用例数、跳过用例和行/分支覆盖率；不能只贴总百分比。
