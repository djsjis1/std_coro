# 第三方依赖审查表（D03）

本表只记录**可核验的消费关系**，不把"目录存在"当成"依赖存在"。判断顺序：
CMake target → `#include` → 链接 → 运行期资源 → 安装导出。

基线：`thirdparty/` 下共 9 个目录。检索方式：仓库内 `add_subdirectory` / `target_link_libraries` /
`#include` / 安装导出文件中的引用（排除 `thirdparty/` 内部自身文件）。

| 目录 | 消费点 | 结论 |
|---|---|---|
| `googletest-main` | 根 `CMakeLists.txt`：`add_subdirectory(... EXCLUDE_FROM_ALL)`，仅 `CORO_BUILD_TESTS=ON`；`GTest::gtest_main` 链接到 `coro_tests` | 测试专用依赖，保留 |
| `liburing` | 根 `CMakeLists.txt`：`CORO_ENABLE_URING` 门控；`coro_io` 链接；Linux 原生 I/O 后端 | Linux 原生 I/O 必需，保留 |
| `openssl` | 根 `CMakeLists.txt`：`CORO_ENABLE_TLS` 门控；`coro_tls` 链接 | 可选 TLS 后端，保留 |
| `http`（llhttp + 自维护包装） | `add_subdirectory(thirdparty/http)`；`coro_http_client`、Web 层链接 `http::http` | **第一方维护范围**：纳入格式化、静态分析、覆盖率与回归测试 |
| `HFSM2-2.12.1` | 未找到任何 CMake / `#include` / 链接 / 安装消费点 | 删除候选（需单独变更评审） |
| `lua-5.4.7` | 同上 | 删除候选 |
| `nlohmann` | 同上 | 删除候选 |
| `sqlite` | 同上（仅教程文档正文提到 "SQLite" 一词，不是代码依赖） | 删除候选 |
| `log` | 同上 | 删除候选 |

## 删除门槛（本表不构成删除授权）

1. 删除前需要在**全部支持配置**下重新验证：`core` / `native` / `web` / `client` / `tls-client`，
   以及安装后独立消费的 `find_package` smoke。
2. 删除动作必须与功能修复分成不同变更，避免"修 bug 顺手删依赖"导致无法二分定位。
3. 保留许可证与上游来源信息；依赖升级与本地逻辑修复分开。
4. 任何"仅凭目录名或一次搜索未命中"的判断都不充分；必须同时检查生成目录与安装导出。

## 目前不做的事

- 不删除上述任何目录：本轮只产出依赖表与候选清单。
- 不迁移 `thirdparty/http` 的路径（迁移会让逻辑修复与构建改动混在一个批次里）。
