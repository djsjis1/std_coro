# 构建选项与模块解耦约定

> 面向改 `CMakeLists.txt` / 新增模块 / 新增第三方依赖的人。
> 目标只有一条：**关掉的模块不参与配置、编译、链接**，任何可选子系统都不能
> 把核心牵连进依赖链。运行时架构见 [architecture.md](architecture.md)。

## 1. 选项矩阵


| 选项                              | 默认                              | 影响                                                                                 |
| --------------------------------- | --------------------------------- | ------------------------------------------------------------------------------------ |
| `CORO_ENABLE_NATIVE_IO`           | Windows/Linux`ON`，其他平台 `OFF` | 原生 I/O 总开关；关闭时核心走`CVEventSource`，网络/文件/管道/监视/进程模块全部不构建 |
| `CORO_ENABLE_URING`               | `ON`                              | Linux 的 io_uring 后端（依赖`NATIVE_IO`）。关闭即纯协程核心                          |
| `CORO_REQUIRE_URING`              | `OFF`                             | `ON` 时 io_uring 后端不可用直接配置失败，禁止静默降级；`OFF` 时只警告                |
| `CORO_ENABLE_WEB`                 | `OFF`                             | `coro::web` 静态库、`web_server` 示例与 Web 层测试的唯一开关                         |
| `CORO_BUILD_TESTS`                | `OFF`                             | googletest 与`coro_tests`                                                            |
| `CORO_BUILD_EXAMPLES`             | `OFF`                             | `examples/`、`tcp_udp` 示例、根 `main.cpp` 练习场                                    |
| `CORO_BUILD_BENCHMARKS`           | `OFF`                             | `coro_stress` 压测程序（`EXAMPLES=ON` 时也会带上）                                   |
| `CORO_GCC13_COROUTINE_WORKAROUND` | `ON`                              | GCC 13 上对协程翻译单元加`-fno-tree-slp-vectorize`                                   |

**开发验证目标默认全 OFF 是刻意的**：本库以 `add_subdirectory` 或 `find_package`
被消费时，不应该附带把测试框架、示例和 Web 服务一起配置进来。

## 2. 能力宏：编译期只有一个

`coro` 目标向消费方导出**唯一**的能力宏：

```
CORO_HAS_URING=0|1        # 由 $<BOOL:${CORO_HAS_URING}> 单点派生
```

头文件里的平台守卫一律写成：

```cpp
#if defined(_WIN32) || (defined(__linux__) && defined(CORO_HAS_URING) && CORO_HAS_URING)
```

注意两点，都有过教训：

1. **不要**写 `#if !defined(CORO_HAS_URING) || CORO_HAS_URING`。宏被显式定义为 `0`
   时 `defined(...)` 为真，该分支照样编译，导致 `URING=OFF` 的配置编译失败或
   测试无法跳过。必须 `defined(MACRO) && MACRO`。
2. `CORO_HAS_NET` / `CORO_HAS_FS` / `CORO_HAS_PIPE` / `CORO_HAS_SIGNAL` /
   `CORO_HAS_FSWATCH` / `CORO_HAS_PROCESS` **只是 CMake 变量**（用来决定哪些
   target、哪些测试源文件参与构建），不会作为编译宏传给 C++。写头文件守卫时
   不要引用它们。

CMake 侧的派生关系（`CORO_ENABLE_NATIVE_IO` 为总门）：

```
WIN32  → NET/FS/PIPE/SIGNAL/FSWATCH/PROCESS 全 ON
Linux  → NET/FS/PIPE/FSWATCH/PROCESS = CORO_HAS_URING,  SIGNAL = ON
其他    → 全 OFF
Web    → 需要 NET && FS, 由 target 属性 CORO_WEB_IO_AVAILABLE 表达
```

`CORO_ENABLE_WEB=ON` 而能力不足时是**配置期 FATAL_ERROR**，不是静默跳过——
避免"以为开了其实没开"。属性要用 `get_target_property` 读，直接
`if(NOT CORO_WEB_IO_AVAILABLE)` 判断未定义变量恒真。

## 3. 模块依赖方向

```
core (task/scheduler/sync/gather/...)        ← 零第三方依赖, 任何平台可构建
  └── IO 模块 (net/fs/pipe/fs_watch/process) ← 仅依赖 IOCP 或 io_uring
        └── tcp_udp, router                   ← router 实际是独立 header 库, 不依赖 IO
              └── coro::web (Web/src)          ← 显式 CORO_ENABLE_WEB
                    └── thirdparty/http        ← HTTP 协议解析 (llhttp)
                          └── Web (web_server 示例)
```

规则：

- 下层**不得**引用上层 target 或上层测试源文件；核心测试不得因为示例开关而
  链接 Web/HTTP。
- 新增可选子系统时，同时提供：CMake `option` + 编译宏（若头文件需要）+
  关闭时的负例验证（配置期必须完全不出现该模块的 target）。
- `Web` 支持 `cmake -S Web` 独立构建（`CORO_WEB_BOOTSTRAP` 路径自动引入根工程），
  改动根 CMake 时必须保持这条路径可用。

### 3.1 消费者目标：`coro::core` 与 `coro::coro`

| 目标 | 携带内容 | 适用消费者 |
|---|---|---|
| `coro::core` | include 路径、`cxx_std_20`、MSVC/GCC 协程方言、GCC13 规避、`pthread` | 只用 Task/调度/同步/定时器；**不**继承 `liburing`、`ws2_32` |
| `coro::coro` | `coro::core` + 原生 I/O 后端（Linux `coro::uring`、Windows `ws2_32`）与 `CORO_HAS_URING` | 使用 net/fs/pipe/process，或沿用旧代码 |

- 依赖方向单向 `coro::coro → coro::core`，禁止反向；核心头不得 include 后端头
  （`coro.hpp` 不含 IO 头，后端由 `event_loop.hpp` 按宏条件引入）。
- 安装导出名由 `EXPORT_NAME` 显式指定：`add_library(ns::name ALIAS)` 不参与
  `install(EXPORT)` 命名，不设则消费者拿到的是 `coro::coro_core`。
- 回归门禁：`tests/core_smoke` 在**带 io_uring 的安装包**上配置 `coro::core` 消费者，
  核心若把 `uring`/`ws2_32` 转交给消费者则配置期直接 `FATAL_ERROR`；Linux 与
  Windows 的 "Verify installed package" 步骤均已纳入。

## 4. 第三方源码

全部放在仓库既有的 `thirdparty/` 下，随仓库提供固定版本源码，**不查找系统
预装库、不自动下载**，只在对应模块开启时才编译。当前清单：


| 目录                                    | 上游         | 用在哪                       | 引入方式                                                          |
| --------------------------------------- | ------------ | ---------------------------- | ----------------------------------------------------------------- |
| `thirdparty/googletest-main`            | GoogleTest   | `coro_tests`                 | `CORO_BUILD_TESTS=ON` 时 `add_subdirectory(... EXCLUDE_FROM_ALL)` |
| `thirdparty/http/llhttp-release-v9.3.0` | llhttp 9.3.0 | `thirdparty/http` 的解析后端 | `add_subdirectory(llhttp-release-v9.3.0)`                         |
| `thirdparty/liburing`                   | liburing 2.9 | Linux io_uring 事件源        | `add_subdirectory(thirdparty/liburing)`，构建静态库 `uring`       |

`thirdparty/liburing/CMakeLists.txt` 是**包装层**（不改上游源码）：上游的
`liburing/compat.h` 与 `liburing/io_uring_version.h` 本是 `configure` 的生成物，
包装层用等价的内核头探测把它们生成到构建目录，因此

- 源码树零污染，老内核头环境自动降级为兼容定义；
- 不需要 autoconf，也不需要外部 `make` 步骤；
- `CORO_REQUIRE_URING=ON` 的语义变为"仓库内源码必须存在"，缺失且未强制时
  警告并降级为纯协程核心。

新增第三方库时照此办理：上游源码原样入目录 + 本仓库自写的 `CMakeLists.txt`
包装层 + 文件头注明上游 tag/commit 与收录范围。**不要**顺手重命名或搬移已有
目录（如 `thirdparty` → `third`）：纯结构性改名没有解耦收益，却会牵动
CI、文档与所有引用点。

安装导出注意：静态库 target 若被 `coro` 以 `INTERFACE` 链接，**不能**用
`add_subdirectory(... EXCLUDE_FROM_ALL)` 引入——`EXCLUDE_FROM_ALL` 会让该子目录
里的 `install()` 规则整体失效，导出的 `coroTargets.cmake` 就引用到未安装的文件。
`uring` 因此直接 `add_subdirectory`，并复用 `coroTargets` 导出集，消费端拿到
`coro::uring`，`find_package(coro)` 后即可直接构建运行。

## 5. Presets


| preset   | 内容                                | 产物目录       |
| -------- | ----------------------------------- | -------------- |
| `core`   | `NATIVE_IO=OFF`，纯协程核心         | `build/core`   |
| `native` | 核心 + io_uring/IOCP 模块（无测试） | `build/native` |
| `web`    | `native` + `CORO_ENABLE_WEB=ON`     | `build/web`    |
| `dev`    | 全特性 + 测试 + 示例 + 压测         | `build/dev`    |

前三个 configure preset 仍继承“测试/示例/压测全关”，只用于验证模块开关本身；
要跑单测就用 `dev`，或临时叠加 `-DCORO_BUILD_TESTS=ON`。

配套还有 `buildPresets`（core/native/web/dev，均 `jobs: 2`，低内存机器友好）
与 `testPresets`（仅 dev）：

```bash
cmake --preset dev
cmake --build --preset dev
ctest --preset dev            # 等价于 ctest --test-dir build/dev --output-on-failure
```

注意 `--build --preset` / `--test-preset` 需要 json 里有对应的
`buildPresets` / `testPresets` 条目——只定义 `configurePresets` 时报
“No such build preset”，因此新增 configure preset 时要同步补上。

## 6. 提交前必须跑过的组合

单一配置通过不代表 CI 矩阵通过（历史上出过两次）。改动构建脚本后至少验证：

1. `-DCORO_ENABLE_URING=OFF -DCORO_BUILD_TESTS=ON`：核心可构建，I/O 测试被跳过；
2. 默认（`NATIVE_IO=ON`）+ `TESTS=ON`：全量单测；
3. `-DCORO_ENABLE_WEB=ON`：Web 库/示例/Web 层测试；`URING=OFF` 时该组合必须配置期报错；
4. `cmake -S Web`：独立构建路径；
5. `--install` + `tests/package_smoke` 消费安装树；
6. CI 四个 job 的 `-D` 组合与 `ctest` 注册保持一致（Web 自测是通过 `add_test`
   挂在 `ctest` 下，不是独立步骤）。
