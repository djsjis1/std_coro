# 质量基线与发布说明

本文记录当前代码库的可验证基线，避免把“本机 Debug 通过”误写成跨平台或
生产环境保证。下列历史基线保留原平台背景；当前变更的 CI 绿灯状态
仍以对应提交的 GitHub Actions 运行记录为准，不能由本地结果推断。

## 当前基线

- Windows / MSVC：`coro_tests` 全部通过；测试覆盖取消、Future、跨线程调度、
  TCP、管道、文件、进程、信号、路由和等待组合器。
- 构建目标是 C++20；核心 `coro::coro` 仍为 header-only，Web/llhttp 是可选的
  独立子项目。
- Future 的等待者按所属事件循环路由；Task 收尾、移动和析构按目标 loop 处理。
- 未完成的 Promise 析构会以 `BrokenPromiseError` 完成 Future；动态等待组合器支持
  仅可移动构造、不可默认构造/不可赋值的结果类型。
- `wait_any`、`gather_all`、`gather_void`、`wait_tasks` 的后台 monitor 自持有
  任务容器，调用方取消后不会继续解引用调用方协程帧。
- HTTP 解析器默认限制 URL 8 KiB、头部总量 64 KiB、头部数量 100、请求体 8 MiB；
  超限在增量回调阶段拒绝，而不是先把数据读入内存。
- 静态目录同时做路径段边界检查、`..`/编码点检查和 canonical path 校验，避免
  `/static-secret` 前缀误匹配以及 symlink 逃逸。

## 路由与构建修复增量（基于 78453c2）

- 路由模式清单覆盖通配、参数、静态和根路径；尾斜杠归一，替换不增加条目。
  include 通过精确模式查找导入，拒绝自包含；所有待导入模式预检后才修改父表。
- 新回归覆盖混合通配/参数、嵌套快照、源对象销毁、非法注册、冻结及中间件边界；
  固定随机种子的等价性测试比较直接注册与 include 后分发。
- `coro::web` 是源码树静态库目标，统一编译 Web 实现；独立入口不再重复创建
  web_server，tests-only 按 I/O 能力引入 Web，portable-core 不引入 HTTP 依赖。
- 压测与测试共用完整读写/解析检查工具；确定性替身覆盖短写、失败及 deadline，
  本地假服务器覆盖早关和损坏响应。成功退出 0，失败 1，参数错误 2。
- CTest 新增小规模 Web 压测和精确退出码参数回归；CI 增加独立 Web 构建检查。
  这些是实现及门禁说明，实际执行结果需另行记录，不能当作跨平台通过证明。

## 当前验证边界

- 本机已验证 Windows/MSVC 的 Debug 与 Release 构建、单元测试、Web 自测、压力测试，
  以及安装后 `find_package(coro)` 的 package smoke；Release 压测参考值见
  [性能指南](performance.md)。
- `.github/workflows/ci.yml` 配置了 Ubuntu 24.04 的 GCC 13/Clang 18、Windows 2022
  的 Debug/Release、Linux ASan/UBSan/TSan、格式检查、cppcheck、覆盖率和安装包 smoke。
  这些是门禁配置，不等同于当前提交已经获得所有平台的绿色结果。
- Linux portable-core 路径应使用 `-DCORO_ENABLE_URING=OFF` 单独验证；该模式不宣称
  提供依赖 io_uring 的网络、文件、管道、目录监视和进程模块。

## 构建、测试、安装

```powershell
cmake -S . -B build -DCORO_BUILD_TESTS=ON -DCORO_BUILD_EXAMPLES=OFF
cmake --build build --config Debug --target coro_tests
ctest --test-dir build -C Debug --output-on-failure

cmake --install build --config Release --prefix <install-prefix>
```

安装后消费者可以使用：

```cmake
find_package(coro CONFIG REQUIRED)
target_link_libraries(app PRIVATE coro::coro)
```

`router::router`、`coro::web`、Web/llhttp 和测试依赖仍属于源码树组件；
`find_package(coro)` 只承诺核心 `coro::coro`，应用需要 Web 时应显式加入源码子目录。

## 尚未宣称的能力

Linux 的 io_uring、macOS/其他 Unix、TLS、HTTP/2、流式响应和 sanitizer/TSAN
结果不能由 Windows 构建推断。发布前应在目标平台重新配置、编译和执行测试；尤其
需要验证 liburing 不可用时的配置行为、IOCP/取消竞态以及静态文件权限策略。
Linux inotify 递归实现会初始遍历子目录，维护 wd 到相对路径的映射，
并跟踪新建、移入和重命名的子目录；发布前仍需在 Linux 目标内核上做高频变更与
`IN_Q_OVERFLOW` 压力验证。

Web 示例的 `stop()` 会关闭监听并对全部活动连接执行双向 shutdown，唤醒挂起的
accept/read/write；`wait_all()` 随后等待连接协程在各自 worker 上安全释放句柄。

## 下一批修复路线

1. 现有 Linux CI 配置已安装 liburing，测试源码覆盖 net/fs/pipe/process/fs_watch；
   CI 另有 `CORO_ENABLE_URING=OFF` 的 portable-core 构建。每次发布仍需保留并核对
   对应提交的 GCC/Clang 与 sanitizer 实际结果。
2. inotify 已具备递归目录表、移动 cookie 跨批次关联和新目录动态 watch；
   下一步用 Linux 压力测覆盖 watch 上限、队列溢出与目录树高频移动。
3. Web 已有活动连接注册表并能在 shutdown 时取消挂起 I/O；可配置超时
   已覆盖读空闲、单请求总时限和响应写入；后续可增加每路由的 handler 执行超时。
4. 增加 TLS/HTTP2/WebSocket/大文件流式响应前，先建立基准、ASan/UBSan/TSan 和
   跨平台兼容性门禁；不要把单机吞吐数字当作发布保证。

## 与 Yalantinglibs 的定位差异

本项目目前是一个聚焦协程调度、取消和平台 I/O 的轻量框架；与
[Yalantinglibs](https://github.com/alibaba/yalantinglibs) 的差距主要在生态和
产品化，而不是单个 awaiter 的 API 数量：YLT 还提供 struct_pack/json/xml/yaml/pb、
easylog、coro_rpc、coro_http、coro_io、async_simple，以及完整安装/版本/CI/示例
体系。下一阶段若要接近它，应优先补齐跨平台 CI 与发布包、TLS/HTTP/2/WebSocket、
可观测性/日志、基准门禁和稳定的 Linux io_uring 验证，再扩展协议和序列化组件。
