# Coro 优化与扩展路线图 — 实施手册

> 本文档是全项目后续演进的**唯一权威计划**，定位是「教学级实施手册」：
> 每个任务都写清楚 —— 改哪个文件、插在哪个位置、代码怎么写（可照抄的骨架 +
> 逐行讲解）、为什么必须这么写（架构约束）、改完用什么命令验证。
> 目标读者：第一次接触本库、但要按计划动手改库的人。
>
> 每完成一项把 `- [ ]` 勾成 `- [x]`；实施中发现锚点漂移（行号变化等）随手修正本文档。
>
> 制定日期：2026-09-18 ｜ 最近核对：2026-09-21 ｜ 状态：**阶段 0 部分完成，需重新核对**
>
> 说明：本手册是演进计划，不是自动生成的能力清单。当前代码已经包含 UDP、
> Linux process 条件编译和多项 Web/生命周期修复；因此下面的历史复选框和行号
> 不能直接当作未实现证明。发布前请以 [质量基线](quality-status.md)、测试结果和
> 当前符号搜索为准。

---

## 0. 如何使用本手册（贡献者工作流）

```
① 读 §1 总原则 + §3 通用方法论（改库前必须理解的部分）
② 在 §4-§8 找到要做的任务，读完整个任务条目再动手
③ 按任务内的「步骤 N」顺序做，每步跑一次「验证」命令，绿了再进下一步
④ 收尾过 §9 横切门禁自查清单
⑤ 提 PR（标题带任务编号，如 "1.1 UDP: UdpSocket 双平台实现"）
```

**开工前必会的命令**（仓库根目录执行）：

```bash
# 配置 + 构建（单配置生成器默认 Release）
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j

# 跑全部单元测试（tests/test_*.cpp 自动被 GLOB 收编进 coro_tests 单目标）
ctest --test-dir build --output-on-failure

# 只看某个主题的用例（gtest 过滤）
./build/coro_tests --gtest_filter='Net*'

# 高并发压测 / 网络基准（独立程序，不在 ctest 里）
./build/coro_stress
./build/bench_net          # 0.2 完成后存在
```

---

## 1. 总原则（所有阶段的硬约束）

1. **零依赖不变**：核心库保持 header-only、无第三方依赖（llhttp 只属于 Web/ 子项目）。
2. **纯增量优先**：阶段 0-2 不触碰核心调度层（`task.hpp` / `event_loop.hpp` /
   事件源）；阶段 3 是唯一允许动核心的实验区，且必须有基准数据护航。
3. **数据驱动**：没有基准就没有优化发言权——阶段 3 的每一项都以 0.2 的
   `bench_net` 基线做前后对比，无收益即回退。
4. **双平台显式对称**：新 IO 模块在 `#ifdef _WIN32` / `#ifdef __linux__`
   两段里各写一份，**不抽公共 awaiter 基类**（理由见 §12-1）。
5. **每阶段收尾必须过横切门禁**（§9），文档与代码同 PR。

---

## 2. 现状基线（缺口核实清单）

以下锚点已于 2026-09-18 逐条核实（之后行号可能漂移，以符号搜索为准）：

| # | 领域 | 现状 | 代码/文档锚点 |
|---|---|---|---|
| 1 | 网络 | TCP + UDP，当前地址 API 仍以 IPv4 为主；暂无 DNS/IPv6 抽象 | `include/coro/net.hpp`（双平台实现） |
| 2 | Web 层 | 无中间件、无 chunked 生成、响应体全量缓存 | `docs/web-framework.md` §已知限制（~251-261） |
| 3 | 平台 | Linux 在检测到 io_uring 时启用 process；portable-core 和 macOS 不提供该 IO 模块 | `CMakeLists.txt`；`.github/workflows/ci.yml` |
| 4 | 性能 | 网络吞吐/延迟、每连接内存无基准 | `docs/performance.md` §3「未覆盖」清单（95-99） |

**核实中发现的补充事实**（比原始设想更精确，实施时以此为准）：

- `process.hpp` **本体已是双平台实现**；当前 CMake 在 Linux 检测到 io_uring
  时启用 process，测试守卫也按 `CORO_HAS_URING` 条件编译，示例由能力开关门控。
  因此 0.1 不再是“解禁代码”的待办，而是应在 GCC/Clang CI 上持续验证的基线。
- `router::route()` 的静态文件分支**已经异步化**（`router.h:142`
  `co_await coro::fs::read_all`）；web-framework.md 已知限制里「file() 同步读」
  仅剩 `http_response::file()` 工厂函数（`http_types.h:63`）这一处。
- Web 写回路径：`web_server.cpp:92` `handle_connection` 解析请求 →
  `:148` 附近 `resp.build()` 全量序列化 → 写回。阶段 2.2 的分叉点就在这里。
- CI 的压测步骤位于 `.github/workflows/ci.yml` 的 Linux/Windows Release job；
  `bench_net` 若加入，应沿用同一 job 结构。
- `docs/performance.md` 已同时列出 Visual Studio 与 Ninja/Make 的压力测试运行方式。

---

## 3. 修改这个库的通用方法论（改任何东西之前先读这节）

### 3.1 分层架构：哪里能改、哪里不能碰

```
L4  应用层        Web/ (http_types → router → web_server)      ← 阶段 2 主战场
L3  IO 模块       net.hpp / fs.hpp / pipe.hpp / process.hpp    ← 阶段 1 主战场
L2  IO 错误通道   io.hpp (set_error 双通道: errno + last_error)
L1  平台事件源    iocp_event_source.hpp / uring_event_source.hpp
L0  核心         task.hpp / event_loop.hpp / scheduler.hpp ... ← 只允许阶段 3 实验
```

规则：**上层可以随意引用下层，下层永远不知道上层存在**。所以加 UDP（L3 内部）
不需要动 L0-L2 的任何一行；加中间件（L4）不需要动 L3 以下。反过来，
如果你发现改 L3 必须动 L0 才能实现，说明设计错了，停下来重新想。

### 3.2 参考范本：`TcpStream::read` 的 awaiter 是怎么工作的

这是本库一切新 awaiter 的**模仿对象**（Linux 版，`net.hpp:550-607`，简化）。
写任何新的异步操作前，先能逐行讲清楚这段代码：

```cpp
struct read_awaiter {
    // ① 成员 = 操作期间必须存活的一切。awaiter 住在协程帧里，
    //    协程挂起即存活 —— 这是 net.hpp 头部声明的生命周期约定，
    //    也是「缓冲为什么放成员而不放栈上」的原因。
    TcpStream* stream;
    char* buf;
    size_t len;
    detail::uring_op op;                        // 通用操作块: continuation + result
    net::UringEventSource* uring_ = nullptr;     // 非空 = 已提交过, 析构时要反注册

    ~read_awaiter() {
        if (uring_)
            uring_->untrack_op(&op);             // ② 板斧之二: 析构反注册
    }

    bool await_ready() noexcept { return false; } // 恒 false: 总要提交一次 SQE

    // ③ 板斧之一: 静态取消钩子。Task::cancel 会通过函数指针找到它。
    //    关键语义: 取消必须产生一个 CQE 来唤醒协程,
    //    保证 op 在协程帧销毁前被消费 (否则悬垂)。
    static void cancel_op(void* self) {
        auto* aw = static_cast<read_awaiter*>(self);
        if (auto* u = current_uring()) {
            io_uring_sqe* sqe = io_uring_get_sqe(u->handle());
            if (sqe) {
                io_uring_prep_cancel(sqe, &aw->op, 0);
                io_uring_submit(u->handle());
            }
        }
    }

    void await_suspend(std::coroutine_handle<> h) {
        op.continuation = h;                     // 记录恢复点
        if (auto* u = current_uring()) {
            io_uring_sqe* sqe = io_uring_get_sqe(u->handle());
            if (!sqe) {                          // 提交队列满: 立即失败, 手动恢复
                op.result = -ENOBUFS;
                EventLoop::get().schedule(h);
                return;
            }
            io_uring_prep_recv(sqe, stream->fd_, buf, len, 0);  // ← 换操作只换这一行
            uring_submit_op(u, u->handle(), &op, sqe);
            uring_ = u;
            u->track_op(&op);                    // 注册: 取消钩子能找到 op
        } else {
            op.result = -ENOTSUP;                // 没有 uring 事件源: 立即失败
            EventLoop::get().schedule(h);
        }
    }

    int await_resume() {
        if (op.error) {
            io::set_error(op.error);             // ④ 板斧之三: 错误双通道
            return -1;                           //    errno=转换值, io::last_error()=原生码
        }
        return op.result;                        // cqe->res: 字节数 / 0=对端关闭 / 负 errno
    }
};
```

Windows 版（`net.hpp:127-173`）结构完全相同，只有三处平台差异：
提交用 `WSARecv`（缓冲装进 `WSABUF`）、完成有**三形态**（同步成功 / 立即失败 /
`WSA_IO_PENDING`——代码里对三种都做了处理）、取消用 `CancelIoEx`。
**记住这个对应关系，写 UDP 时就是换掉「提交那一行」+ 把地址缓冲挂进成员。**

### 3.3 新增一个 IO 模块的标准 7 步（ checklist ）

1. **头文件**：`include/coro/xxx.hpp`，进 `namespace coro`；
   net 的扩展直接加在 `net.hpp` 的 `namespace coro::net` 内部则免新建。
2. **能力开关**：根 `CMakeLists.txt:26-47` 增 `CORO_HAS_XXX` + 平台条件，
   目标（examples/tests）用它门控。
3. **双平台对称实现**：`#ifdef _WIN32` 一段 / `#ifdef __linux__` 一段，
   类名与公开 API 完全一致，仅提交机制不同。
4. **取消安全三板斧**（§3.2 的 ②③④，每个 awaiter 强制）：
   `cancel_op` 静态钩子 + 析构 `untrack_op`（Linux）/完成包消费保证（Windows）
   + `io::set_error` 双通道。
5. **测试**：`tests/test_xxx.cpp`，文件开头照抄 `test_net.cpp:2` 的平台守卫
   `#if defined(_WIN32) || defined(__linux__)`；被 `CMakeLists.txt:124` 的
   GLOB 自动收编，**不需要改 CMake**。
6. **文档**：同 PR 更新 `docs/api-reference.md`（新节）、
   `docs/architecture.md`（代码规模表）、受影响 tutorial。
7. **fire-and-forget 协程**一律「命名协程 + `start()+detach()`」
   （`architecture.md` §11.5），禁止裸 lambda Task 泄漏。

### 3.4 新增 Web 层功能的标准动作

- 改动只落在 `Web/src/`（`http_types.h/.cpp`、`router.h`、`web_server.h/.cpp`）；
- 测试加进 `tests/test_router.cpp`（已存在于 GLOB，自动编译）；
- 涉及 HTTP 语义的改动，除 gtest 外必须加 `curl` 实测（`web_server --selftest`
  起服务后 `curl --raw` 看帧格式）； 
- 同步更新 `docs/web-framework.md` 的「已知限制」清单。

---

## 4. 阶段 0：清障与基线（低风险，立即执行）

### 0.1 解锁 Linux process 模块

> **目标**：让 Linux 上 GCC≥13 / Clang 的 CI 矩阵编译并运行 process 用例。
> **背景**：`process.hpp` 本体已是双平台代码，当初只因 GCC 12 的协程 ICE
> （结构化绑定 + co_await 编译器内部错误）整体禁用。

> **当前状态（2026-09-21）**：CMake 能力开关和测试条件编译已经在当前代码中
> 接线；本节剩余价值是核对目标提交的 Linux GCC/Clang CI 结果，不要重复做已落地的
> CMake/守卫改动。

- [ ] **步骤 1｜确认编译器版本**

  ```bash
  g++ --version | head -1     # 需要 ≥ 13
  clang++ --version | head -1  # 任意近版均可（Clang 无此 ICE）
  ```

- [ ] **步骤 2｜验证 ICE 已修复**（不改任何文件，先证明问题不存在）：
  临时把 `tests/test_process.cpp:2` 的 `#ifdef _WIN32` 复制一份改成
  `#if 1` 存盘，单独编译该翻译单元：

  ```bash
  g++ -std=c++20 -fsyntax-only -Iinclude -Ithirdparty/googletest-main/googletest/include tests/test_process.cpp
  ```

  能过语法检查 → 继续步骤 3；仍报 internal compiler error → **本任务整体挂起**，
  在 CMake 注释记录编译器版本，恢复守卫，其他阶段不受影响。

- [ ] **步骤 3｜CMake 解禁**（`CMakeLists.txt:40-47`，Linux 分支追加）：

  ```cmake
  elseif(CMAKE_SYSTEM_NAME STREQUAL "Linux")
      set(CORO_HAS_NET ON)
      ...（现有五行不动）
      # process: GCC 12 coroutine ICE 已在 13 修复; Clang 无此问题
      if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL 13)
          set(CORO_HAS_PROCESS ON)
      elseif(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
          set(CORO_HAS_PROCESS ON)
      endif()
  ```

- [ ] **步骤 4｜测试守卫改造**：`tests/test_process.cpp:2` 改为
  `#if defined(_WIN32) || defined(__linux__)`（与 `test_net.cpp:2` 同款写法）；
  然后逐用例检查子进程命令是否平台写死——Windows 常见 `cmd /c echo`、
  Linux 应为 `sh -c echo`。写法参考：

  ```cpp
  #ifdef _WIN32
      const char* cmd[] = {"cmd", "/c", "echo", "hi", nullptr};
  #else
      const char* cmd[] = {"echo", "hi", nullptr};
  #endif
  ```

- [ ] **步骤 5｜验证**：

  ```bash
  cmake --build build -j && ctest --test-dir build --output-on-failure
  ```

  process 用例混在 `coro_tests` 单目标里（没有独立 `-R Process` 过滤项），
  验收看输出中 `Process.*` 用例全绿，且原有用例无一回归。

- [ ] **步骤 6｜CI**：`ci.yml` Linux 矩阵**无需改 yaml**——解锁后自动纳入编译+测试。

**验收标准**：Linux 下 GCC 13 与 Clang 18 全矩阵（Debug/Release + sanitizers）
process 用例通过，Windows 侧无任何行为变化。

### 0.2 网络基准补位

> **目标**：填补 `performance.md` §3 的网络空白，给阶段 3 提供对比基线。
> **产出**：`tests/bench_net.cpp`（独立可执行，不进 ctest）+ 文档表格。

- [ ] **步骤 1｜写 `tests/bench_net.cpp`**。风格对照 `tests/stress.cpp`：
  `Timer` RAII 计时、命名协程函数、`[RUN]/[OK]` 输出。骨架如下（可直接照抄再填）：

  ```cpp
  // bench_net.cpp — 网络基准: echo 吞吐 / 往返延迟 / 并发建连 / 每连接内存
  // 独立程序, 不进 ctest (文件名不匹配 tests/test_*.cpp 的 GLOB)
  #include <coro/coro.hpp>
  #include <coro/net.hpp>

  #include <algorithm>
  #include <atomic>
  #include <chrono>
  #include <cstdio>
  #include <fstream>
  #include <vector>

  using namespace std::chrono_literals;
  namespace {
      struct Timer { /* 照抄 stress.cpp:15-23 */ };

      // 每连接内存: 读 /proc/self/status 的 VmRSS 行 (KB)
      size_t vm_rss_kb() {
          std::ifstream f("/proc/self/status");
          std::string line;
          while (std::getline(f, line))
              if (line.rfind("VmRSS:", 0) == 0)
                  return (size_t)strtoull(line.c_str() + 6, nullptr, 10);
          return 0;
      }

      constexpr unsigned short PORT = 19100;   // 与 tests/ 现有用例同段, 避开常用口

      // ---- echo 服务器: 收到什么写回什么, 直到对端关闭 ----
      coro::Task<> echo_conn(coro::net::TcpStream conn, std::atomic<int64_t>* served) {
          // 反模式示范 (performance.md §4.4): 大缓冲用 unique_ptr, 不进协程帧
          auto buf = std::make_unique<char[]>(64 * 1024);
          while (true) {
              int n = co_await conn.read(buf.get(), 64 * 1024);
              if (n <= 0) break;                       // 0=对端关闭, -1=错误
              int off = 0;
              while (off < n) {                        // write 可能部分写, 循环补齐
                  int w = co_await conn.write(buf.get() + off, (size_t)(n - off));
                  if (w <= 0) co_return;
                  off += w;
              }
          }
          if (served) ++*served;
      }

      coro::Task<> echo_server(coro::net::TcpListener* l, std::atomic<bool>* stop) {
          while (!stop->load()) {
              auto conn = co_await l->accept();
              if (!conn.valid()) co_return;            // listener 关闭
              coro::spawn(echo_conn(std::move(conn), nullptr)).detach();
          }
      }

      // ---- 场景 1: echo 吞吐 (4 客户端 × 各 256MB, 64KB 块) ----
      coro::Task<> throughput_client(int64_t* total_bytes) { /* 写-读回显循环, 累计字节数 */ }

      // ---- 场景 2: 往返延迟 (单连接 ping-pong 一万次) ----
      coro::Task<> latency_client(std::vector<double>* samples_us) {
          // 每轮: steady_clock 打点 → write(8B) → read 回显 → 再打点, 差值入 samples
      }

      // ---- 场景 3: 一万并发连接建立 + 首字节回显 ----
      // 一万个 connect 协程 spawn 后 start(), 计时到全部完成, 统计成功数

      // ---- 场景 4: 每连接内存 ----
      // 建万连接前后各读一次 vm_rss_kb(), 差分 ÷ 连接数
      // Windows 分支: GetProcessMemoryInfo (条件编译跳过亦可)

      coro::Task<> run_all() { /* 依序驱动四个场景, 打印结果 */ }
  } // namespace

  int main() {
      coro::run(run_all());   // 启动全部基准场景
      return 0;
  }
  ```

  写客户端协程时参考 `tests/test_net.cpp` 的 client_side / server_side
  配对写法（spawn 两个任务 + `co_await std::move(t)` 汇合）。
  P50/P99 用 `std::nth_element` 排序取样，不要全排序。

- [ ] **步骤 2｜CMake 加目标**（`CMakeLists.txt:64-67` 的 `CORO_HAS_NET` 块内）：

  ```cmake
      # 网络基准 (独立程序, 不计入常规 ctest — 同 coro_stress)
      add_executable(bench_net tests/bench_net.cpp)
      target_link_libraries(bench_net PRIVATE coro::coro)
  ```

- [ ] **步骤 3｜本机验证**：

  ```bash
  cmake --build build --target bench_net -j && ./build/bench_net
  ```

  四场景出数、无崩溃、重复跑数字稳定（±10% 内）。

- [ ] **步骤 4｜CI 接线**（`ci.yml`）：Linux job 在 134-137 行 stress 步骤后加：

  ```yaml
      - name: Run network bench (Release only)
        if: matrix.build_type == 'Release'
        run: ./build/bench_net
  ```

  Windows job（194-197 行后）同款，路径 `build\Release\bench_net.exe`。

- [ ] **步骤 5｜文档落表**：`docs/performance.md` §2 末尾加「2.x 网络基准
  （bench_net）」小节：环境说明（CPU/编译器/构建类型）+ 四场景表格；
  §3 未覆盖清单**删去**「网络吞吐/延迟」与「每连接内存」两条，保留文件 IO 一条；
  §2 构建命令改成 Windows/Linux 双写（修掉 33-34 行的 exe 硬编码）。

**验收标准**：本机 Release 跑通四场景；`docs/performance.md` 有实测表格；
此数据即阶段 3 的对比基线（连同 `coro_stress` 场景 3/5）。

### 0.3 路线图文档化（本文档）

- [x] 建立 `docs/roadmap.md`（本文件）。
- [x] `docs/README.md` §参考 增加路线图入口链接。
- [ ] 根 `README.md` 末尾加一行指向本文档（可选，随下次 README 改动顺带）。

---

## 5. 阶段 1：网络栈扩展（纯增量，不触碰核心）

> 前置：阶段 0 完成。动手前重读 §3.2 范本——两个任务的核心动作都是
> 「复制 TcpStream 的 awaiter 骨架，换掉提交那一行，把地址缓冲挂进成员」。

### 1.1 UDP 支持

> **目标**：`coro::net` 内新增 `UdpSocket`，API 与 TcpStream/TcpListener 对称。
> **关键认知**：UDP 与 TCP 在这个库里只差两件事——① socket 类型
> `SOCK_DGRAM`；② 无连接模式下每次收发都要携带/返回对端地址。

- [ ] **步骤 1｜定 API**（写进 `net.hpp` 头注释，先想清楚再写实现）：

  ```cpp
  class UdpSocket {
    public:
      bool bind(const char* ip, unsigned short port);   // 同步一次性, 同 bind_listen 风格
      // 无连接收发。收必须返回对端地址 → 返回结构体而不是裸 int:
      struct recv_from_result {
          int n = -1;               // <0 错误(io::set_error 已设), 0=空数据报, >0 字节数
          char ip[64] = {};         // 点分十进制
          unsigned short port = 0;
      };
      auto send_to(const char* buf, size_t len, const char* ip, unsigned short port);
      auto recv_from(char* buf, size_t len);
      // 已连接 UDP (第二拍, 可选): connect(ip,port) 后复用 read/write 语义
      void close();
      bool valid() const;
  };
  ```

- [ ] **步骤 2｜Linux 实现**（`#ifdef __linux__` 段，TcpListener 类之后）。
  完整骨架——与 §3.2 范本逐行对照，差异处已标注：

  ```cpp
  class UdpSocket {
    public:
      bool bind(const char* ip, unsigned short port) {
          fd_ = ::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK, 0);   // ← SOCK_DGRAM
          if (fd_ < 0) return false;
          sockaddr_in addr{};
          addr.sin_family = AF_INET;
          addr.sin_port = htons(port);
          addr.sin_addr.s_addr = inet_addr(ip);
          return ::bind(fd_, (sockaddr*)&addr, sizeof(addr)) == 0;
      }

      struct recv_from_awaiter {
          UdpSocket* sock;
          char* buf;
          size_t len;
          detail::uring_op op;
          net::UringEventSource* uring_ = nullptr;

          // ★ UDP 特有: recvmsg 的辅助结构全部作为成员 (协程帧存活保证,
          //   这是 IORING_OP_RECV 不携带对端地址的唯一正确姿势)
          struct msghdr mh{};
          struct iovec iov{};
          struct sockaddr_storage from{};    // storage 兼容未来 IPv6
          recv_from_result out{};

          ~recv_from_awaiter() { if (uring_) uring_->untrack_op(&op); }
          bool await_ready() noexcept { return false; }

          static void cancel_op(void* self) {
              // 照搬 net.hpp:567-577 read_awaiter::cancel_op, 一字不改
          }

          void await_suspend(std::coroutine_handle<> h) {
              op.continuation = h;
              iov.iov_base = buf;  iov.iov_len = len;
              mh.msg_name = &from; mh.msg_namelen = sizeof(from);
              mh.msg_iov = &iov;   mh.msg_iovlen = 1;
              if (auto* u = current_uring()) {
                  io_uring_sqe* sqe = io_uring_get_sqe(u->handle());
                  if (!sqe) { op.result = -ENOBUFS; EventLoop::get().schedule(h); return; }
                  // ★ 与 TCP 的唯一实质差异: prep_recv → prep_recvmsg
                  io_uring_prep_recvmsg(sqe, sock->fd_, &mh, 0);
                  uring_submit_op(u, u->handle(), &op, sqe);
                  uring_ = u; u->track_op(&op);
              } else { op.result = -ENOTSUP; EventLoop::get().schedule(h); }
          }

          recv_from_result await_resume() {
              if (op.error) { io::set_error(op.error); return out; }  // n=-1
              // ★ 把内核地址翻译成可读形式 (inet_ntop, 同步、无 IO)
              auto* sa = (sockaddr_in*)&from;
              out.n = op.result;                       // res: 字节数 / 负 errno
              out.port = ntohs(sa->sin_port);
              inet_ntop(AF_INET, &sa->sin_addr, out.ip, sizeof(out.ip));
              return out;
          }
      };

      auto recv_from(char* buf, size_t len) { return recv_from_awaiter{this, buf, len, {}, {}, {}, {}, {}, {}}; }

      // send_to_awaiter: 同构, prep_sendmsg + 目的地址在 await_suspend 里
      // 用 inet_addr 填一个 sockaddr_in 成员挂到 msghdr 上 (不需要取回地址,
      // 结构比 recv 简单); await_resume 返回 int (字节数/-1)
      // ...

    private:
      int fd_ = -1;
  };
  ```

  **`MSG_TRUNC` 语义写进头注释**：`io_uring_prep_recvmsg` 的 flags 传
  `MSG_TRUNC` 时，`res` 是**完整数据报长度**（即使缓冲不够被截断）；
  不传则 `res` 是实际装入缓冲的字节数。默认不传（与 recvfrom 传统一致），
  注释里写清楚两条路径的差别。

- [ ] **步骤 3｜Windows 实现**（`#ifdef _WIN32` 段，TcpListener 之后）：
  复制 `read_awaiter`（`net.hpp:127-173`）骨架，三处改动——
  ① `WSARecv` → `WSARecvFrom`，多传 `sockaddr_in from` 成员 + `fromlen`、
  `MSG_PARTIAL` 语义注释；② `await_resume` 里 `inet_ntop` 翻译 `from`；
  ③ 发送用 `WSASendTo`（目的地址每次传参，比 Linux 还简单，不需要 msghdr）。
  取消钩子 `CancelIoEx` 原样保留（完成包以 `ERROR_OPERATION_ABORTED` 回来）。

- [ ] **步骤 4｜测试 `tests/test_net_udp.cpp`**（守卫抄 `test_net.cpp:2`）：

  ```cpp
  // 用例 1 回环收发: bind 两socket, send_to → recv_from, 断言内容一致且
  //                返回的 ip/port == 发送方绑定地址 (datagram 边界不粘连:
  //                连发 3 个不同长度报文, 3 次 recv_from 各得一个完整报文)
  // 用例 2 双向并发: A/B 各 bind, 互相 send_to 一千次, 各自收满一千个, 无丢失
  // 用例 3 取消挂起中的 recv_from: 参考 test_net.cpp 的 io_cancel_scenario
  //                写法 — wait_for(recv_from(...), 50ms) 抛 TimeoutError 后,
  //                socket 仍可正常收发 (资源未泄漏的证明)
  ```

- [ ] **步骤 5｜验证**：

  ```bash
  cmake --build build -j && ./build/coro_tests --gtest_filter='*Udp*'
  ctest --test-dir build --output-on-failure   # 全量回归
  ```

- [ ] **步骤 6｜文档**：`docs/api-reference.md` TCP 节扩为「网络 net.hpp」
  （TcpStream / TcpListener / UdpSocket 三小节，UdpSocket 节写清
  MSG_TRUNC 与返回结构）；`docs/architecture.md` 代码规模表 +1 行；
  tutorial 第 6 讲补 UDP 小节。

### 1.2 DNS 解析（域名 → IP）

> **目标**：`resolve("localhost", 8080)` 返回 IP 列表；连接/监听接受域名。
> **核心思路**：`getaddrinfo` 是阻塞调用，**绝不能在事件循环线程上直接调**
> （`performance.md` §4.1 头号反模式）——用 `coro::to_thread` 把它桥到
> 线程池（`thread.hpp`），事件循环线程只 `co_await` 结果。

- [ ] **步骤 1｜实现 `resolve`**（`net.hpp` 两个平台段共用，放在平台
  `#endif` 之后、`namespace net` 收尾之前）：

  ```cpp
  /// 域名 → IP 列表 (点分十进制, 去重)。阻塞的 getaddrinfo 在 to_thread
  /// 的线程池里执行, 不占事件循环线程。失败: 返回空列表 + io::set_error。
  inline coro::Task<std::vector<std::string>> resolve(const std::string& host,
                                                      unsigned short port) {
      // 错误码不能用 errno 通道直接搬: getaddrinfo 返回的是 EAI_* 码,
      // 在 worker 线程里 set_error 也传不回来 (错误通道是线程局部的)。
      // → 在线程里只收集结果、rc 和 errno, 回到事件循环线程再 set_error。
      //
      // 关键: 所有输入按值捕获 (host 拷贝, port 值传递), 结果通过返回值
      // 传回。后台线程不引用协程帧内的任何变量, 取消后即使线程仍在执行
      // 也不会访问已释放的协程帧。
      struct resolve_result {
          int gai_rc = 0;
          int saved_errno = 0;
          std::vector<std::string> ips;
      };
      auto r = co_await coro::to_thread([host, port]() -> resolve_result {
          resolve_result r;
          addrinfo hints{};
          hints.ai_family = AF_INET;          // IPv6 留给阶段 4 (AF_UNSPEC)
          hints.ai_socktype = SOCK_STREAM;
          char service[16];
          std::snprintf(service, sizeof(service), "%u", (unsigned)port);
          addrinfo* res = nullptr;
          r.gai_rc = getaddrinfo(host.c_str(), service, &hints, &res);
          if (r.gai_rc != 0) {
              r.saved_errno = errno;          // 在工作线程内立即保存 errno
              return r;
          }
          for (addrinfo* p = res; p; p = p->ai_next) {
              char ip[INET_ADDRSTRLEN];
              if (inet_ntop(AF_INET, &((sockaddr_in*)p->ai_addr)->sin_addr,
                            ip, sizeof(ip))) {
                  std::string s(ip);
                  if (std::find(r.ips.begin(), r.ips.end(), s) == r.ips.end())  // 去重
                      r.ips.push_back(std::move(s));
              }
          }
          freeaddrinfo(res);
          return r;
      });
      if (r.gai_rc != 0) {
          // EAI_SYSTEM: getaddrinfo 返回系统错误, errno 已保存在 r.saved_errno;
          // 其他 EAI_* 码映射为 EINVAL (保留原始 EAI 码供上层判断)。
          int err = (r.gai_rc == EAI_SYSTEM) ? r.saved_errno : EINVAL;
          io::set_error(err);
      }
      co_return r.ips;
  }
  ```

  需要补的 include：`<netdb.h>`（Windows 是 `<ws2tcpip.h>`，已包含）+
  `<algorithm>`。

- [ ] **步骤 2｜域名版 connect**。注意：现有 `TcpStream::connect(ip, port)` 是
  **同步构造 awaiter** 的静态函数，域名解析是异步的，塞不进去——新增协程版：

  ```cpp
  /// 域名/IP 通吃的连接入口: 点分 IP 走原快路径 (零额外开销),
  /// 域名 resolve 后逐个尝试, 全部失败返回 valid()==false 的流。
  static coro::Task<TcpStream> connect_host(const std::string& host, unsigned short port) {
      in_addr fast{};
      if (inet_pton(AF_INET, host.c_str(), &fast) == 1)      // 本来就是 IP
          co_return co_await TcpStream::connect(host.c_str(), port);
      auto ips = co_await resolve(host, port);
      if (ips.empty()) { io::set_error(EINVAL); co_return TcpStream{}; }
      for (const auto& ip : ips) {
          auto s = co_await TcpStream::connect(ip.c_str(), port);
          if (s.valid()) co_return s;
      }
      co_return TcpStream{};                                   // 逐个尝试均失败
  }
  ```

  `TcpListener::bind_listen` 是同步函数：**不加域名重载**（服务端通常绑
  字面 IP 或 `"0.0.0.0"`），在 api-reference 里写明服务端如需域名解析，
  先 `co_await resolve()` 再把结果传给 `bind_listen`。

- [ ] **步骤 3｜测试 `tests/test_dns.cpp`**：
  - `"localhost"` 解析结果包含 `127.0.0.1`（Linux 读 /etc/hosts，离线可测）；
  - `connect_host("localhost", port)` 连上本机 echo 服务并完成一轮收发；
  - 解析不存在的域名（`"no.such.host.invalid"`）返回空列表且
    `io::last_error() != 0`；
  - 在线用例（解析公网域名）用环境变量门控，默认跳过：
    `if (!std::getenv("CORO_NET_TEST_ONLINE")) GTEST_SKIP();`

- [ ] **步骤 4｜验证 + 文档**：同 1.1 步骤 5；api-reference「网络 net.hpp」
  节补 `resolve` / `connect_host`；tutorial 第 6 讲连接示例改用域名。

---

## 6. 阶段 2：Web 层增强（仅 `Web/` 与 `router/`，不碰核心库）

> 前置：无硬依赖（中间件不依赖阶段 1；流式响应用 fs 模块）。
> 可与阶段 1 并行推进。

### 2.1 中间件机制

> **目标**：`routes().use(mw)` 链式注册；中间件 = 「接 request、接 next、
> 返回 response」的高阶协程函数。不引入新抽象层——就是教程第 9 讲练习 3
> 「包装 handler」模式的官方语法糖。

- [ ] **步骤 1｜`router.h` 增类型与注册**（`class router` 公有段顶部）：

  ```cpp
  public:
      using next_fn       = std::function<coro::Task<http_response>()>;
      using middleware_fn = std::function<coro::Task<http_response>(http_request&, next_fn)>;

      /// 注册中间件。注册顺序 = 包裹顺序: 先注册者在最外层。
      /// 中间件在路由匹配之前执行 —— 此时 req.params 尚未填充,
      /// 鉴权/日志请用 req.path_view() 与 req.header(...) 判断。
      void use(middleware_fn mw) { middlewares_.push_back(std::move(mw)); }

  private:
      std::vector<middleware_fn> middlewares_;
  ```

- [ ] **步骤 2｜拆分 dispatch**：把现有 `route()`（`router.h:68-154`）的
  **四步分发逻辑原样搬进**新的私有协程 `dispatch_core(http_request&)`
  （radix 命中 → 405/OPTIONS → 静态目录 → 404），一行不改——
  这是「行为零变化」的硬要求，搬移用纯剪切粘贴完成。

- [ ] **步骤 3｜链组装 + 异常兜底**：

  ```cpp
  /// 执行第 idx 层中间件; 到达链尾则做真正的路由分发
  coro::Task<http_response> run_chain(size_t idx, http_request& req) const {
      if (idx == middlewares_.size())
          co_return co_await dispatch_core(req);
      co_return co_await middlewares_[idx](req, [this, &req, idx]() -> coro::Task<http_response> {
          return run_chain(idx + 1, req);
      });
  }

  coro::Task<http_response> route(http_request& req) const {
      try {
          co_return co_await run_chain(0, req);
      } catch (const std::exception& e) {
          co_return http_response::error(500, std::string("internal error: ") + e.what());
      } catch (...) {
          co_return http_response::error(500, "internal error");
      }
  }
  ```

  **生命周期说明（写进头注释）**：`next_fn` 按引用捕获 `req` 与 `this`，
  只能在当前中间件协程体内 `co_await next()` 使用，**不许存起来跨协程调用**
  （`std::function` 复制出去 = 悬垂引用）。

- [ ] **步骤 4｜使用示例**（写进 web-framework.md，测试也照这个写）：

  ```cpp
  router routes;
  routes.use([](http_request& req, router::next_fn next) -> coro::Task<http_response> {
      auto t0 = std::chrono::steady_clock::now();
      http_response resp = co_await next();                  // 交给下一层
      log("%s %s -> %d (%ld us)", req.method.c_str(),
          req.path_view().data(), resp.status, elapsed(t0)); // 外层事后处理
      co_return resp;
  });
  routes.use([](http_request& req, router::next_fn next) -> coro::Task<http_response> {
      if (req.header("Authorization").empty())
          co_return http_response::error(401, "unauthorized"); // 短路: 不调 next()
      co_return co_await next();
  });
  ```

- [ ] **步骤 5｜测试**（`tests/test_router.cpp` 增补）：
  日志顺序（外→内→外，用 vector 记录断言）、鉴权短路 401（handler 未触达，
  用标志位证明）、异常兜底 500（handler 里 `throw std::runtime_error`）。
- [ ] **步骤 6｜文档**：web-framework.md 已知限制删「无中间件」，
  增「中间件」章节（含 params 未填充时机、next 生命周期两条警告）。

### 2.2 流式响应 + chunked

> **目标**：响应体可以边生产边发送（chunked 编码）；大文件不再全量驻留内存。
> **本项最大风险**：chunked 与 Content-Length 的互斥语义搞错会**撕裂
> keep-alive 连接**（客户端无法判断响应边界）——所以步骤 4 的四条语义测试
> 是硬门禁。

- [ ] **步骤 1｜扩展 `http_response`**（`http_types.h`）：

  ```cpp
  struct http_response {
      // ...现有 status/headers/body 不动...

      /// 流式生产者: 每次调用产出一块数据; 返回 nullopt 表示流结束。
      /// 非空 = web_server 走 chunked 编码路径 (此时忽略 body 字段)。
      std::function<coro::Task<std::optional<std::string>>() > chunk_producer;

      /// 异步文件工厂 (旧同步 file() 保留, 文档标记 deprecated):
      /// 中小文件: fs::read_all 异步整读; 大文件: 直接构造 chunk_producer。
      static coro::Task<http_response> file_async(const std::string& path);

      /// 只序列化状态行 + 头部 (chunked 路径用; body 由调用方分块写)
      std::string build_head() const;   // http_types.cpp 实现, 与 build() 共享头部拼装
  };
  ```

  `file_async` 第一版实现 = `router.h:142` 静态文件分支的同款代码
  （`co_await coro::fs::read_all` + mime_type）。真正的逐块磁盘读
  （`read_at` 偏移接口）需要 fs 模块小扩展，**列为可选第二拍**——
  第一版 producer 也可以「read_all 后切片」，已能解耦网络发送与磁盘等待。

- [ ] **步骤 2｜写回路径分叉**（`web_server.cpp` `handle_connection`，
  `:148` `resp.build()` 处改为）：

  ```cpp
  // write_all: 循环补齐, 保证全部写完或返回错误。所有协议片段统一使用。
  // 返回实际写入总字节数; <= 0 表示连接已断, 调用方应停止生产并关闭。
  inline coro::Task<ssize_t> write_all(coro::net::TcpStream& conn,
                                       const char* data, size_t len) {
      size_t off = 0;
      while (off < len) {
          ssize_t w = co_await conn.write(data + off, len - off);
          if (w <= 0) co_return w;   // 对端断开或错误: 立即返回
          off += (size_t)w;
      }
      co_return (ssize_t)off;
  }

  if (resp.chunk_producer) {
      // ---- 分支一: chunked 流式 ----
      // 互斥铁律: Transfer-Encoding: chunked 出现时绝不输出 Content-Length
      resp.header("Transfer-Encoding", "chunked", /*replace=*/true);
      std::string head = resp.build_head();
      if (co_await write_all(conn, head.data(), head.size()) <= 0)
          co_return;                                   // 对端已断: 停止一切生产
      while (true) {
          auto chunk = co_await resp.chunk_producer(); // 生产一块
          if (!chunk) break;                           // nullopt = 流结束
          if (chunk->empty()) continue;                // 空块跳过, 不产生 0 长度帧
          char size_hdr[24];
          int m = std::snprintf(size_hdr, sizeof(size_hdr), "%zx\r\n", chunk->size());
          // 所有协议片段统一用 write_all 补齐, 避免部分写入破坏响应边界
          if (co_await write_all(conn, size_hdr, (size_t)m) <= 0)
              co_return;
          if (co_await write_all(conn, chunk->data(), chunk->size()) <= 0)
              co_return;
          static const char CRLF[] = "\r\n";
          if (co_await write_all(conn, CRLF, 2) <= 0)
              co_return;
      }
      // 终止帧完整写出后才能复用连接
      if (co_await write_all(conn, "0\r\n\r\n", 5) <= 0)
          co_return;
  } else {
      std::string wire = resp.build();                 // ---- 分支二: 现有全量路径不动 ----
      // ...(原写回代码)
  }
  ```

  注意 `build()` 里强制 Content-Length 的逻辑**不改**（全量路径语义不变），
  chunked 头由 `build_head()` 路径产出——两条路径永不交叉。

- [ ] **步骤 3｜HEAD 语义**：HEAD 请求命中 chunked 响应时，只写头 +
  `Content-Length` 未知就不写，**不写任何 chunk**（协议允许）；
  web_server 解析层已知 method，直接在分叉前判断。

- [ ] **步骤 4｜HTTP 语义测试**（`tests/test_router.cpp` 或新建
  `tests/test_http_stream.cpp`，四条全部必过）：
  1. 大文件分块下载：临时文件若干 MB → producer 切 64KB → 客户端
     （测试内用 `TcpStream::connect` 手写 HTTP/1.1 客户端或复用现有测试基建）
     收到的字节流拼回去与源文件 `memcmp` 相等；
  2. chunked 响应后 keep-alive 复用：同一连接接着发第二个请求，
     能收到第二个完整响应（验证终止帧正确）；
  3. 客户端提前断开：服务端 producer 里设「生产次数」计数，
     客户端收首块即 RST → 断言生产次数停在个位数（不再为死连接生产）；
  4. 互斥检查：producer 响应的线上字节里同时断言
     `Transfer-Encoding: chunked` 存在、`Content-Length:` 不存在。
- [ ] **步骤 5｜实测脚本**：`web_server --selftest` 增流式路由；
  新增 `tests/curl_stream_test.sh`（`curl --raw` 看 chunked 帧、
  `curl -o /dev/null -w '%{size_download}'` 对比文件字节数）。
- [ ] **步骤 6｜文档**：web-framework.md 已知限制删「chunked / 全量缓存 /
  file() 同步」三条对应项，增「流式响应」章节（producer 约定 + 互斥铁律）。

### 2.3 可观测性（`/__stats`）

- [ ] **步骤 1｜计数器**（`web_server.h` 私有成员，全部 atomic）：

  ```cpp
  struct stats_t {
      std::atomic<uint64_t> requests{0};   // 累计完成请求数
      std::atomic<uint64_t> errors{0};     // 4xx/5xx 响应数
      std::atomic<uint64_t> in_flight{0};  // 当前并发
      std::atomic<uint64_t> peak{0};       // 峰值并发 (CAS 更新)
  } stats_;
  // handle_connection 入口: ++in_flight 后用 compare_exchange 循环抬 peak;
  // 出口 --in_flight; 响应写出后按 status>=400 ++errors; 其余 ++requests。
  ```

- [ ] **步骤 2｜端点**：serve 前注册
  `GET /__stats` → 手拼 JSON（`snprintf` 即可，不引依赖）：
  requests / errors / in_flight / peak + 每 worker `active_task_count()`
  （注意：任务注册表需 `CORO_TASK_REGISTRY` 编译期开启，未开启时该字段输出
  `-1` 并注释说明，别让用户误读）。
- [ ] **步骤 3｜测试**：打 N 个请求（含若干 404）后 `/__stats` 计数吻合；
  TSan 下无竞争告警。

---

## 7. 阶段 3：性能实验（数据驱动，每项独立开关）

> 前置：0.2 基准在手。**每项默认关闭（宏开关），基准无收益即回退删除。**
> 方法：改前/改后各跑 5 次 `bench_net` 四场景 + `coro_stress` 场景 3/5，
  取中位数对比，记录进 `performance.md`。

### 3.1 对称转移（仅非 MSVC）

- [ ] 改造点：`task.hpp` 内搜「对称转移」三处注释（约 `:127` 语言机制说明、
  `:570` Task<T> await_suspend「本可对称转移」、`:812` 同因）。
  `:570` 处的现状（示意）是子协程完成后入就绪队列、由事件循环二次调度：

  ```cpp
  // 现状 (示意): 排队恢复 — 一次队列 push/pop + 事件循环往返
  void await_suspend(std::coroutine_handle<> h) {
      /* 记录 continuation */ ;
      EventLoop::get().schedule(handle_);   // 子协程进就绪队列
  }
  // 实验版 (#if !defined(_MSC_VER) 且定义 CORO_SYMMETRIC_TRANSFER):
  // await_suspend 返回子协程句柄 — 编译器生成无条件跳转, 零队列往返
  std::coroutine_handle<> await_suspend(std::coroutine_handle<> h) noexcept {
      /* 记录 continuation */ ;
      return handle_;                       // 直跳执行子协程
  }
  ```

  以 `:570` 附近实际代码为准（上面是教学示意）；MSVC 保持现状
  （C4737，`architecture.md` §13.1）。编译期开关 `CORO_SYMMETRIC_TRANSFER`，
  默认 OFF。
- [ ] 取消语义回归：对称转移改变了「子协程何时开始跑」的时序，
  `test_cancel.cpp` 全量必须过；不过 = 直接回退，不带病合入。
- [ ] 数据不涨（<3%）或回归 → 删除实验代码，只在 architecture.md §13.1
  补一句「已实验，无收益」。

### 3.2 io_uring 参数化

- [ ] `uring_event_source.hpp:~73` 现状是硬编码：

  ```cpp
  io_uring_queue_init(256, &ring_, 0);
  ```

  改为（默认行为完全不变）：

  ```cpp
  // 同时挂起的异步操作上限; 万级并发挂起 IO 的场景可经编译宏调大
  #ifndef CORO_RING_SIZE
  #define CORO_RING_SIZE 256
  #endif
  io_uring_queue_init(CORO_RING_SIZE, &ring_, 0);
  ```

- [ ] 评估 `io_uring_register_ring_fd`（liburing ≥2.2 / 内核 ≥5.18，
  submit 少一次 fd 查找）：liburing 版本探测可用才启用，否则静默跳过；
  用 bench_net 场景 1（echo 吞吐）验证收益。
- [ ] **拒绝默认启用 SQPOLL**（需特权）——只在 performance.md 写手动开启方式。

### 3.3 每连接内存与缓冲

- [ ] 依 0.2 场景 4 的实测（每连接内存 = 协程帧 + 读缓冲 + 内核 socket 缓冲），
  若读缓冲占比显著：评估 `web_server.cpp` 读缓冲自适应策略
  （首包 4KB，头部超限再倍增，上限 cap），对照 `performance.md` §4.4
  「大 buffer 进协程帧」反模式；**有数据支撑才动代码**，并回跑场景 4 验证。

---

## 8. 阶段 4：战略扩展（需前置决策，最后启动）

| 项 | 前置决策 | 工作量 | 依赖 |
|---|---|---|---|
| TLS（TlsStream 装饰 TcpStream） | 依赖选型：OpenSSL vs mbedTLS vs 编译开关 `CORO_WITH_TLS`（OFF 时零依赖不变，倾向后者） | 大 | TCP 稳定即可，无硬依赖 |
| IPv6（`sockaddr_storage` + `getaddrinfo` 遍历） | API 兼容策略（默认 `AF_UNSPEC`，现有 IPv4 行为不变）；触碰 net.hpp 全部地址代码与两类接口 | 大 | 建议在 1.2 DNS（已引入 getaddrinfo）之后 |
| kqueue 事件源（macOS） | macOS CI runner（`ci.yml:141` 预留位）；模仿 CVEventSource + kevent | 中 | 无 |
| HTTP/2 | 生态收益/工作量比低，暂缓 | 极大 | 阶段 2.2 流式完成后再议 |

每项启动前先在本文档补一节「决策记录」（备选项/取舍/结论），再写实施步骤。

---

## 9. 横切门禁（每阶段 Done 的定义）

1. **文档同步**：新 API 同 PR 更新 `docs/api-reference.md`、
   `docs/architecture.md`（分层图/代码规模表）、受影响 tutorial；
   Web 层改动更新 `docs/web-framework.md` 已知限制清单。
2. **代码惯例**：awaiter 三板斧（§3.2-②③④）；fire-and-forget 用
   「命名协程 + `start()+detach()`」（`architecture.md` §11.5）；
   错误走 `io::set_error` 双通道。
3. **测试门禁**：新模块配 `tests/test_*.cpp`（GLOB 自动收编）；
   Linux 改动过 CI 全矩阵（GCC 13/Clang 18 × Debug/Release + ASan/UBSan + TSan）。
4. **格式**：clang-format（CI lint job）。

**提交前自查**（30 秒过一遍）：

- [ ] `ctest --test-dir build --output-on-failure` 全绿？
- [ ] 新 awaiter 有 `cancel_op` + 反注册/完成包保证 + `set_error` 三样？
- [ ] 新文件被 GLOB 收编了吗（文件名对不对）？
- [ ] api-reference / architecture / web-framework 三处该改的都改了？

---

## 10. 里程碑与依赖顺序

```
M0  阶段 0 全部完成  →  产出: process 解禁 + 网络基线数据 + 本路线图挂载
M1  阶段 1 完成      →  产出: UdpSocket + resolve + 域名连接
M2  阶段 2 完成      →  产出: 中间件 + 流式/chunked + /__stats
    (M1 与 M2 可并行; M2 只依赖 M0)
M3  阶段 3 按单项推进 →  每项独立宏开关, 基准护航, 无益即回退
M4  阶段 4 逐项决策   →  每项先出决策记录(依赖选型/API 兼容策略)再动工
```

---

## 11. 风险与回退

| 风险 | 触发场景 | 缓解 |
|---|---|---|
| GCC 13 仍有 ICE 变体 | 0.1 步骤 2 | 任务挂起不阻塞其他阶段；CMake 注释记录版本 |
| chunked 撕裂 keep-alive | 2.2 语义错误 | §6-2.2 步骤 4 的四条语义测试是硬门禁，不过不合入 |
| 对称转移破坏取消语义 | 3.1 直跳绕过队列 | 宏默认 OFF；`test_cancel.cpp` 全量回归 + 基准无益即回退 |
| 基准数据噪声 | 3.x 对比失真 | 每场景 5 次取中位数；机器空闲时段跑；记录环境 |
| UDP msghdr 生命周期 | 1.1 对端地址悬垂 | 地址缓冲全放 awaiter 成员（协程帧存活保证，§3.2-①） |
| 中间件 next 悬垂 | 2.1 跨协程存 next | 头注释 + 文档双警告；不做运行时防御（零开销原则） |

---

## 12. 已拒绝的备选方案（不再复议）

1. **net.hpp 双平台公共 awaiter 基类**：项目惯例是双平台显式对称
   （`architecture.md` §13.5）；IOCP 三形态完成 vs uring 恒有 CQE，
   抽象层收益为负。
2. **work-stealing 调度器**：与线程亲缘模型冲突，`architecture.md` §9
   已论证「帧不迁移」收益（零内部锁、无跨线程堆问题）。
3. **定时器 token 池化**：`architecture.md` §13.2——条件归还等于重造引用计数。
4. **c-ares 异步 DNS**：引入第三方依赖，违背零依赖定位；`to_thread`
   桥接在连接场景够用。
5. **默认启用 SQPOLL**：需特权/新内核，破坏零配置开箱即用。
6. **asyncio.shield 专设 API**：`catch CancelledError` 已覆盖语义
   （`faq.md` §290 官方立场）。

---

## 附：关键文件索引

| 文件 | 在本计划中的角色 |
|---|---|
| `include/coro/net.hpp` | 1.1 UDP / 1.2 DNS / 阶段 4 IPv6 主战场（双平台实现）；§3.2 范本所在地 |
| `include/coro/task.hpp` | 3.1 对称转移实验点（搜「对称转移」三处注释） |
| `Web/src/http_types.h/.cpp` | 2.2 chunk_producer / file_async / build_head |
| `Web/src/router.h` | 2.1 use / dispatch_core / run_chain；2.2 静态文件异步读的现成范本（:142） |
| `Web/src/web_server.cpp` | 2.2 写回分叉（:92 handle_connection, :148 build 处）；2.3 stats 计数 |
| `include/coro/uring_event_source.hpp` | 3.2 ring 参数化（当前队列深度 256） |
| `include/coro/thread.hpp` | 1.2 to_thread 桥接（resolve 的执行引擎） |
| `tests/test_net.cpp` | 平台守卫与回环测试的抄写范本（:2 守卫、io_cancel_scenario） |
| `tests/stress.cpp` | bench_net 的风格母版（Timer/命名协程/run 驱动） |
| `CMakeLists.txt:26-47` | 平台/编译器能力开关（0.1 改造点）；:124 测试 GLOB |
| `.github/workflows/ci.yml:134-137` | 0.2 基准运行步骤插入点 |
| `docs/performance.md` + `docs/architecture.md` | 基线记录与架构约束权威来源 |
