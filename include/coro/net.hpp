#pragma once

#include "event_loop.hpp"
#include "io.hpp"
#include "net_common.hpp"

#include <coroutine>

// 平台实现按平台分别落地, 本头只做选择与文档:
//   Windows → net_iocp.hpp   (TcpStream/TcpListener/UdpSocket 走 IOCP)
//   Linux   → net_uring.hpp  (同名三件走 io_uring; 未启用时内容为空)
#ifdef _WIN32
#include "net_iocp.hpp"
#elif defined(__linux__)
#include "net_uring.hpp"
#endif

// ============================================================================
// coro::net — 异步网络层 (Windows IOCP / Linux io_uring)
// ============================================================================
//
// 本头只负责选择平台实现并说明架构; 具体代码在:
//   net_iocp.hpp    Windows IOCP 实现
//   net_uring.hpp   Linux io_uring 实现
//   net_common.hpp  两边共用的平台无关件 (如 parse_ipv4)
//
// 架构 (模仿 Python asyncio ProactorEventLoop):
//
//   ┌────────────────────────────────────────────────────┐
//   │ TcpListener / TcpStream (协程 API)                 │
//   │   co_await listener.accept()                       │
//   │   co_await stream.read(buf, n)                     │
//   │   co_await stream.write(buf, n)                    │
//   └───────────────────────┬────────────────────────────┘
//                           │ 每个操作 = iocp_op (OVERLAPPED)
//   ┌───────────────────────▼────────────────────────────┐
//   │ IOCP 完成端口 (EventLoop 的默认 EventSource)        │
//   │   wait_for  → GetQueuedCompletionStatus(timeout)   │
//   │   wake      → PostQueuedCompletionStatus (唤醒包)  │
//   └───────────────────────┬────────────────────────────┘
//                           │ 完成 → 恢复对应协程
//   ┌───────────────────────▼────────────────────────────┐
//   │ EventLoop (schedule + resume, 与定时器统一调度)     │
//   └────────────────────────────────────────────────────┘
//
// 零配置: EventLoop 在 Windows 上默认使用 IOCP, 网络 API
// 自动关联 socket 到完成端口, 用户不需要任何初始化代码。
// Linux 侧把 IOCP 换成 io_uring, 层次与生命周期约定完全相同。
//
// 生命周期约定 (同 Task): 操作挂起期间, awaiter 必须保持存活
// (它在协程帧中, 协程挂起即存活; 不要在操作进行中销毁协程)。
// ============================================================================

