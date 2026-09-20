#pragma once

#include "event_loop.hpp"

#include <cassert>
#include <cstdio>
#include <coroutine>
#include <exception>
#include <functional>
#include <iostream>
#include <optional>
#include <type_traits>
#include <utility>
#include <variant>
#include <atomic>

// ============================================================================
// coro::Task<T> — C++20 协程返回类型（框架的核心抽象）
// ============================================================================
//
// 这是整个框架最重要的类型。一个返回 Task<T> 的函数会被编译器
// 转换为协程, Task<T> 扮演了三个角色:
//   1. 协程的「返回类型」(return type)    — 编译器通过 promise_type 创建
//   2. 协程的「句柄」(handle)            — 持有 coroutine_handle
//   3. 一个「可等待对象」(awaitable)      — 可以被 co_await
//
// Python 映射:
//   async def foo(): ...     →  Task<T> foo()
//   await foo()              →  co_await foo()
//   asyncio.create_task(f)   →  coro::spawn(foo())
//
// ============================================================================
//
// C++20 协程的生命周期（以 Task<T> 为例）:
//
//   1. 调用 Task<T> foo() → 编译器创建协程帧, 调用 promise_type 构造函数
//   2. promise_type::get_return_object() → 创建 Task 对象并返回给调用者
//   3. promise_type::initial_suspend()   → 返回 suspend_always, 协程在此挂起
//      (这就是「惰性启动」: 协程不会立即执行, 需要外部 resume)
//   4. 外部调用 Task::start() 或 co_await task → 协程恢复执行
//   5. 协程体执行, 可能 co_await 其他对象 → 反复挂起和恢复
//   6. co_return 或函数结束时:
//      - promise_type::return_value() 或 return_void() 被调用
//      - promise_type::final_suspend() 被调用
//      - final_awaiter 将结果写入 Task, 调度 continuation
//      - 协程帧被销毁
//   7. Task 析构: 如果协程还未完成, 调用 handle_.destroy() 销毁帧
//
// ============================================================================
//
// 关键设计决策:
//
//   initial_suspend = suspend_always (惰性启动)
//     优点: 调用者可以先创建 Task, 再决定何时启动
//     缺点: 需要显式 start() 或 co_await
//
//   final_suspend = 自定义 final_awaiter (返回 false)
//     行为: 在 await_suspend 中完成收尾工作后返回 false
//     效果: 协程不被挂起, 直接销毁协程帧
//     原因: 我们不需要在 final_suspend 后继续访问 promise,
//           因为结果已经移到了 Task 对象中
//
//   结果存储:
//     promise 使用 std::variant<std::monostate, T> 暂存结果
//     final_suspend 时将结果从 variant 移到 Task::result_ (std::optional<T>)
//     这样协程帧销毁后, Task 仍然可以访问结果
//
//   指针回指 (task_ 指针):
//     promise 持有一个指向 Task 的原始指针 task_
//     作用: 让 promise 在 final_suspend 时能把结果写入正确的 Task
//     代价: Task 被移动时需要更新 task_ (在移动构造/赋值中处理)
//     风险: Task 被销毁后 task_ 变成悬空指针 → UB
//     缓解: final_suspend 中 release_handle() 将 handle_ 设为 nullptr,
//           避免析构时 double-free
//
//   取消机制 (await_transform 注入, 对标 Python Task.cancel()):
//     Task::cancel() 置 promise.cancelled_ = true。协程体内的每次 co_await
//     都经过 await_transform 包装 (cancel_check_awaiter), 因此:
//       - 已挂起的协程被强制唤醒后, 在 await_resume 处抛出 CancelledError
//       - 还在就绪队列中的协程, 运行到第一个 await 点即抛出
//       - 尚未启动的协程直接被标记为已完成(带异常)
//     协程体收到 CancelledError 后正常 unwind (栈上对象析构 = 清理逻辑),
//     这与 Python asyncio 的语义一致。
//
// ============================================================================

namespace coro {

    // ============================================================================
    // CancelledError — 协程取消异常（类似 Python asyncio.CancelledError）
    // ============================================================================
    //
    //   当 Task::cancel() 被调用后，被取消的协程会在下一个 await 点
    //   收到此异常（注入协程体内，可 catch 做清理），任何 co_await 该
    //   Task 的协程也会在 await_resume 时收到此异常。
    //
    //   用法:
    //     try { co_await task; }
    //     catch (const CancelledError&) { /* 清理工作 */ }
    // ============================================================================
    struct CancelledError : std::runtime_error {
        CancelledError() : std::runtime_error("coroutine cancelled") {}
    };

    // ============================================================================
    // TimeoutError — 超时异常（wait_for 超时时抛出）
    // ============================================================================
    struct TimeoutError : std::runtime_error {
        TimeoutError() : std::runtime_error("operation timed out") {}
    };

    // 前向声明 (默认 T = void, 这样 Task<> 等价于 Task<void>)
    template <typename T = void> class Task;

    namespace detail {

        // ======================================================================
        // final_awaiter — 协程体执行完毕后的收尾工作
        // ======================================================================
        //
        // 当协程体执行完毕 (co_return 或运行到结尾) 时,
        // 编译器会调用 promise_type::final_suspend() 返回此对象,
        // 然后按 await_ready → await_suspend → await_resume 顺序调用。
        //
        // await_suspend 返回 false 的含义:
        //   "不要挂起, 继续执行 await_resume 然后销毁协程帧"
        //   (返回 true 则挂起, 返回 coroutine_handle 则对称转移)
        //
        // 收尾工作:
        //   1. 调用 promise.store_result()     — 将结果从 promise 移到 Task
        //   2. 如果有 continuation, 将其调度到事件循环
        //   3. 调用 promise.release_handle()   — 防止 Task 析构时 double-free
        //   4. 返回 false                      — 销毁协程帧
        // ======================================================================
        template <typename Promise> struct final_awaiter {
            bool await_ready() noexcept { return false; }

            // 非模板成员函数: 避免 MSVC Debug 对模板 final_suspend awaiter 的处理 bug
            bool await_suspend(std::coroutine_handle<Promise> h) noexcept {
                auto& promise = h.promise();

                // 步骤 1: 将结果/异常从 promise 移交到 Task
                promise.store_result();

                // 步骤 2: 先捕获 target_loop (在 schedule continuation 之前!)
                // continuation 被 schedule 后可能立即运行并销毁 Task,
                // 导致后续访问 promise 成员时状态已失效。
                EventLoop* target = promise.target_loop_ ? promise.target_loop_ : &EventLoop::get();

                // 步骤 3: 先保存 continuation，并完成 owner-loop/Task 外壳
                // 的收尾。另一个 loop 可能在 schedule 后立即恢复等待者，
                // 进而销毁它持有的 Task；因此不能把 schedule 放在这些操作前。
                auto continuation = promise.continuation_;
                EventLoop* cont_loop = promise.continuation_loop_ ? promise.continuation_loop_ : &EventLoop::get();

                // 步骤 4: 通知 Task 不再持有有效句柄
                promise.release_handle();

                // 步骤 5: 活跃协程计数 -1, 移出任务注册表 (与启动时的 loop 配对)
                // 使用步骤 2 预先捕获的 target (避免访问可能已失效的 promise)
                target->on_coroutine_finished(h);

                // 步骤 6: 所有内部状态已经提交后再发布 continuation。
                if (continuation)
                    cont_loop->schedule(continuation);

                // 步骤 7: 返回 false → 不挂起, 编译器将销毁协程帧
                return false;
            }

            void await_resume() noexcept {}
        };

        // ======================================================================
        // 取消注入机制 — 对标 Python 的「下一个 await 点抛出 CancelledError」
        // ======================================================================
        //
        // 每个 Task 协程的 promise_type 都提供 await_transform(),
        // 因此协程体内的每一次 co_await 都会被包装成 cancel_check_awaiter:
        //
        //   co_await X   →   实际是 co_await wrap(X, &promise.cancelled_)
        //
        // 取消路径 (Task::cancel() 置 cancelled_=true 后):
        //   1. 协程被强制唤醒 (schedule 或 I/O 完成包)
        //   2. 恢复执行时先进入 wrapper::await_resume()
        //   3. 检查到 cancelled_ → 抛出 CancelledError 进入协程体
        //   4. 协程体 unwind (栈上对象析构 = 清理逻辑), 任务带异常完成
        //
        // 这与 Python asyncio 的语义一致: 取消发生在挂起点, 而不是
        // 等任务自然结束 (旧实现里循环任务被取消后永远无法完成)。
        //
        // wrapper 的额外职责:
        //   - await_ready() 检查取消标志: 已取消则不再挂起, 直接走
        //     await_resume() 抛异常 (防止取消后任务再次挂起而悬死)
        //   - 析构时通知 inner: 若协程帧被销毁 (取消/异常路径) 而
        //     await_resume 尚未执行, 帮 inner 摘除等待队列中的僵尸句柄
        //   - 挂起 I/O 时向 promise 注册 cancel_hook, 让 Task::cancel()
        //     能先取消底层 I/O (CancelIoEx / ASYNC_CANCEL), 保证完成包
        //     先于协程帧销毁被消费 (OVERLAPPED 生命周期安全)
        //   - await_suspend 时置 promise.suspended_ = true, 让 Task::cancel()
        //     区分「挂起中」与「还在就绪队列中」, 避免 double-schedule
        //     (同一句柄被 resume 两次 → 帧销毁后 done() 是 UB)
        // ======================================================================

        // 静态缓存的 CancelledError 异常指针。
        // 取消路径原本每次 make_exception_ptr (一次堆分配 + 异常对象构造);
        // 改为进程级单例 —— exception_ptr 拷贝只是原子引用计数加一,
        // rethrow_exception 每次抛出仍会复制出独立的异常对象, 语义不变。
        inline std::exception_ptr cancelled_exception() {
            static std::exception_ptr e = std::make_exception_ptr(CancelledError{});
            return e;
        }

        // 检测 awaiter 是否提供 static cancel_op(void*) (网络 I/O 层)
        template <typename T, typename = void> struct has_cancel_op : std::false_type {};
        template <typename T>
        struct has_cancel_op<T, std::void_t<decltype(T::cancel_op(std::declval<void*>()))>> : std::true_type {};

        // 检测 awaiter 是否提供 on_waiter_destroyed(句柄) (等待队列清理)
        template <typename T, typename = void> struct has_on_waiter_destroyed : std::false_type {};
        template <typename T>
        struct has_on_waiter_destroyed<
            T, std::void_t<decltype(std::declval<T&>().on_waiter_destroyed(std::declval<std::coroutine_handle<>>()))>>
            : std::true_type {};

        template <typename T> void notify_waiter_destroyed(T& inner, std::coroutine_handle<> h) {
            if constexpr (has_on_waiter_destroyed<T>::value)
                inner.on_waiter_destroyed(h);
        }

        // detach 任务未捕获异常的报告回调 (默认打印警告, 用户可替换)
        // 对标 Python 的 "Task exception was never retrieved" 警告
        inline std::function<void(std::exception_ptr)>& detached_exception_handler() {
            static std::function<void(std::exception_ptr)> fn = [](std::exception_ptr e) {
                try {
                    std::rethrow_exception(e);
                } catch (const CancelledError&) {
                    return; // 正常取消, 不算错误
                } catch (const std::exception& ex) {
                    std::fprintf(stderr, "[coro] detached task exception: %s\n", ex.what());
                } catch (...) {
                    std::fprintf(stderr, "[coro] detached task exception: unknown\n");
                }
            };
            return fn;
        }

        // ======================================================================
        // cancel_check_awaiter — co_await 包装器 (由 await_transform 创建)
        // ======================================================================
        //
        // 模板参数:
        //   Promise — 当前协程的 promise_type (用于注册取消钩子)
        //   Awaiter — 转发引用, 保留左值/右值信息:
        //     左值 (Lock& 等)  → 按引用存储, 不拷贝
        //     右值 (Task 等)   → 按值存储 (移动), 避免悬空引用
        // ======================================================================
        template <typename Promise, typename Awaiter> struct cancel_check_awaiter {
            using stored_t =
                std::conditional_t<std::is_lvalue_reference_v<Awaiter>, Awaiter, std::remove_reference_t<Awaiter>>;
            using inner_t = std::remove_reference_t<stored_t>;

            stored_t inner;                       // 被包装的 awaiter (引用或值)
            Promise* promise;                     // 所属协程的 promise (注册取消钩子)
            const std::atomic<bool>* cancel_flag; // 指向 promise.cancelled_ (帧内, 地址稳定)
            std::coroutine_handle<> my_handle;    // 挂起时的当前协程句柄

            cancel_check_awaiter(stored_t a, Promise* p, const std::atomic<bool>* flag)
                : inner(std::forward<Awaiter>(a)), promise(p), cancel_flag(flag) {}

            ~cancel_check_awaiter() {
                // 注意: 不清除 cancel_hook_ / cancel_hook_self_。
                // 钩子在 await_suspend 时被下一次 I/O 覆盖, 或随帧销毁失效。
                // 保留钩子使 cancel() 在协程恢复后仍能有效取消底层 I/O
                // (修复 signal::handle 取消时 io_uring 操作泄漏导致的 use-after-free)。
                // my_handle 非空说明协程挂起后帧被销毁而 await_resume 未执行
                // (取消注入的异常路径): 通知 inner 摘除等待队列中的僵尸句柄
                if (my_handle) {
                    notify_waiter_destroyed(inner, my_handle);
                }
            }

            bool await_ready() {
                // 已请求取消: 不再挂起, 直接进 await_resume 抛异常
                // (acquire: 与 cancel() 的 release-store 配对, 单向标志足够)
                if (cancel_flag->load(std::memory_order_acquire))
                    return true;
                return inner.await_ready();
            }

            // await_suspend: 正确传播内层 awaiter 的返回值。
            // 关键修复: 旧实现返回 void, 导致内层 await_suspend 返回 false
            // (不挂起) 时协程仍然挂起 → 死锁 (Lock 重入等场景)。
            // 现在根据内层返回类型选择:
            //   bool   → 传播 (false = 不挂起)
            //   void   → 返回 void (总是挂起)
            //   handle → 传播 (对称转移)
            auto await_suspend(std::coroutine_handle<Promise> h) {
                my_handle = h;
                promise->suspended_ = true; // 标记挂起: Task::cancel 据此决定是否强制唤醒
                // 网络 I/O awaiter: 向 promise 注册取消钩子,
                // Task::cancel() 用它取消底层 I/O 并等待完成包唤醒
                if constexpr (has_cancel_op<inner_t>::value) {
                    promise->cancel_hook_ = [](void* self) { inner_t::cancel_op(self); };
                    promise->cancel_hook_self_ = &inner;
                    promise->pending_io_ = true; // 标记有挂起的底层 I/O 可被取消
                }
                using inner_result_t = decltype(inner.await_suspend(h));
                if constexpr (std::is_void_v<inner_result_t>) {
                    inner.await_suspend(h);
                    // void: 总是挂起 (无需传播)
                } else if constexpr (std::is_same_v<inner_result_t, bool>) {
                    bool result = inner.await_suspend(h);
                    if (!result) {
                        // 内层决定不挂起: 清除挂起标记并传播
                        my_handle = nullptr;
                        promise->suspended_ = false;
                        if constexpr (has_cancel_op<inner_t>::value) {
                            promise->pending_io_ = false;
                        }
                    }
                    return result;
                } else {
                    // coroutine_handle: 对称转移
                    auto transfer_to = inner.await_suspend(h);
                    if (!transfer_to) {
                        // 返回了空句柄: 不转移, 清除标记
                        my_handle = nullptr;
                        promise->suspended_ = false;
                        if constexpr (has_cancel_op<inner_t>::value) {
                            promise->pending_io_ = false;
                        }
                    }
                    return transfer_to;
                }
            }

            decltype(auto) await_resume() {
                // 协程已恢复: 必须清除挂起标记 — cancel() 不得把「运行中的」
                // 协程再次 schedule, 否则队列会残留已销毁帧的僵尸句柄
                // (final_suspend 不挂起, 帧立即释放 → done()/resume() 是 UB)。
                // 若协程再次挂起, 下一次 await_suspend 会重新置位。
                promise->suspended_ = false;
                // 底层 I/O 已完成: cancel() 不再需要取消它
                promise->pending_io_ = false;
                // 取消注入点: 恢复时若已被取消, 抛 CancelledError
                // (不执行 inner.await_resume, 底层结果直接丢弃 — Python 语义)
                // 注入后立即清除标志: 取消只注入一次。若协程体 catch 了
                // CancelledError (取消保护/清理), 任务可以正常完成。
                if (cancel_flag->load(std::memory_order_acquire)) {
                    promise->cancelled_.store(false, std::memory_order_release);
                    throw CancelledError{};
                }
                my_handle = nullptr; // 正常恢复: 析构时无需再清理等待队列
                return inner.await_resume();
            }
        };

    } // namespace detail

    // ============================================================================
    // Task<T> — 主模板（T 非 void）
    // ============================================================================
    template <typename T> class Task {
      public:
        // ==================================================================
        // promise_type — 编译器要求的嵌套类型
        // ==================================================================
        //
        // 编译器在遇到 co_await / co_return / co_yield 时,
        // 会通过 std::coroutine_traits 找到返回类型的 promise_type,
        // 然后用它来管理协程的整个生命周期。
        //
        // promise_type 必须提供以下方法:
        //   get_return_object()  — 创建返回给调用者的对象
        //   initial_suspend()    — 协程开始时是否挂起
        //   final_suspend()      — 协程结束时是否挂起
        //   unhandled_exception()— 未捕获异常的处理
        //   return_value()       — co_return expr; 的处理 (T 非 void)
        //   return_void()        — co_return; 或自然结束的处理 (T = void)
        // ==================================================================
        struct promise_type {
            /// 创建 Task 对象并返回给调用者
            /// from_promise 获取指向 promise 的 coroutine_handle
            Task get_return_object() { return Task{std::coroutine_handle<promise_type>::from_promise(*this)}; }

            /// 初始挂起: 返回 suspend_always 实现「惰性启动」
            /// 协程不会立即执行, 需要外部调用 start() 或 co_await 来恢复
            std::suspend_always initial_suspend() noexcept { return {}; }

            /// 最终挂起: 返回自定义 final_awaiter 做收尾工作
            detail::final_awaiter<promise_type> final_suspend() noexcept { return {}; }

            /// 未捕获异常: co_await 表达式或协程体中的异常
            /// 如果没有 try/catch 捕获, 会到这里
            /// 我们存储异常指针, 在 await_resume 时重新抛出
            void unhandled_exception() { exception_ = std::current_exception(); }

            // ---- await_transform: 取消注入机制的核心 ----
            //
            // 协程体内的每次 co_await 都会经过这里:
            //   co_await X  →  co_await cancel_check_awaiter<X>(X, this, &cancelled_)
            //
            // 作用: 让协程在每一个 await 点都能感知到 Task::cancel(),
            // 被取消后在下一次恢复时抛出 CancelledError 终止协程体
            // (对标 Python asyncio 的取消语义)。
            template <typename Awaiter> auto await_transform(Awaiter&& awaiter) {
                return detail::cancel_check_awaiter<promise_type, Awaiter&&>(std::forward<Awaiter>(awaiter), this,
                                                                             &cancelled_);
            }

            /// co_return expr; → 调用 return_value(expr)
            /// 使用 variant 存储, 因为 result 可能在后续被异常覆盖
            template <typename U> void return_value(U&& value) { result_.template emplace<1>(std::forward<U>(value)); }

            // ---- 以下方法由 final_awaiter 调用 ----

            /// 将结果从 promise 的 variant 移到 Task 的 optional
            /// 必须在协程帧销毁前调用
            void store_result() {
                if (task_) {
                    // 如果被取消了, 设置 CancelledError 异常
                    if (cancelled_.load(std::memory_order_acquire)) {
                        task_->exception_ = detail::cancelled_exception();
                    }
                    // 如果 variant 的 index 为 1 (存储了 T), 移动结果
                    else if (result_.index() == 1) {
                        task_->result_.emplace(std::move(std::get<1>(result_)));
                    }
                    // 如果有异常, 也传递过去
                    if (exception_) {
                        task_->exception_ = std::move(exception_);
                    }
                    task_->ready_ = true; // 标记为已完成
                } else if (exception_) {
                    // detach 的任务: 异常无处可去, 交给全局回调报告
                    // (对标 Python 的 "Task exception was never retrieved")
                    detail::detached_exception_handler()(exception_);
                }
            }

            /// 标记协程帧已完成, 防止 Task 析构时 double-free。
            /// 帧销毁前清空 Task 持有的句柄 (promise 是 Task 的嵌套类,
            /// 可访问其私有成员 handle_): 使「handle_ 非空 ⟺ 帧存活」
            /// 成为不变量, 从根源消灭悬空句柄的 UB 类别。
            /// (不依赖任何堆上标志: 本函数总在帧销毁前运行, 此时 task_
            /// 必然有效或为 null, 直接清句柄即可 —— 省掉每协程一次堆分配)
            void release_handle() {
                if (task_)
                    task_->handle_ = nullptr;
            }

            // ---- 状态成员 ----

            // 使用 variant 临时存储结果 (因为 return_value 可能先于
            // 异常发生, 需要在 final_suspend 时才确定最终状态)
            std::variant<std::monostate, T> result_;

            // 异常指针 (如果协程抛出了未捕获的异常)
            std::exception_ptr exception_;

            // 等待当前协程完成的那个协程的句柄
            // 例如: A 中 co_await B → B 的 continuation_ = A 的句柄
            // B 完成时, 事件循环恢复 A
            std::coroutine_handle<> continuation_;

            // 等待者所在的事件循环 (设置 continuation_ 的同时捕获 ——
            // await_suspend 运行在等待者线程上, EventLoop::get() 即其 loop)。
            // 完成时把 continuation 路由回这里, 而不是完成者线程的 loop。
            EventLoop* continuation_loop_ = nullptr;

            // 指向拥有此 promise 的 Task 对象
            // 作用: final_suspend 时把结果写入正确的 Task
            // 注意: Task 被移动时需要更新此指针
            Task* task_ = nullptr;

            // 是否已被外部取消 (Task::cancel() 设置)
            // 设 true 后, 协程在下一个 await 点抛出 CancelledError
            // atomic: cancel 可能从其他线程调用 (Scheduler 场景)
            // 放在最后避免影响 MSVC 协程帧布局
            std::atomic<bool> cancelled_ = false;

            // 协程当前是否挂起在某个 await 点 (由 wrapper 维护)
            // Task::cancel 据此区分「挂起中」(需强制唤醒) 与
            // 「还在就绪队列中」(无需唤醒, 避免 double-schedule)
            bool suspended_ = false;

            // 底层 I/O 是否挂起 (wrapper 在 await_suspend 置 true, await_resume 置 false)
            // cancel() 据此判断是否调用取消钩子: pending_io_ = true 时钩子有效。
            bool pending_io_ = false;

            // 目标事件循环 (Scheduler 分发用; nullptr = 当前线程的 loop)
            // 必须在 start()/首次 co_await 之前由 bind_loop() 设置
            EventLoop* target_loop_ = nullptr;

            // ---- 取消钩子 (挂起在网络 I/O 上时由 wrapper 注册) ----
            // Task::cancel() 时先取消底层 I/O (CancelIoEx / ASYNC_CANCEL),
            // 由完成包唤醒协程, 保证 OVERLAPPED 先于协程帧销毁被消费。
            // 仅事件循环线程访问 (cancel 由事件循环线程发起)。
            void (*cancel_hook_)(void*) = nullptr;
            void* cancel_hook_self_ = nullptr;
        };

        // ==================================================================
        // 构造 / 析构 / 移动
        // ==================================================================

        Task() = default;

        /// 从 coroutine_handle 构造 (由 promise_type::get_return_object 调用)
        Task(std::coroutine_handle<promise_type> h) : handle_(h) {
            if (handle_) {
                // 建立双向关联: promise 知道 Task 的位置
                handle_.promise().task_ = this;
            }
        }

        /// 析构: 如果协程还没有完成, 强制销毁协程帧。
        /// 如果协程已完成 (final_suspend 不挂起, 帧已自动销毁), 不 destroy。
        /// 不变量: handle_ 非空 ⟺ 帧存活 (release_handle/cancel/detach
        /// 都在帧销毁前置空 handle_), 因此非空即可安全 destroy。
        ~Task() {
            if (handle_) {
                EventLoop* owner = handle_.promise().target_loop_;
                EventLoop& loop = owner ? *owner : EventLoop::get();
                if (started_ && !ready_) {
                    auto& promise = handle_.promise();
                    if (promise.pending_io_ && promise.cancel_hook_) {
                        // OVERLAPPED/uring_op 位于协程帧内，不能在内核仍持有
                        // 指针时直接 destroy。先把帧标成废弃并请求取消；完成
                        // 包/CQE 到达后 EventLoop 只销毁、不恢复该帧。
                        promise.task_ = nullptr;
                        loop.mark_io_abandoned(handle_);
                        promise.cancel_hook_(promise.cancel_hook_self_);
                    } else if (!loop.mark_abandoned(handle_)) {
                        // 不在就绪队列 (挂起在定时器/同步原语等): 安全销毁
                        // 定时器路径: sleep_awaiter 析构会置 token, 事件循环跳过僵尸条目
                        loop.on_coroutine_finished(handle_);
                        handle_.destroy();
                    }
                    // 在就绪队列: mark_abandoned 返回 true, 帧保持存活
                    // 事件循环 resume 前检测废弃集合, 安全销毁
                } else {
                    handle_.destroy();
                }
            }
        }

        /// 移动构造: 转移所有权, 更新 promise 中的 task_ 指针。
        /// handle_ 非空即帧存活 (见析构注释), 无需额外标志判断。
        Task(Task&& other) noexcept(std::is_nothrow_move_constructible_v<T>)
            : handle_(std::exchange(other.handle_, nullptr)), result_(std::move(other.result_)),
              exception_(std::move(other.exception_)), ready_(other.ready_), started_(other.started_) {
            if (handle_) {
                handle_.promise().task_ = this; // 重定位指针
            }
        }

        /// 移动赋值: 先销毁旧协程 (如果有), 再转移所有权
        Task& operator=(Task&& other) noexcept(std::is_nothrow_move_constructible_v<T>) {
            if (this != &other) {
                if (handle_) {
                    EventLoop* owner = handle_.promise().target_loop_;
                    EventLoop& loop = owner ? *owner : EventLoop::get();
                    // 销毁旧帧 (同析构逻辑): 在就绪队列则延迟销毁
                    if (started_ && !ready_) {
                        auto& promise = handle_.promise();
                        if (promise.pending_io_ && promise.cancel_hook_) {
                            promise.task_ = nullptr;
                            loop.mark_io_abandoned(handle_);
                            promise.cancel_hook_(promise.cancel_hook_self_);
                        } else if (!loop.mark_abandoned(handle_)) {
                            loop.on_coroutine_finished(handle_);
                            handle_.destroy();
                        }
                    } else {
                        handle_.destroy();
                    }
                }
                handle_ = std::exchange(other.handle_, nullptr);
                result_.reset();
                if (other.result_)
                    result_.emplace(std::move(*other.result_));
                exception_ = std::move(other.exception_);
                ready_ = other.ready_;
                started_ = other.started_;
                if (handle_)
                    handle_.promise().task_ = this;
            }
            return *this;
        }

        Task(const Task&) = delete; // 协程句柄不可复制
        Task& operator=(const Task&) = delete;

        // ==================================================================
        // Awaitable 接口 — 使 Task 可以被 co_await
        // ==================================================================
        //
        // 当协程 A 中写 co_await task_B 时, 编译器会:
        //   1. 调用 task_B.await_ready()
        //      - 如果返回 true (B 已完成), 直接调用 await_resume() 获取结果
        //      - 如果返回 false (B 未完成), 继续步骤 2
        //   2. 调用 task_B.await_suspend(A 的 coroutine_handle)
        //      - 这里我们设置 B 的 continuation 为 A
        //      - 如果是第一次 await (惰性启动), 将 B 加入就绪队列
        //   3. A 被挂起, 控制权返回事件循环
        //   4. B 完成后, final_suspend 将 A 加入就绪队列
        //   5. 事件循环恢复 A, A 调用 task_B.await_resume() 获取结果
        // ==================================================================

        bool await_ready() const noexcept {
            // 如果任务已完成, 直接取结果, 不需要挂起
            return ready_;
        }

        void await_suspend(std::coroutine_handle<> continuation) {
            assert(handle_ && "Task has no coroutine (already completed?)");
            // 设置 continuation: 当前协程完成时恢复谁。
            // 同时捕获等待者的 loop: 此刻正运行在等待者线程上。
            handle_.promise().continuation_ = continuation;
            handle_.promise().continuation_loop_ = &EventLoop::get();

            // 惰性启动: 只有第一次 await 时才将协程送入事件循环
            if (!started_) {
                started_ = true;
                EventLoop& loop = handle_.promise().target_loop_ ? *handle_.promise().target_loop_ : EventLoop::get();
                loop.on_coroutine_started(handle_); // 活跃计数 +1 (记到目标 loop)
                loop.schedule(handle_);
            }
            // 如果已经启动了 (例如被 spawn 过):
            //   continuation 已设置, 协程完成时会自动恢复
            //
            // 注: 这里本可做对称转移 (return handle_ 直跳子协程, 省一次
            // 队列往返), 但 MSVC 要求 awaiter 临时对象支持尾调用 ——
            // cancel_check_awaiter 的析构函数 (取消清理必需) 使 C4737
            // 无法满足。启用需先重设计等待者清理协议 (见架构文档)。
        }

        T await_resume() {
            // 如果协程抛出了异常, 在这里重新抛出
            if (exception_) {
                std::rethrow_exception(exception_);
            }
            // 返回结果 (result_ 在 final_suspend 时已被设置)
            assert(result_.has_value());
            return std::move(*result_);
        }

        // ==================================================================
        // 公开 API
        // ==================================================================

        bool is_ready() const noexcept { return ready_; }
        bool is_started() const noexcept { return started_; }

        /// 显式启动协程 (不通过 co_await)
        /// 用于 spawn 模式: 立即让协程在后台运行, 后续再 await
        void start() {
            if (!started_ && handle_) {
                started_ = true;
                EventLoop& loop = handle_.promise().target_loop_ ? *handle_.promise().target_loop_ : EventLoop::get();
                loop.on_coroutine_started(handle_); // 活跃计数 +1 (记到目标 loop)
                loop.schedule(handle_);
            }
        }

        /// 获取底层的 coroutine_handle (供内部使用)
        std::coroutine_handle<promise_type> handle() const { return handle_; }

        /// 绑定目标事件循环 (Scheduler 分发用)。
        /// 必须在 start()/首次 co_await 之前调用; 之后协程的调度/定时器/
        /// 取消唤醒都路由到该 loop。nullptr 重置为"当前线程的 loop"。
        void bind_loop(EventLoop* loop) noexcept {
            if (handle_)
                handle_.promise().target_loop_ = loop;
        }

        /// 取消此协程 (对标 Python Task.cancel())
        ///   置 cancelled_ 标志后, 协程在下一个 await 点抛出 CancelledError,
        ///   协程体立即终止 (栈上对象析构 = 清理逻辑), 任务带异常完成。
        ///   任何 co_await 此 Task 的协程将在 await_resume 时收到 CancelledError。
        ///   若协程尚未启动, 直接标记为已完成(带异常)并销毁帧。
        ///   注意: 请在事件循环线程调用 (wait_for 的定时器等内部路径已满足)。
        void cancel() {
            if (handle_ && !ready_) {
                auto& p = handle_.promise();
                p.cancelled_.store(true, std::memory_order_release);
                if (!started_) {
                    // 尚未启动: 存异常结果 → 清回指 → 销毁帧 (防止泄漏)。
                    // 顺序要点:
                    //   - release_handle 会清空 task_ 指向的 Task 的 handle_
                    //     (此处即本 Task 自身), 且只能在本帧销毁前调用
                    //     (之后 promise 悬空) → 先保存帧句柄副本再 destroy
                    auto h = handle_; // 帧句柄副本 (release_handle 会清空 handle_)
                    p.store_result();
                    p.release_handle();
                    h.destroy();
                    handle_ = nullptr;
                } else if (p.pending_io_ && p.cancel_hook_) {
                    // 挂起在网络 I/O 上: 先取消底层 I/O, 由完成包唤醒协程。
                    // pending_io_ 确保钩子在 I/O 完成后不再被调用。
                    // 不直接 schedule: 必须等完成包先消费 OVERLAPPED,
                    // 协程帧销毁才是安全的。
                    p.cancel_hook_(p.cancel_hook_self_);
                } else if (p.suspended_) {
                    // 挂起在时间/同步/任务等待上: 强制唤醒,
                    // 协程恢复时在 await_resume 处抛出 CancelledError
                    // 调度到目标 loop (而非调用者线程的 loop), 同 Task<void>
                    EventLoop& loop = p.target_loop_ ? *p.target_loop_ : EventLoop::get();
                    loop.schedule(handle_);
                }
                // else: 协程已在就绪队列中 (spawn 后尚未运行),
                //   不重复 schedule (double-schedule 会 resume 已销毁的帧 → UB)。
                //   它运行到第一个 await 点时自然抛出 CancelledError。
            }
        }

        /// 等待者协程帧被销毁时的清理回调 (由 cancel_check_awaiter 析构调用)
        /// 作用: 清除指向已销毁协程的 continuation, 防止 final_suspend
        /// 调度一个已销毁的句柄 (UB)。
        /// 注意: 若本任务已完成 (final_suspend 不挂起, 帧已销毁),
        /// handle_ 已被 release_handle 置空, 直接跳过 → 永不悬空访问 promise。
        void on_waiter_destroyed(std::coroutine_handle<> h) noexcept {
            if (handle_ && handle_.promise().continuation_ == h)
                handle_.promise().continuation_ = nullptr;
        }

        /// 非协程上下文取结果 (main / coro::run 使用): 有异常则重新抛出
        T take_result() {
            if (exception_)
                std::rethrow_exception(exception_);
            assert(result_.has_value());
            return std::move(*result_);
        }

        /// 放弃所有权 (detach): 协程帧不再由此 Task 管理,
        /// 协程自行运行到完成 (final_suspend 时自动销毁帧)。
        /// 用于 spawn 但不需要 await 的场景。
        void detach() noexcept {
            if (handle_) {
                handle_.promise().task_ = nullptr; // 清除回指指针
            }
            handle_ = nullptr;
            ready_ = true; // 防止后续 await
        }

      private:
        std::coroutine_handle<promise_type> handle_; // 协程句柄 (非空 ⟺ 帧存活)
        std::optional<T> result_;                    // 协程结果 (完成后才有值)
        std::exception_ptr exception_;               // 协程异常 (如果有)
        bool ready_ = false;                         // 是否已完成
        bool started_ = false;                       // 是否已启动
    };

    // ============================================================================
    // Task<void> — void 特化
    // ============================================================================
    //
    // 与 Task<T> 几乎相同, 区别:
    //   - promise_type 使用 return_void() 代替 return_value()
    //   - 不需要 result_ 成员 (没有返回值)
    //   - await_resume() 返回 void
    // ============================================================================
    template <> class Task<void> {
      public:
        struct promise_type {
            Task get_return_object() { return Task{std::coroutine_handle<promise_type>::from_promise(*this)}; }

            std::suspend_always initial_suspend() noexcept { return {}; }

            detail::final_awaiter<promise_type> final_suspend() noexcept { return {}; }

            void unhandled_exception() { exception_ = std::current_exception(); }

            // 取消注入机制: 同 Task<T>::promise_type
            template <typename Awaiter> auto await_transform(Awaiter&& awaiter) {
                return detail::cancel_check_awaiter<promise_type, Awaiter&&>(std::forward<Awaiter>(awaiter), this,
                                                                             &cancelled_);
            }

            /// co_return; 或协程体自然结束 → 调用 return_void()
            void return_void() noexcept {}

            void store_result() {
                if (task_) {
                    // 如果被取消了, 设置 CancelledError 异常
                    if (cancelled_.load(std::memory_order_acquire)) {
                        task_->exception_ = detail::cancelled_exception();
                    } else if (exception_) {
                        task_->exception_ = std::move(exception_);
                    }
                    task_->ready_ = true;
                } else if (exception_) {
                    // detach 的任务: 异常无处可去, 交给全局回调报告
                    detail::detached_exception_handler()(exception_);
                }
            }

            /// 标记协程帧已完成, 防止 Task 析构时 double-free。
            /// 帧销毁前清空 Task 持有的句柄 (同 Task<T>): handle_ 永不悬空。
            void release_handle() {
                if (task_)
                    task_->handle_ = nullptr;
            }

            std::exception_ptr exception_;
            std::coroutine_handle<> continuation_;
            EventLoop* continuation_loop_ = nullptr; // 等待者的 loop (同 Task<T>)
            Task* task_ = nullptr;
            std::atomic<bool> cancelled_ = false; // atomic: cancel 可跨线程 (Scheduler 场景)
            bool suspended_ = false;              // 挂起标记 (同 Task<T>)
            bool pending_io_ = false;             // 底层 I/O 挂起标记 (同 Task<T>)
            EventLoop* target_loop_ = nullptr;    // 目标事件循环 (同 Task<T>)

            // 取消钩子: 同 Task<T>::promise_type
            void (*cancel_hook_)(void*) = nullptr;
            void* cancel_hook_self_ = nullptr;
        };

        // ---- 构造/析构/移动 (与 Task<T> 相同) ----
        Task() = default;
        Task(std::coroutine_handle<promise_type> h) : handle_(h) {
            if (handle_)
                handle_.promise().task_ = this;
        }
        ~Task() {
            if (handle_) {
                EventLoop* owner = handle_.promise().target_loop_;
                EventLoop& loop = owner ? *owner : EventLoop::get();
                if (started_ && !ready_) {
                    auto& promise = handle_.promise();
                    if (promise.pending_io_ && promise.cancel_hook_) {
                        promise.task_ = nullptr;
                        loop.mark_io_abandoned(handle_);
                        promise.cancel_hook_(promise.cancel_hook_self_);
                    } else if (!loop.mark_abandoned(handle_)) {
                        loop.on_coroutine_finished(handle_);
                        handle_.destroy();
                    }
                } else {
                    handle_.destroy();
                }
            }
        }

        Task(Task&& other) noexcept
            : handle_(std::exchange(other.handle_, nullptr)), exception_(std::move(other.exception_)),
              ready_(other.ready_), started_(other.started_) {
            if (handle_)
                handle_.promise().task_ = this;
        }

        Task& operator=(Task&& other) noexcept {
            if (this != &other) {
                if (handle_) {
                    EventLoop* owner = handle_.promise().target_loop_;
                    EventLoop& loop = owner ? *owner : EventLoop::get();
                    if (started_ && !ready_) {
                        auto& promise = handle_.promise();
                        if (promise.pending_io_ && promise.cancel_hook_) {
                            promise.task_ = nullptr;
                            loop.mark_io_abandoned(handle_);
                            promise.cancel_hook_(promise.cancel_hook_self_);
                        } else if (!loop.mark_abandoned(handle_)) {
                            loop.on_coroutine_finished(handle_);
                            handle_.destroy();
                        }
                    } else {
                        handle_.destroy();
                    }
                }
                handle_ = std::exchange(other.handle_, nullptr);
                exception_ = std::move(other.exception_);
                ready_ = other.ready_;
                started_ = other.started_;
                if (handle_)
                    handle_.promise().task_ = this;
            }
            return *this;
        }

        Task(const Task&) = delete;
        Task& operator=(const Task&) = delete;

        // ---- Awaitable ----
        bool await_ready() const noexcept { return ready_; }
        void await_suspend(std::coroutine_handle<> continuation) {
            assert(handle_);
            handle_.promise().continuation_ = continuation;
            handle_.promise().continuation_loop_ = &EventLoop::get(); // 等待者的 loop
            if (!started_) {
                started_ = true;
                EventLoop& loop = handle_.promise().target_loop_ ? *handle_.promise().target_loop_ : EventLoop::get();
                loop.on_coroutine_started(handle_); // 活跃计数 +1 (记到目标 loop)
                loop.schedule(handle_);
            }
            // 对称转移不可用的原因同 Task<T>::await_suspend 注释 (MSVC C4737)
        }
        void await_resume() {
            if (exception_)
                std::rethrow_exception(exception_);
        }

        // ---- API ----
        bool is_ready() const noexcept { return ready_; }
        bool is_started() const noexcept { return started_; }
        void start() {
            if (!started_ && handle_) {
                started_ = true;
                EventLoop& loop = handle_.promise().target_loop_ ? *handle_.promise().target_loop_ : EventLoop::get();
                loop.on_coroutine_started(handle_); // 活跃计数 +1 (记到目标 loop)
                loop.schedule(handle_);
            }
        }
        std::coroutine_handle<promise_type> handle() const { return handle_; }

        /// 绑定目标事件循环 (同 Task<T>::bind_loop)
        void bind_loop(EventLoop* loop) noexcept {
            if (handle_)
                handle_.promise().target_loop_ = loop;
        }

        /// 取消此协程 (同 Task<T>::cancel())
        /// 请在事件循环线程调用。
        void cancel() {
            if (handle_ && !ready_) {
                auto& p = handle_.promise();
                p.cancelled_.store(true, std::memory_order_release);
                if (!started_) {
                    // 尚未启动: 存异常结果 → 清回指 → 销毁帧 (顺序同 Task<T>)
                    auto h = handle_; // 帧句柄副本 (release_handle 会清空 handle_)
                    p.store_result();
                    p.release_handle();
                    h.destroy();
                    handle_ = nullptr;
                } else if (p.pending_io_ && p.cancel_hook_) {
                    // 挂起在网络 I/O 上: 取消底层 I/O, 由完成包唤醒
                    // pending_io_ 确保钩子在 I/O 完成后不再被调用
                    p.cancel_hook_(p.cancel_hook_self_);
                } else if (p.suspended_) {
                    // 挂起中: 强制唤醒 — 调度到目标 loop (而非调用者线程的 loop)
                    EventLoop& loop = p.target_loop_ ? *p.target_loop_ : EventLoop::get();
                    loop.schedule(handle_);
                }
                // else: 已在就绪队列中, 运行到第一个 await 点自然抛出
            }
        }

        /// 等待者协程帧被销毁时的清理回调 (同 Task<T>)
        void on_waiter_destroyed(std::coroutine_handle<> h) noexcept {
            if (handle_ && handle_.promise().continuation_ == h)
                handle_.promise().continuation_ = nullptr;
        }

        /// 非协程上下文取结果 (void 版): 有异常则重新抛出
        void take_result() {
            if (exception_)
                std::rethrow_exception(exception_);
        }

        void detach() noexcept {
            if (handle_) {
                handle_.promise().task_ = nullptr;
            }
            handle_ = nullptr;
            ready_ = true;
        }

      private:
        std::coroutine_handle<promise_type> handle_; // 协程句柄 (非空 ⟺ 帧存活)
        std::exception_ptr exception_;
        bool ready_ = false;
        bool started_ = false;
    };

    // ============================================================================
    // spawn — 立即将 Task 投入事件循环后台运行
    // ============================================================================
    //
    // 类似 Python 的 asyncio.create_task():
    //   t = asyncio.create_task(some_coro())  →  auto t = coro::spawn(some_coro());
    //   await t                                →  co_await std::move(t);
    //
    // 与直接 co_await 的区别:
    //   co_await task  — 启动 task 并立即等待它完成 (串行)
    //   spawn(task)    — 启动 task 但不等待, 返回句柄让你稍后再 await (可并发)
    //
    // 使用场景:
    //   auto t1 = spawn(work1());  // t1 开始在后台运行
    //   auto t2 = spawn(work2());  // t2 也开始了, 与 t1 并发
    //   auto r1 = co_await std::move(t1);  // 等待 t1
    //   auto r2 = co_await std::move(t2);  // 等待 t2
    // ============================================================================
    template <typename T> Task<T> spawn(Task<T> task) {
        task.start();           // 立即启动
        return std::move(task); // 返回句柄供后续 await
    }

} // namespace coro
