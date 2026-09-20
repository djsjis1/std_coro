// test_lifecycle.cpp — 资源生命周期: Task/Queue/Lock/Event 在异常/提前退出时的清理
#include <gtest/gtest.h>

#include <coro/coro.hpp>
#if defined(_WIN32) || (defined(__linux__) && (!defined(CORO_HAS_URING) || CORO_HAS_URING))
#include <coro/net.hpp>
#endif

#include "test_util.h"

#include <atomic>
#include <memory>
#include <vector>

using namespace std::chrono_literals;

namespace {

    // ── RAII 追踪器: 检测析构是否执行 ──
    struct DestroyTracker {
        int* count;
        explicit DestroyTracker(int* c) : count(c) {}
        ~DestroyTracker() { ++*count; }
    };

    struct AtomicDestroyTracker {
        std::atomic<int>* count;
        explicit AtomicDestroyTracker(std::atomic<int>* c) : count(c) {}
        ~AtomicDestroyTracker() { count->fetch_add(1); }
    };

    // ── Task 在 I/O 挂起时被销毁 ──
    coro::Task<> io_pending_task(int* destroyed) {
        DestroyTracker tracker(destroyed);
        co_await coro::sleep(10s);
    }

    coro::Task<> destroy_during_io(int* destroyed) {
        {
            auto t = coro::spawn(io_pending_task(destroyed));
            co_await coro::yield();
        }
        co_await coro::sleep(10ms);
    }

    // ── Lock 在持有者异常退出时释放 ──
    coro::Task<> lock_holder_exception(coro::Lock* lock, bool* acquired_after) {
        auto g = co_await lock->guard();
        throw std::runtime_error("holder failed");
        co_return;
    }

    coro::Task<> lock_exception_cleanup(bool* acquired_after) {
        coro::Lock lock;
        try {
            auto holder = coro::spawn(lock_holder_exception(&lock, acquired_after));
            co_await std::move(holder);
        } catch (const std::runtime_error&) {
        }
        auto g2 = co_await lock.guard();
        *acquired_after = true;
    }

    // ── Event 在等待者被取消时清理 ──
    coro::Task<> event_waiter_cancel(coro::Event* ev, bool* woken) {
        co_await ev->wait();
        *woken = true;
    }

    coro::Task<> event_cancel_cleanup(bool* woken) {
        coro::Event ev;
        auto waiter = coro::spawn(event_waiter_cancel(&ev, woken));
        co_await coro::yield();
        waiter.cancel();
        try {
            co_await std::move(waiter);
        } catch (const coro::CancelledError&) {
        }
        ev.set();
    }

    // ── 多个 Task 同时销毁 ──
    coro::Task<> batch_destroy_worker(std::atomic<int>* destroyed) {
        AtomicDestroyTracker tracker(destroyed);
        co_await coro::sleep(10s);
    }

    coro::Task<> batch_destroy(std::atomic<int>* destroyed_count) {
        std::vector<coro::Task<>> tasks;
        for (int i = 0; i < 10; ++i)
            tasks.push_back(batch_destroy_worker(destroyed_count));

        for (auto& t : tasks)
            t.start();
        co_await coro::yield();
        tasks.clear();
        co_await coro::sleep(10ms);
    }

    coro::Task<> tracked_delay(int* destroyed) {
        DestroyTracker tracker(destroyed);
        co_await coro::sleep(1ms);
    }

#if defined(_WIN32) || (defined(__linux__) && (!defined(CORO_HAS_URING) || CORO_HAS_URING))
    coro::Task<> pending_socket_read(coro::net::TcpListener* listener, bool* entered, int* destroyed) {
        auto conn = co_await listener->accept();
        if (!conn.valid())
            co_return;

        DestroyTracker tracker(destroyed);
        *entered = true;
        char byte = 0;
        (void)co_await conn.read(&byte, 1);
    }

    coro::Task<> destroy_during_socket_read(bool* bound, bool* connected, bool* entered, int* destroyed) {
        coro::net::TcpListener listener;
        unsigned short port = 0;
        for (unsigned short candidate = 19100; candidate < 19120; ++candidate) {
            if (listener.bind_listen("127.0.0.1", candidate)) {
                port = candidate;
                break;
            }
        }
        if (port == 0)
            co_return;
        *bound = true;

        coro::net::TcpStream client;
        {
            auto reader = coro::spawn(pending_socket_read(&listener, entered, destroyed));
            client = co_await coro::net::TcpStream::connect("127.0.0.1", port);
            *connected = client.valid();
            for (int i = 0; i < 50 && !*entered; ++i)
                co_await coro::sleep(1ms);
        } // 析构仍挂在 read 上的 Task：必须先取消 IO，再延迟销毁帧

        co_await coro::sleep(50ms); // 让取消完成包/CQE 被事件循环消费
        client.close();
    }
#endif

    // ── 嵌套 Task 异常清理 ──
    coro::Task<> inner_thrower(int* cleaned) {
        DestroyTracker tracker(cleaned);
        throw std::runtime_error("inner boom");
        co_return;
    }

    coro::Task<> middle_wrapper(int* cleaned) {
        DestroyTracker tracker(cleaned);
        co_await inner_thrower(cleaned);
    }

    coro::Task<> outer_catcher(int* cleaned, bool* caught) {
        try {
            co_await middle_wrapper(cleaned);
        } catch (const std::runtime_error&) {
            *caught = true;
        }
    }

    coro::Task<> nested_exception_cleanup(int* cleaned, bool* caught) {
        auto t = coro::spawn(outer_catcher(cleaned, caught));
        co_await std::move(t);
    }

    // ── gather 中部分任务异常: 其余任务应完成 ──
    coro::Task<int> gather_fail_fast() {
        co_await coro::sleep(10ms);
        throw std::runtime_error("fast fail");
        co_return 0;
    }

    coro::Task<int> gather_slow_ok() {
        co_await coro::sleep(100ms);
        co_return 42;
    }

    coro::Task<> gather_partial_failure(int* result, bool* caught) {
        try {
            auto [a, b] = co_await coro::gather(gather_fail_fast(), gather_slow_ok());
            *result = a + b;
        } catch (const std::runtime_error&) {
            *caught = true;
        }
    }

    // ── 大量 Task 快速创建销毁 (内存泄漏检测) ──
    coro::Task<> rapid_task_lifecycle(int* created, int* destroyed) {
        std::vector<coro::Task<>> tasks;
        tasks.reserve(100);
        for (int i = 0; i < 100; ++i) {
            tasks.push_back(coro::spawn(tracked_delay(destroyed)));
            ++*created;
        }
        for (auto& task : tasks)
            co_await std::move(task);
    }

} // namespace

TEST(LifecycleTest, TaskDestroyDuringPendingIO) {
    int destroyed = 0;
    test_util::run_task([&] { return destroy_during_io(&destroyed); });
    EXPECT_GE(destroyed, 1);
}

#if defined(_WIN32) || (defined(__linux__) && (!defined(CORO_HAS_URING) || CORO_HAS_URING))
TEST(LifecycleTest, TaskDestroyDuringPendingSocketRead) {
    bool bound = false;
    bool connected = false;
    bool entered = false;
    int destroyed = 0;
    test_util::run_task([&] { return destroy_during_socket_read(&bound, &connected, &entered, &destroyed); });
    EXPECT_TRUE(bound);
    EXPECT_TRUE(connected);
    EXPECT_TRUE(entered);
    EXPECT_EQ(destroyed, 1);
}
#endif

TEST(LifecycleTest, LockReleasedOnException) {
    bool acquired_after = false;
    test_util::run_task([&] { return lock_exception_cleanup(&acquired_after); });
    EXPECT_TRUE(acquired_after);
}

TEST(LifecycleTest, BatchTaskDestroy) {
    std::atomic<int> destroyed_count{0};
    test_util::run_task([&] { return batch_destroy(&destroyed_count); });
    EXPECT_EQ(destroyed_count.load(), 10);
}

TEST(LifecycleTest, NestedExceptionCleanup) {
    int cleaned = 0;
    bool caught = false;
    test_util::run_task([&] { return nested_exception_cleanup(&cleaned, &caught); });
    EXPECT_TRUE(caught);
}

TEST(LifecycleTest, GatherPartialFailure) {
    int result = 0;
    bool caught = false;
    test_util::run_task([&] { return gather_partial_failure(&result, &caught); });
    EXPECT_TRUE(caught);
}

TEST(LifecycleTest, RapidTaskLifecycle) {
    int created = 0, destroyed = 0;
    test_util::run_task([&] { return rapid_task_lifecycle(&created, &destroyed); });
    EXPECT_EQ(created, 100);
    EXPECT_EQ(destroyed, 100);
}
