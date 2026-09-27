// test_pool.cpp — coro::Pool<T> / PoolLease<T> 的所有权与配额语义 (计划 M4d)
//
// 覆盖: RAII 归还与复用、不超发、discard 让位、等待者被取消后不残留、
// 池先于借出句柄析构仍然安全、工厂失败不卡死。
#include <gtest/gtest.h>

#include <coro/coro.hpp>
#include <coro/pool.hpp>

#include "test_util.h"

#include <atomic>
#include <chrono>
#include <memory>
#include <optional>
#include <stdexcept>
#include <vector>

using namespace std::chrono_literals;

namespace {

    /// 可辨识的资源: 记录它是第几个被工厂造出来的 (只允许移动, 连接类资源本就不可复制)
    struct probe {
        int id = 0;
        probe() = default;
        explicit probe(int i) : id(i) {}
        probe(probe&&) noexcept = default;
        probe& operator=(probe&&) noexcept = default;
        probe(const probe&) = delete;
        probe& operator=(const probe&) = delete;
    };

    struct probe_fixture {
        std::atomic<int> created{0};
        std::atomic<int> fail_at{0};     ///< 第 N 次建连抛异常 (0 = 不抛)
        std::atomic<int> delay_ticks{0}; ///< >0 时建连前多让出若干次, 制造并发窗口
    };

    /// 命名协程而不是 lambda 协程: 临时 lambda 协程的闭包会先于挂起点销毁
    coro::Task<probe> make_probe(probe_fixture* fx) {
        const int ticks = fx->delay_ticks.load();
        for (int i = 0; i < ticks; ++i)
            co_await coro::yield();
        const int n = fx->created.fetch_add(1) + 1;
        if (fx->fail_at.load() != 0 && n >= fx->fail_at.load())
            throw std::runtime_error("factory failed");
        co_return probe{n};
    }

    using pool_t = coro::Pool<probe>;

    pool_t make_pool(probe_fixture* fx, std::size_t max_agents, std::size_t max_idle) {
        pool_t::options o;
        o.max_agents = max_agents;
        o.max_idle = max_idle;
        return pool_t([fx]() -> coro::Task<probe> { co_return co_await make_probe(fx); }, o);
    }

    coro::Task<> reuse_scenario(probe_fixture* fx, std::vector<int>* seen) {
        auto pool = make_pool(fx, 4, 4);
        {
            auto lease = co_await pool.acquire();
            seen->push_back(lease->id);
        } // 析构自动归还
        auto second = co_await pool.acquire();
        seen->push_back(second->id);
        co_return;
    }

    coro::Task<> holder_task(pool_t* pool, std::atomic<int>* holders, std::atomic<int>* peak) {
        auto lease = co_await pool->acquire();
        if (!lease.valid())
            co_return;
        const int now = holders->fetch_add(1) + 1;
        int old = peak->load();
        while (now > old && !peak->compare_exchange_weak(old, now)) {
        }
        co_await coro::sleep(15ms);
        holders->fetch_sub(1);
        co_return;
    }

    coro::Task<> cap_scenario(probe_fixture* fx, std::size_t* peak_in_use) {
        auto pool = make_pool(fx, 2, 2);
        fx->delay_ticks.store(3); // 建连跨几次 yield, 让并发窗口真实存在
        std::atomic<int> holders{0};
        std::atomic<int> peak{0};
        std::vector<coro::Task<>> jobs;
        for (int i = 0; i < 5; ++i)
            jobs.push_back(coro::spawn(holder_task(&pool, &holders, &peak)));
        for (auto& j : jobs)
            co_await std::move(j);
        fx->delay_ticks.store(0);
        *peak_in_use = static_cast<std::size_t>(peak.load());
        co_return;
    }

    coro::Task<> discard_scenario(probe_fixture* fx, std::size_t* after_idle) {
        auto pool = make_pool(fx, 4, 4);
        auto lease = co_await pool.acquire();
        (void)lease->id;
        lease.discard(); // 模拟"连接已被对端关掉": 不该回到空闲队列
        *after_idle = pool.idle_count();
        co_return;
    }

    coro::Task<> wait_and_report(pool_t* pool, std::atomic<bool>* cancelled) {
        try {
            auto lease = co_await pool->acquire();
            (void)lease;
        } catch (const coro::CancelledError&) {
            cancelled->store(true);
        }
        co_return;
    }

    coro::Task<> cancel_waiter_scenario(probe_fixture* fx, std::atomic<bool>* cancelled, std::size_t* waiting_after) {
        auto pool = make_pool(fx, 1, 1);
        auto holder = std::make_shared<pool_t::lease_type>(co_await pool.acquire());
        EXPECT_TRUE(holder->valid());
        co_await coro::yield();

        auto waiter = std::make_shared<coro::Task<>>(coro::spawn(wait_and_report(&pool, cancelled)));
        // 必须等到等待者真正挂上去再取样: spawn 的任务何时推进不由本协程决定,
        // 单次 yield 不保证它已经走到 await 点 (那会把它"没排队"误读成"队列空")。
        co_await coro::sleep(5ms);
        *waiting_after = pool.waiting_count();
        auto* loop = &coro::EventLoop::get();
        loop->dispatch([waiter] { waiter->cancel(); });
        co_await coro::sleep(40ms);
        co_await std::move(*waiter);

        holder.reset(); // 归还名额
        auto again = co_await pool.acquire();
        EXPECT_TRUE(again.valid()) << "被取消的等待者把名额吃掉了";
        co_return;
    }

    coro::Task<> pool_dies_before_lease(probe_fixture* fx, bool* still_valid) {
        std::optional<pool_t::lease_type> held;
        {
            auto pool = make_pool(fx, 4, 4);
            auto lease = co_await pool.acquire();
            *still_valid = lease.valid();
            held.emplace(std::move(lease));
        } // pool 先析构
        held.reset(); // lease 之后才析构: 归还给已关闭的状态, 不应崩
        co_return;
    }

    coro::Task<> factory_failure_scenario(probe_fixture* fx, bool* threw) {
        auto pool = make_pool(fx, 2, 2);
        fx->fail_at.store(1);
        try {
            auto lease = co_await pool.acquire();
            (void)lease;
        } catch (const std::runtime_error&) {
            *threw = true;
        }
        fx->fail_at.store(0);
        auto ok = co_await pool.acquire();
        EXPECT_TRUE(ok.valid()) << "一次建连失败后池卡死了";
        EXPECT_EQ(pool.in_use_count(), 1u) << "失败没有归还名额";
        co_return;
    }

} // namespace

TEST(PoolTest, ReusesIdleResource) {
    probe_fixture fx;
    std::vector<int> seen;
    test_util::run_task([&] { return reuse_scenario(&fx, &seen); });
    ASSERT_EQ(seen.size(), 2u);
    EXPECT_EQ(seen[0], seen[1]) << "归还后应复用同一个资源";
    EXPECT_EQ(fx.created.load(), 1) << "复用不该再建一次";
}

TEST(PoolTest, NeverExceedsMaxAgents) {
    probe_fixture fx;
    std::size_t peak = 0;
    test_util::run_task([&] { return cap_scenario(&fx, &peak); });
    EXPECT_LE(peak, 2u) << "同时借出数超过 max_agents: 池在超发";
    EXPECT_GE(fx.created.load(), 2) << "配额内应至少建了两个资源";
}

TEST(PoolTest, DiscardRemovesSlotInsteadOfReturning) {
    probe_fixture fx;
    std::size_t after_idle = 99;
    test_util::run_task([&] { return discard_scenario(&fx, &after_idle); });
    EXPECT_EQ(after_idle, 0u) << "discard 的资源不该回到空闲队列";
}

TEST(PoolTest, CancelledWaiterUnlinksAndFreesQuota) {
    probe_fixture fx;
    std::atomic<bool> cancelled{false};
    std::size_t waiting_after = 0;
    test_util::run_task([&] { return cancel_waiter_scenario(&fx, &cancelled, &waiting_after); });
    EXPECT_GE(waiting_after, 1u) << "第二个请求应当确实在排队";
    EXPECT_TRUE(cancelled.load()) << "取消必须以 CancelledError 浮现";
}

TEST(PoolTest, PoolOutlivedByItsLease) {
    probe_fixture fx;
    bool still_valid = false;
    test_util::run_task([&] { return pool_dies_before_lease(&fx, &still_valid); });
    EXPECT_TRUE(still_valid);
}

TEST(PoolTest, FactoryFailureFreesQuotaAndRethrows) {
    probe_fixture fx;
    bool threw = false;
    test_util::run_task([&] { return factory_failure_scenario(&fx, &threw); });
    EXPECT_TRUE(threw) << "工厂异常必须原样上抛, 不能伪装成空句柄";
}

TEST(PoolTest, NullFactoryRejectedAtConstruction) {
    pool_t::options o;
    EXPECT_THROW(pool_t(typename pool_t::factory_fn{}, o), coro::StructuredConcurrencyError);
}

TEST(PoolTest, ClosedPoolGivesInvalidLease) {
    probe_fixture fx;
    auto pool = make_pool(&fx, 4, 4);
    coro::run([&pool]() -> coro::Task<> {
        pool.close();
        auto lease = co_await pool.acquire();
        EXPECT_FALSE(lease.valid()) << "已关闭的池必须给出无效句柄";
    }());
}
