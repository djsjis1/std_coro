// test_router.cpp — radix_router 基数树动态路由单元测试
// ============================================================================
// 覆盖: 静态匹配 / :param 捕获 / *wildcard / 优先级 / DFS 回溯 /
//       根路径与尾斜杠归一 / 参数名冲突检测 / 失败插入无副作用 /
//       params 复用清理 / 非法模式拒绝 / 重复注册替换 / 千级路由基准
// ============================================================================
#include <gtest/gtest.h>

#include <radix_router.h>

#include <chrono>
#include <cstdio>
#include <string>
#include <string_view>

namespace
{
    using router_int = radix_router<int>;
    using params = router_int::params_view;
}

// ── 静态路由 ──

TEST(Router, StaticExactMatch)
{
    router_int r;
    ASSERT_TRUE(r.insert("/", 1));
    ASSERT_TRUE(r.insert("/hello", 2));
    ASSERT_TRUE(r.insert("/api/v1/users", 3));

    ASSERT_NE(r.lookup("/"), nullptr);
    EXPECT_EQ(*r.lookup("/"), 1);
    EXPECT_EQ(*r.lookup("/hello"), 2);
    EXPECT_EQ(*r.lookup("/api/v1/users"), 3);
}

TEST(Router, StaticNoPartialMatch)
{
    router_int r;
    r.insert("/api/v1/users", 1);
    // 前缀不是完整路由 → 不命中 (静态路由必须逐段完整相等)
    EXPECT_EQ(r.lookup("/api"), nullptr);
    EXPECT_EQ(r.lookup("/api/v1"), nullptr);
    EXPECT_EQ(r.lookup("/api/v1/users/42"), nullptr);
    EXPECT_EQ(r.lookup("/api/v1/usersx"), nullptr);
}

// ── 参数路由: :param 捕获 ──

TEST(Router, ParamCapture)
{
    router_int r;
    ASSERT_TRUE(r.insert("/user/:id", 10));

    params p;
    auto *v = r.lookup("/user/42", &p);
    ASSERT_NE(v, nullptr);
    EXPECT_EQ(*v, 10);
    ASSERT_EQ(p.size(), 1u);
    EXPECT_EQ(p[0].first, "id");
    EXPECT_EQ(p[0].second, "42");
}

TEST(Router, ParamRequiresSegment)
{
    router_int r;
    r.insert("/user/:id", 10);
    // /user 后面没有段, :id 无东西可捕获 → 不命中
    EXPECT_EQ(r.lookup("/user"), nullptr);
    // 尾斜杠归一后仍是 /user (value 在 param 节点, user 节点无 value)
    EXPECT_EQ(r.lookup("/user/"), nullptr);
}

TEST(Router, MultiParam)
{
    router_int r;
    ASSERT_TRUE(r.insert("/user/:uid/post/:pid", 20));

    params p;
    auto *v = r.lookup("/user/7/post/99", &p);
    ASSERT_NE(v, nullptr);
    EXPECT_EQ(*v, 20);
    ASSERT_EQ(p.size(), 2u);
    EXPECT_EQ(p[0].first, "uid");
    EXPECT_EQ(p[0].second, "7");
    EXPECT_EQ(p[1].first, "pid");
    EXPECT_EQ(p[1].second, "99");
}

// ── 通配路由: *wildcard 吞掉剩余路径 ──

TEST(Router, WildcardCaptureRest)
{
    router_int r;
    ASSERT_TRUE(r.insert("/files/*path", 30));

    params p;
    auto *v = r.lookup("/files/a/b/c.txt", &p);
    ASSERT_NE(v, nullptr);
    EXPECT_EQ(*v, 30);
    ASSERT_EQ(p.size(), 1u);
    EXPECT_EQ(p[0].first, "path");
    EXPECT_EQ(p[0].second, "a/b/c.txt");
}

TEST(Router, WildcardOnlyLast)
{
    router_int r;
    // 通配符后面还有段 → 非法模式
    EXPECT_FALSE(r.insert("/files/*path/edit", 1));
}

// ── 优先级: 静态 > 参数 > 通配 ──

TEST(Router, PriorityStaticOverParam)
{
    router_int r;
    // 故意先注册参数路由, 再注册静态路由, 验证匹配按优先级而非注册顺序
    r.insert("/user/:id", 1);
    r.insert("/user/admin", 2);

    EXPECT_EQ(*r.lookup("/user/admin"), 2); // 静态优先
    EXPECT_EQ(*r.lookup("/user/42"), 1);    // 其他值落到参数
}

TEST(Router, PriorityParamOverWildcard)
{
    router_int r;
    r.insert("/user/:id", 1);
    r.insert("/user/*rest", 2);

    params p;
    EXPECT_EQ(*r.lookup("/user/42", &p), 1); // 单段优先参数
}

// ── DFS 回溯: 参数分支深层失败时改试其他分支 ──

TEST(Router, BacktrackingParamToWildcard)
{
    router_int r;
    r.insert("/files/:lang/readme", 1);
    r.insert("/files/*any", 2);

    params p;
    // /files/go/readme 命中参数分支
    EXPECT_EQ(*r.lookup("/files/go/readme", &p), 1);

    // /files/go/x/y: :lang 捕获 go 后深层找不到 readme → 回溯到通配
    p.clear();
    auto *v = r.lookup("/files/go/x/y", &p);
    ASSERT_NE(v, nullptr);
    EXPECT_EQ(*v, 2);
    ASSERT_EQ(p.size(), 1u);
    EXPECT_EQ(p[0].first, "any");
    EXPECT_EQ(p[0].second, "go/x/y");
}

TEST(Router, BacktrackingStaticPreferredButParamFallback)
{
    router_int r;
    r.insert("/a/b/c", 1);
    r.insert("/a/:x/c", 2);

    EXPECT_EQ(*r.lookup("/a/b/c"), 1); // 静态全匹配
    EXPECT_EQ(*r.lookup("/a/z/c"), 2); // b≠z → 落到参数分支
}

// ── 根路径 & 尾斜杠归一 ──

TEST(Router, RootPath)
{
    router_int r;
    ASSERT_TRUE(r.insert("/", 1));
    ASSERT_NE(r.lookup("/"), nullptr);
    EXPECT_EQ(*r.lookup("/"), 1);
    EXPECT_EQ(r.lookup(""), nullptr); // 空路径不是合法请求目标
}

TEST(Router, TrailingSlashNormalization)
{
    router_int r;
    // 查找方向: "/user/" 归一到 "/user"
    ASSERT_TRUE(r.insert("/user", 1));
    EXPECT_EQ(*r.lookup("/user/"), 1);

    // 注册方向: "/x/" 与 "/x" 是同一路由
    ASSERT_TRUE(r.insert("/x/", 2));
    EXPECT_EQ(*r.lookup("/x"), 2);
    EXPECT_EQ(r.size(), 2u); // /user 与 /x 各一个

    // 动态路由的尾斜杠同样归一
    ASSERT_TRUE(r.insert("/u/:id", 3));
    params p;
    auto *v = r.lookup("/u/42/", &p);
    ASSERT_NE(v, nullptr);
    EXPECT_EQ(*v, 3);
    ASSERT_EQ(p.size(), 1u);
    EXPECT_EQ(p[0].second, "42");
}

// ── 冲突检测: 同位置参数名/通配名不一致必须拒绝 ──

TEST(Router, ParamNameConflictRejected)
{
    router_int r;
    ASSERT_TRUE(r.insert("/user/:id/profile", 1));
    // 同位置的参数名不同 → 拒绝 (否则会静默改写第一条路由的参数名)
    EXPECT_FALSE(r.insert("/user/:name/settings", 2));
    EXPECT_EQ(r.size(), 1u);

    // 同名不冲突 (相当于共享参数节点)
    EXPECT_TRUE(r.insert("/user/:id/settings", 3));
    params p;
    auto *v = r.lookup("/user/42/profile", &p);
    ASSERT_NE(v, nullptr);
    EXPECT_EQ(p[0].first, "id"); // 参数名未被污染
}

TEST(Router, WildcardNameConflictRejected)
{
    router_int r;
    ASSERT_TRUE(r.insert("/files/*a", 1));
    EXPECT_FALSE(r.insert("/files/*b", 2)); // 异名冲突
    EXPECT_TRUE(r.insert("/files/*a", 3));  // 同名 = 替换
    EXPECT_EQ(*r.lookup("/files/x"), 3);
    EXPECT_EQ(r.size(), 1u);
}

TEST(Router, EmptyParamNameRejected)
{
    router_int r;
    EXPECT_FALSE(r.insert("/x/:", 1));
    EXPECT_FALSE(r.insert("/x/:/y", 1));
    EXPECT_EQ(r.size(), 0u);
}

// ── 失败插入无副作用 (两遍扫描: 先校验后建树) ──

TEST(Router, FailedInsertNoSideEffect)
{
    router_int r;
    EXPECT_FALSE(r.insert("/a/*w/illegal", 1)); // 通配不在末尾
    EXPECT_EQ(r.size(), 0u);
    EXPECT_EQ(r.lookup("/a"), nullptr);

    // 冲突导致的失败同样无副作用
    r.insert("/u/:id", 1);
    EXPECT_FALSE(r.insert("/u/:other", 2));
    EXPECT_EQ(r.size(), 1u);
    params p;
    EXPECT_EQ(*r.lookup("/u/9", &p), 1);
    EXPECT_EQ(p[0].first, "id");
}

// ── params 容器复用: lookup 必须先清空 ──

TEST(Router, ParamsClearedOnReuse)
{
    router_int r;
    r.insert("/a/:x/:y", 1);
    r.insert("/static", 2);

    params p;
    ASSERT_NE(r.lookup("/a/1/2", &p), nullptr);
    ASSERT_EQ(p.size(), 2u);

    // 复用同一容器查静态路由: 不允许残留上次的捕获
    ASSERT_NE(r.lookup("/static", &p), nullptr);
    EXPECT_TRUE(p.empty());

    // 复用查未命中: 同样清空
    EXPECT_EQ(r.lookup("/nomatch", &p), nullptr);
    EXPECT_TRUE(p.empty());
}

// ── 模式校验 ──

TEST(Router, RejectInvalidPatterns)
{
    router_int r;
    EXPECT_FALSE(r.insert("", 1));            // 空
    EXPECT_FALSE(r.insert("no-slash", 1));    // 无 / 开头
    EXPECT_FALSE(r.insert("user/1", 1));      // 无 / 开头
    EXPECT_FALSE(r.insert("/a//b", 1));       // 空段
    EXPECT_FALSE(r.insert("/files/*p/x", 1)); // 通配符不在末尾
    EXPECT_TRUE(r.insert("/ok", 1));
}

TEST(Router, RejectMisplacedSpecialChars)
{
    // ':'/'*' 只允许出现在段首 (与 httprouter 对齐),
    // 防止 "/user:id" 拼写错误被静默当静态字面量注册
    router_int r;
    EXPECT_FALSE(r.insert("/abc*def", 1));
    EXPECT_FALSE(r.insert("/user:id", 1));
    EXPECT_FALSE(r.insert("/a:b/c", 1));
    EXPECT_EQ(r.size(), 0u);
}

TEST(Router, ManyParamsOverflowInlineBuffer)
{
    // 参数超过内联容量 (8) 时迁移到堆, 行为不变
    router_int r;
    ASSERT_TRUE(r.insert("/a/:p1/:p2/:p3/:p4/:p5/:p6/:p7/:p8/:p9", 1));
    params p;
    auto *v = r.lookup("/a/1/2/3/4/5/6/7/8/9", &p);
    ASSERT_NE(v, nullptr);
    ASSERT_EQ(p.size(), 9u);
    EXPECT_EQ(p[0].first, "p1");
    EXPECT_EQ(p[0].second, "1");
    EXPECT_EQ(p[8].first, "p9");
    EXPECT_EQ(p[8].second, "9");

    // 回溯后回到内联容量内, 计数仍然正确
    ASSERT_TRUE(r.insert("/a/:p1/x", 2));
    p.clear();
    v = r.lookup("/a/5/x", &p);
    ASSERT_NE(v, nullptr);
    EXPECT_EQ(*v, 2);
    ASSERT_EQ(p.size(), 1u);
    EXPECT_EQ(p[0].second, "5");
}

TEST(Router, ReplaceExisting)
{
    router_int r;
    r.insert("/x", 1);
    r.insert("/x", 2); // 重复注册 = 替换
    EXPECT_EQ(*r.lookup("/x"), 2);
    EXPECT_EQ(r.size(), 1u); // 数量不重复累加

    r.insert("/y/:id", 10);
    r.insert("/y/:id", 20);
    EXPECT_EQ(*r.lookup("/y/1"), 20);
    EXPECT_EQ(r.size(), 2u);
}

// ── 未命中 & 计数 ──

TEST(Router, MissReturnsNull)
{
    router_int r;
    r.insert("/a", 1);
    EXPECT_EQ(r.lookup("/b"), nullptr);
    EXPECT_EQ(r.lookup(""), nullptr);
}

TEST(Router, SizeCounts)
{
    router_int r;
    EXPECT_TRUE(r.empty());
    r.insert("/a", 1);
    r.insert("/b/:id", 2);
    r.insert("/c/*w", 3);
    EXPECT_EQ(r.size(), 3u);
    r.clear();
    EXPECT_TRUE(r.empty());
    EXPECT_EQ(r.lookup("/a"), nullptr);
}

// ── string_view 生命周期: 捕获视图指向传入的 path ──

TEST(Router, ParamViewPointsIntoPath)
{
    router_int r;
    r.insert("/u/:id", 1);
    std::string path = "/u/abc123";
    params p;
    ASSERT_NE(r.lookup(path, &p), nullptr);
    ASSERT_EQ(p.size(), 1u);
    // 视图应直接引用 path 内部, 而非新分配
    EXPECT_EQ(p[0].second.data(), path.data() + 3);
    EXPECT_EQ(p[0].second, "abc123");
}

// ── 基准: 千级路由下的匹配开销 (只报告数据, 不做易碎断言) ──

TEST(Router, BenchmarkManyRoutes)
{
    router_int r;
    // 1000 条静态路由 (一级路径 200 + 深层路径 800) + 100 条动态路由
    char buf[64];
    for (int i = 0; i < 200; ++i)
    {
        std::snprintf(buf, sizeof(buf), "/top%d", i);
        r.insert(buf, i);
    }
    for (int i = 0; i < 800; ++i)
    {
        std::snprintf(buf, sizeof(buf), "/api/v%d/res%d/item", i / 40, i);
        r.insert(buf, i);
    }
    for (int i = 0; i < 100; ++i)
    {
        std::snprintf(buf, sizeof(buf), "/dyn%d/:id/sub", i);
        r.insert(buf, 10000 + i);
    }
    ASSERT_EQ(r.size(), 1100u);

    // 待查路径: 静态命中 / 动态命中 / 未命中 三类混合
    std::vector<std::string> paths;
    paths.reserve(300);
    for (int i = 0; i < 100; ++i)
    {
        std::snprintf(buf, sizeof(buf), "/top%d", i);
        paths.emplace_back(buf);
        std::snprintf(buf, sizeof(buf), "/api/v%d/res%d/item", i / 40, i);
        paths.emplace_back(buf);
        std::snprintf(buf, sizeof(buf), "/dyn%d/77/sub", i);
        paths.emplace_back(buf);
    }

    params p;
    const int rounds = 200;
    size_t hits = 0;
    auto t0 = std::chrono::steady_clock::now();
    for (int rep = 0; rep < rounds; ++rep)
        for (const auto &path : paths)
            if (r.lookup(path, &p))
                ++hits;
    auto t1 = std::chrono::steady_clock::now();

    long long total = (long long)rounds * paths.size();
    double ns = std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
    EXPECT_EQ(hits, (size_t)total); // 全部命中 (含动态)
    std::printf("[bench] %lld lookups over %d routes: %.1f ns/lookup\n",
                total, (int)r.size(), ns / total);
}

// ── 分档基准: 验证路由规模增长时的查找开销趋势 ──

TEST(Router, BenchmarkScaleTiers)
{
    char buf[64];
    for (int tier : {100, 1000, 10000})
    {
        router_int r;
        for (int i = 0; i < tier; ++i)
        {
            std::snprintf(buf, sizeof(buf), "/api/v%d/res%d/item", i / 50, i);
            ASSERT_TRUE(r.insert(buf, i));
        }

        // 均匀采样命中路径 (上限 256 条)
        std::vector<std::string> paths;
        const int step = (tier + 255) / 256;
        for (int i = 0; i < tier; i += step)
        {
            std::snprintf(buf, sizeof(buf), "/api/v%d/res%d/item", i / 50, i);
            paths.emplace_back(buf);
        }

        params p;
        const int rounds = tier <= 1000 ? 400 : 40;
        size_t hits = 0;
        auto t0 = std::chrono::steady_clock::now();
        for (int rep = 0; rep < rounds; ++rep)
            for (const auto &path : paths)
                if (r.lookup(path, &p))
                    ++hits;
        auto t1 = std::chrono::steady_clock::now();

        long long total = (long long)rounds * paths.size();
        double ns = std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
        EXPECT_EQ(hits, (size_t)total);
        std::printf("[bench] tier %6d routes: %7.1f ns/lookup (%lld lookups)\n",
                    tier, ns / total, total);
    }
}
