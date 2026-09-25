#pragma once

#include <algorithm>
#include <deque>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

// ============================================================================
// radix_router<Value> — 高性能动态路由库 (header-only, 泛型)
// ============================================================================
//
// 设计参考 (站在开源巨人的肩膀上):
//   - Go httprouter: 单棵字典树 (静态路由也进树, 不另设快表),
//     :param / *wildcard 语法, 优先级 静态 > 参数 > 通配,
//     同位置参数名冲突视为非法 (httprouter 直接 panic, 这里返回 false)
//   - 字节 Hertz: DFS + 回溯 (参数分支深层失败时撤销捕获, 改试其他分支)
//   - 匹配全程 string_view, 零字符串分配
//
// 结构说明:
//   按段字典树 (segment trie) —— URL 每一段一个节点, 段内不做字符级压缩。
//   相比 httprouter 的字符级压缩 radix tree, 取舍了万级路由量下的极限性能,
//   换来实现简洁与可读性 (中小规模实测无差距)。
//
// 性能设计:
//   - 节点 arena: 所有节点集中存放在 deque 池里 (指针稳定), 消除每节点
//     一次堆分配的碎片化, 提升缓存局部性; clear() 也不会递归析构深树
//   - 静态子节点「有序 vector」: 兄弟 ≤8 个时线性扫描 (无间接调用开销),
//     超过后 lower_bound 二分, 根节点挂上百个一级路径也不退化
//   - 参数捕获容器内联 8 槽 small buffer, 常规参数数 (0~2) 零堆分配
//
// 模式语法:
//   /user/list         静态路由
//   /user/:id          参数路由, 捕获单个路径段
//   /files/*filepath   通配路由 (只允许在最后一段), 吞掉剩余全部路径
//   ':'/'*' 只允许出现在段首, 段中出现视为非法 (与 httprouter 一致,
//   防止 "/user:id" 这类拼写错误被静默当成静态字面量)
//
// 语义约定:
//   - 尾斜杠归一: "/user/" 与 "/user" 视为同一路由 (注册与查找都归一)
//   - 根路径 "/" 合法; 空路径 "" 拒绝
//   - insert 失败保证无副作用 (先校验后建树)
//   - 同位置 :param / *wild 名字冲突 → 返回 false (防参数名被静默改写)
//
// 用法:
//   radix_router<int> r;
//   r.insert("/user/:id", 42);
//   radix_router<int>::params_view params;
//   const int *v = r.lookup("/user/7", &params); // *v=42, params={{"id","7"}}
//
// 线程安全:
//   insert() 必须在并发 lookup() 之前全部完成 (注册完再对外服务);
//   lookup() 是只读操作, 可安全并发调用。
//   注意: 本类型 move-only (节点指针指向内部 arena, 不可拷贝)。
// ============================================================================

template <typename Value> class radix_router {
  public:
    // ==================================================================
    // params_view — 参数捕获容器 (内联 small buffer)
    // ==================================================================
    // 实战中 URL 参数几乎总是 0~2 个, 内联 8 槽覆盖绝大多数场景,
    // 全程零堆分配; 极端情况 (>8 个参数) 才迁移到堆上, 行为不变。
    // 元素是指向 lookup() 的 path 内部的 string_view, 仅在 path 存活期间有效。
    class params_view {
      public:
        using value_type = std::pair<std::string_view, std::string_view>;

        void emplace_back(std::string_view k, std::string_view v) {
            if (spilled_) // 已迁移到堆: 只操作 overflow_
            {
                overflow_.emplace_back(k, v);
                return;
            }
            if (n_ < kInline) {
                inline_[n_++] = {k, v};
                return;
            }
            // 罕见: 超过内联容量 → 整体迁移到堆
            spilled_ = true;
            overflow_.assign(inline_, inline_ + n_);
            overflow_.emplace_back(k, v);
        }

        void pop_back() noexcept {
            if (spilled_) {
                if (!overflow_.empty())
                    overflow_.pop_back();
            } else if (n_ > 0) {
                --n_;
            }
        }

        void clear() noexcept {
            n_ = 0;
            spilled_ = false;
            overflow_.clear();
        }

        void reserve(size_t) noexcept {} // 内联缓冲天然就绪; 保留接口兼容

        size_t size() const noexcept { return spilled_ ? overflow_.size() : n_; }
        bool empty() const noexcept { return size() == 0; }

        const value_type* begin() const noexcept { return spilled_ ? overflow_.data() : inline_; }
        const value_type* end() const noexcept { return begin() + size(); }
        const value_type& operator[](size_t i) const noexcept { return begin()[i]; }

      private:
        static constexpr size_t kInline = 8;
        value_type inline_[kInline];
        size_t n_ = 0;
        bool spilled_ = false;
        std::vector<value_type> overflow_; // 仅 >8 个参数时使用
    };

    // ==================================================================
    // insert — 注册一个路由模式 (两遍扫描: 先校验后建树, 失败无副作用)
    // ==================================================================
    /// 模式必须以 '/' 开头; 重复注册同一模式会替换旧的 value。
    /// 返回 false 表示模式非法 (未以 '/' 开头 / 空段 / 通配符不在末尾 /
    /// 参数名为空 / 段中间出现 ':' 或 '*' / 与已注册路由的参数名冲突),
    /// 且不会改动树。
    bool insert(std::string_view pattern, Value value) {
        if (pattern.empty() || pattern.front() != '/')
            return false; // 非法: 必须以 '/' 开头

        // ---- 第 1 遍: 纯校验 (含与现有树的冲突检测) ----
        if (!validate(pattern))
            return false;

        // ---- 第 2 遍: 建树 / 替换 value ----
        node* cur = &root_;
        std::string_view rest = pattern;
        while (true) {
            auto [seg, remaining] = next_segment(rest);
            if (seg.empty())
                break; // 根路径 "/" 或尾斜杠: 模式到此结束 (已校验合法)
            if (seg.front() == '*') {
                if (!cur->wild) {
                    cur->wild = new_node();
                    cur->wild->name = seg.substr(1); // '*' 之后是参数名
                }
                if (!cur->wild->value)
                    ++count_;
                cur->wild->value = std::move(value);
                return true;
            }
            if (seg.front() == ':') {
                if (!cur->param) {
                    cur->param = new_node();
                    cur->param->name = seg.substr(1); // ':' 之后是参数名
                }
                cur = cur->param;
            } else {
                cur = find_or_create_child(cur->statics, seg);
            }
            if (remaining.empty())
                break;
            rest = remaining;
        }
        if (!cur->value)
            ++count_;
        cur->value = std::move(value); // 模式终点, 存放 handler
        log_.emplace_back(pattern);
        return true;
    }

    // ==================================================================
    // lookup — 查找请求路径
    // ==================================================================
    /// 命中返回指向 value 的指针, 未命中返回 nullptr。
    /// out_params 非空时, 先清空再写入捕获的动态参数 (视图指向 path 内部)。
    const Value* lookup(std::string_view path, params_view* out_params = nullptr) const {
        if (path.empty())
            return nullptr; // 空路径不是合法请求目标 (根路由请用 "/")

        params_view tmp; // 调用方不关心参数时用的丢弃槽
        params_view& caps = out_params ? *out_params : tmp;
        caps.clear(); // 清掉上次的残留捕获 (调用方可能复用同一容器)

        const Value* result = nullptr;
        if (dfs(root_, path, caps, result))
            return result;
        return nullptr;
    }

    size_t size() const noexcept { return count_; }
    bool empty() const noexcept { return count_ == 0; }

    /// 注册顺序日志: 按 insert 成功顺序记录原始模式字符串。
    /// 用于 router::include() 按序回放子路由器的路由。
    const std::vector<std::string>& patterns() const noexcept { return log_; }

    /// 清空全部路由 (热重载场景: 清空重建)。
    /// 节点在 arena 池里, pool_.clear() 线性析构, 不递归深树。
    void clear() {
        root_ = node{};
        pool_.clear();
        count_ = 0;
        log_.clear();
    }

    // 节点指针指向内部 arena: move 转移所有权安全, 拷贝会悬空 → 禁止
    radix_router() = default;
    radix_router(const radix_router&) = delete;
    radix_router& operator=(const radix_router&) = delete;
    radix_router(radix_router&&) noexcept = default;
    radix_router& operator=(radix_router&&) noexcept = default;

  private:
    struct node;
    // 静态子节点表: 段文本 → 子节点, 按段文本「有序」维护。
    // 兄弟少时线性扫描最快 (无间接调用/分支预测开销), 多了走二分。
    using seg_list = std::vector<std::pair<std::string, node*>>;

    // 分段字典树节点 (URL 每一段一个节点)
    // 例: 注册 /user/:id 与 /files/*any 后的树形:
    //   root
    //   ├── statics["files"] ── wild(*any) ★value
    //   └── statics["user"]  ── param(:id) ★value
    struct node {
        seg_list statics;           // 静态子节点: 有序 vector
        node* param = nullptr;      // :name 参数子节点 (至多一个)
        node* wild = nullptr;       // *name 通配子节点 (吞掉剩余路径)
        std::string name;           // 参数名 (本节点是 param/wild 节点时有效)
        std::optional<Value> value; // 非空 ⟺ 本节点是某个模式的终点
    };

    // ---- 节点 arena ----
    // deque 保证: emplace_back 永不失效已有元素的指针/引用,
    // 且同一块 chunk 内节点内存连续 (比逐节点 new 缓存友好)。
    std::deque<node> pool_;

    node* new_node() {
        pool_.emplace_back();
        return &pool_.back();
    }

    // ---- 静态子节点查找 ----

    // 兄弟数 ≤ 该阈值时线性扫描优于二分 (实测经验值, 参考 Hertz 的做法)
    static constexpr size_t kLinearMax = 8;

    template <typename List> static auto lower_bound_child(List& list, std::string_view seg) {
        return std::lower_bound(list.begin(), list.end(), seg, [](const auto& item, std::string_view sv) {
            return std::string_view(item.first) < sv;
        });
    }

    static node* find_child(const seg_list& list, std::string_view seg) {
        if (list.size() <= kLinearMax) {
            for (const auto& [s, child] : list)
                if (std::string_view(s) == seg)
                    return child;
            return nullptr;
        }
        auto it = lower_bound_child(list, seg);
        if (it != list.end() && std::string_view(it->first) == seg)
            return it->second;
        return nullptr;
    }

    /// 查找或创建静态子节点 (insert 第 2 遍用; 注册期冷路径)
    node* find_or_create_child(seg_list& list, std::string_view seg) {
        if (list.size() <= kLinearMax) {
            for (auto& [s, child] : list)
                if (std::string_view(s) == seg)
                    return child;
        } else {
            auto it = lower_bound_child(list, seg);
            if (it != list.end() && std::string_view(it->first) == seg)
                return it->second;
        }
        // 未命中: 原位插入保持有序 (维持二分前提; 搬移开销只在注册期)
        auto pos = lower_bound_child(list, seg);
        return list.insert(pos, {std::string(seg), new_node()})->second;
    }

    // ---- 插入前的两遍扫描之第 1 遍: 校验 ----

    /// 段内 (非段首) 出现 ':' 或 '*' → 非法。
    /// 与 httprouter 对齐: 防止 "/user:id" 这类拼写错误被静默当字面量注册。
    static bool segment_has_misplaced_special(std::string_view seg) {
        return seg.find_first_of(":*") != std::string_view::npos;
    }

    /// 校验模式合法性 + 与现有树的冲突。全程只读, 不改树。
    bool validate(std::string_view pattern) const {
        const node* cur = &root_;
        std::string_view rest = pattern;
        while (true) {
            auto [seg, remaining] = next_segment(rest);
            if (seg.empty()) {
                if (!remaining.empty())
                    return false; // 空段 ("//"), 非法
                return true;      // 根路径或尾斜杠: 合法收尾
            }
            if (seg.front() == '*') {
                if (!remaining.empty())
                    return false; // 通配符必须是最后一段
                // 通配名必须与已有的一致 (防同位置异名歧义)
                if (cur->wild && std::string_view(cur->wild->name) != seg.substr(1))
                    return false;
                return true;
            }
            if (seg.front() == ':') {
                if (seg.size() == 1)
                    return false; // 空参数名 (":")
                if (cur->param) {
                    // 与已注册路由的参数名冲突: httprouter 在此 panic,
                    // 这里拒绝注册 —— 否则旧路由的参数名会被静默改写
                    if (std::string_view(cur->param->name) != seg.substr(1))
                        return false;
                    cur = cur->param;
                } else {
                    return validate_syntax(remaining); // 树外部分只需语法校验
                }
            } else {
                if (segment_has_misplaced_special(seg))
                    return false; // 段中间出现 ':'/'*': 非法
                const node* child = find_child(cur->statics, seg);
                if (!child)
                    return validate_syntax(remaining); // 树外部分只需语法校验
                cur = child;
            }
            if (remaining.empty())
                return true;
            rest = remaining;
        }
    }

    /// 纯语法校验 (用于模式尚未入树的后半段)
    static bool validate_syntax(std::string_view rest) {
        while (true) {
            if (rest.empty())
                return true;
            auto [seg, remaining] = next_segment(rest);
            if (seg.empty()) {
                if (!remaining.empty())
                    return false; // 空段 ("//")
                return true;      // 尾斜杠收尾
            }
            if (seg.front() == '*')
                return remaining.empty(); // 通配符必须是最后一段
            if (seg.front() == ':') {
                if (seg.size() == 1)
                    return false; // 空参数名
            } else if (segment_has_misplaced_special(seg)) {
                return false; // 段中间出现 ':'/'*': 非法
            }
            rest = remaining;
        }
    }

    // ---- 匹配核心 ----

    /// 切出下一段: "/user/42/x" → ("user", "/42/x"); 最后一段 remaining 为空。
    /// 返回值里的两个视图都指向原字符串, 零分配。
    static std::pair<std::string_view, std::string_view> next_segment(std::string_view p) {
        if (!p.empty() && p.front() == '/')
            p.remove_prefix(1); // 跳过分隔符
        auto pos = p.find('/');
        if (pos == std::string_view::npos)
            return {p, {}};                       // 最后一段
        return {p.substr(0, pos), p.substr(pos)}; // remaining 保留开头的 '/'
    }

    /// DFS 匹配 + 回溯 (Hertz 同款算法):
    /// 为什么需要回溯 —— :param 捕获当前段后, 深层可能匹配失败。
    /// 例: 注册 /files/:lang/readme 与 /files/*any, 请求 /files/go/x/y:
    ///   :lang 捕获 "go" 后深层找不到 readme → 必须撤销捕获,
    ///   退回上层改试通配分支 *any, 才能正确命中。
    static bool dfs(const node& cur, std::string_view rest, params_view& caps, const Value*& result) {
        // rest 为空或仅剩尾斜杠 "/": 路径走完 (尾斜杠归一)
        if (rest.empty() || rest == "/") {
            if (cur.value) {
                result = &*cur.value;
                return true;
            }
            return false;
        }

        auto [seg, remaining] = next_segment(rest);

        // 优先级 1: 静态子节点 (深层失败自动落到参数分支 = 回溯)
        if (!seg.empty()) {
            if (node* child = find_child(cur.statics, seg))
                if (dfs(*child, remaining, caps, result))
                    return true;
        }
        // 优先级 2: 参数子节点 (捕获当前段; 深层失败时 pop 回溯)。
        // 参数段必须非空: /user 不应命中 /user/:id
        if (cur.param && !seg.empty()) {
            caps.emplace_back(std::string_view(cur.param->name), seg);
            if (dfs(*cur.param, remaining, caps, result))
                return true;
            caps.pop_back(); // 撤销捕获, 尝试其他分支
        }
        // 优先级 3: 通配子节点 (吞掉剩余全部路径, 含所有 '/')。
        // 捕获时去掉开头的 '/', 得到纯相对路径 (便于拼接文件路径)
        if (cur.wild) {
            std::string_view cap = rest;
            if (!cap.empty() && cap.front() == '/')
                cap.remove_prefix(1);
            caps.emplace_back(std::string_view(cur.wild->name), cap);
            if (cur.wild->value) {
                result = &*cur.wild->value;
                return true;
            }
            caps.pop_back();
        }
        return false;
    }

    node root_;                    // 字典树根 (静态路由也在树里, httprouter 同款单树结构)
    size_t count_ = 0;             // 已注册模式数
    std::vector<std::string> log_; // 注册顺序日志 (include 回放用)
};
