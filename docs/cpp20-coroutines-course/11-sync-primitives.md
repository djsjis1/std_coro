# 第 11 讲：单线程事件循环中的同步原语

## 核心要点

- 单线程事件循环里**不能**用 `std::mutex`/`std::condition_variable` 做协程同步——它们会阻塞整条线程，所有协程一起停摆。
- 协程同步原语的通用实现模式：**等待队列（存句柄）+ 唤醒即 `schedule`**。
- "拿不到资源"的正确姿势是**挂起自己并排队**，而不是阻塞等待。
- 队列里存的是 `coroutine_handle<>`（类型擦除），唤醒 = 把句柄放回调度器。
- 单线程模型下**不需要原子操作**：没有真正的并发，只有交替执行。

---

## 11.1 为什么 `std::mutex` 在这里是灾难

```cpp
coro::Task<> worker() {
    std::mutex m;                         // ❌ 反例
    m.lock();                             // 若已被占: 整个线程阻塞
    // 其他所有协程全部冻结 —— 包括"即将释放锁的那个协程"
}
```

`std::mutex::lock()` 在竞争时**阻塞线程**。单线程事件循环里线程被阻塞 = 一切停摆。更糟的是：持有锁的协程可能正等着被调度器 resume——它永远等不到，因为线程被锁占住了（**自我死锁**）。

正确的同步原语必须遵守一条铁律：**等待 = 挂起协程，绝不阻塞线程**。

## 11.2 通用模式：等待队列

所有协程同步原语都是同一个骨架：

```
获取资源失败
  → 把自己 (coroutine_handle) 放进等待队列
  → 挂起 (co_await 的 await_suspend 返回)

资源可用时 (release/set/put)
  → 从等待队列取出一个 (或全部) 句柄
  → schedule(句柄) 交回调度器
```

伪代码模板：

```cpp
struct sync_primitive {
    std::deque<std::coroutine_handle<>> waiters;   // 等待队列

    // 挂起路径: co_await 走这里
    bool await_ready() { return try_acquire(); }           // 能拿到 → 不挂起
    void await_suspend(std::coroutine_handle<> h) {
        waiters.push_back(h);                              // 拿不到 → 排队
    }
    void await_resume() {}

    // 唤醒路径: 释放方调用
    void release() {
        if (!waiters.empty()) {
            auto h = waiters.front(); waiters.pop_front();
            scheduler.schedule(h);                         // 唤醒队首
        }
    }
};
```

## 11.3 Lock：互斥锁

```cpp
// 编译: g++ -std=c++20 async_lock.cpp && ./a.out
#include <coroutine>
#include <deque>
#include <iostream>
#include <queue>

// ── 调度器 (同第 6 讲) ──
struct Scheduler {
    std::queue<std::coroutine_handle<>> ready;
    void schedule(std::coroutine_handle<> h) { ready.push(h); }
    void run() {
        while (!ready.empty()) {
            auto h = ready.front(); ready.pop();
            if (!h.done()) h.resume();
        }
    }
};
Scheduler sched;

// ── 协程互斥锁: FIFO 公平 ──
struct AsyncLock {
    std::deque<std::coroutine_handle<>> waiters;
    bool locked = false;

    // co_await lock 时: 能拿则拿 (不挂起), 不能拿则排队挂起
    bool await_ready() noexcept {
        if (!locked) { locked = true; return true; }   // 拿到锁, 继续执行
        return false;                                   // 被占, 挂起
    }
    void await_suspend(std::coroutine_handle<> h) { waiters.push_back(h); }
    void await_resume() noexcept {}

    // 释放: 有等待者 → 锁直接移交队首 (locked 保持 true!)
    void release() {
        if (!waiters.empty()) {
            auto h = waiters.front(); waiters.pop_front();
            sched.schedule(h);                          // 唤醒下一个持有者
        } else {
            locked = false;                             // 没人等, 解锁
        }
    }
};

// ── 演示: 三个协程抢锁, 串行进入临界区 ──
// Task 与 yield_now 复用第 6 讲的完整定义 (Scheduler sched 同理)
// 编译: 将本讲 Lock/Semaphore/Event/Queue 与第 6 讲调度器/Task 合并为单文件

Task worker(AsyncLock& lock, char name, int* counter) {
    for (int i = 0; i < 2; ++i) {
        co_await lock;                     // 获取锁 (可能挂起排队)
        std::cout << name;
        (*counter)++;
        lock.release();                    // 释放 → 唤醒下一个等待者
        co_await yield_now{};
    }
}

int main() {
    AsyncLock lock;
    int counter = 0;
    Task a = worker(lock, 'A', &counter);
    Task b = worker(lock, 'B', &counter);
    Task c = worker(lock, 'C', &counter);
    a.start(); b.start(); c.start();       // 惰性协程全部启动
    sched.run();                           // 交替执行
    std::cout << " counter=" << counter << "\n";
}

// 输出: A B C A B C counter=6 —— 锁保证了任意时刻只有一个协程在临界区
```

**关键细节**：`release()` 里锁**直接移交**给队首等待者（`locked` 保持 true），中间不存在"解锁 → 再抢"的空窗——这在单线程下无竞态，但语义上更严谨。

## 11.4 Semaphore：信号量

与 Lock 的区别只是"计数"：

```cpp
struct AsyncSemaphore {
    std::deque<std::coroutine_handle<>> waiters;
    int permits;

    explicit AsyncSemaphore(int n) : permits(n) {}

    bool await_ready() noexcept {
        if (permits > 0) { --permits; return true; }   // 有余量 → 直接拿
        return false;
    }
    void await_suspend(std::coroutine_handle<> h) { waiters.push_back(h); }
    void await_resume() noexcept {}

    void release() {
        if (!waiters.empty()) {
            sched.schedule(waiters.front());
            waiters.pop_front();
            // 注意: 不 ++permits —— 许可直接转给被唤醒者
        } else {
            ++permits;
        }
    }
};
```

典型用途：**限制并发数**（如"最多 5 个协程同时访问数据库"）。

## 11.5 Event：一次性信号

```cpp
struct AsyncEvent {
    std::deque<std::coroutine_handle<>> waiters;
    bool set_ = false;

    bool await_ready() const noexcept { return set_; }   // 已 set → 立即通过
    void await_suspend(std::coroutine_handle<> h) { waiters.push_back(h); }
    void await_resume() const noexcept {}

    void set() {                                         // 广播: 唤醒全部
        set_ = true;
        while (!waiters.empty()) {
            sched.schedule(waiters.front());
            waiters.pop_front();
        }
    }
    void clear() { set_ = false; }
};
```

Event 与 Lock 的唤醒差异：Lock/Semaphore **唤醒一个**（公平移交），Event **唤醒全部**（广播）。

## 11.6 Queue：生产者-消费者

队列有两个等待方向，是原语组合的经典例子：

```cpp
template <typename T>
struct AsyncQueue {
    std::deque<T> items;
    std::deque<std::coroutine_handle<>> getters;   // 消费者等待队列
    std::deque<std::coroutine_handle<>> putters;   // 生产者等待队列
    size_t maxsize = 0;                            // 0 = 无限

    // ── get: 空则挂起消费者 ──
    struct get_op {
        AsyncQueue* q;
        bool await_ready() const { return !q->items.empty(); }
        void await_suspend(std::coroutine_handle<> h) { q->getters.push_back(h); }
        T await_resume() {
            T v = std::move(q->items.front());
            q->items.pop_front();
            q->wake_one(q->putters);               // 腾出空间 → 唤醒一个生产者
            return v;
        }
    };
    auto get() { return get_op{this}; }

    // ── put: 满则挂起生产者 ──
    struct put_op {
        AsyncQueue* q;
        T value;
        bool await_ready() const {
            return q->maxsize == 0 || q->items.size() < q->maxsize;
        }
        void await_suspend(std::coroutine_handle<> h) { q->putters.push_back(h); }
        void await_resume() {
            q->items.push_back(std::move(value));
            q->wake_one(q->getters);               // 有货了 → 唤醒一个消费者
        }
    };
    auto put(T v) { return put_op{this, std::move(v)}; }

private:
    static void wake_one(std::deque<std::coroutine_handle<>>& q) {
        if (!q.empty()) {
            sched.schedule(q.front());
            q.pop_front();
        }
    }
};
```

组合规则：**put 成功 → 唤醒一个 getter；get 成功 → 唤醒一个 putter**。这是背压（backpressure）的基础设施。

## 11.7 单线程模型的推论

- 所有原语**不需要锁**：代码在"交替执行"而非"并发执行"，每段临界区天然原子。
- 但也**不提供跨线程保护**：如果 `Promise::set_value` 要从工作线程调用，还是需要 `std::mutex` 保护数据结构（第 12 讲讨论线程桥接）。
- 等待者被销毁（取消/异常）时，队列里的句柄会悬空——生产实现必须处理"僵尸等待者"（见第 14 讲）。

## 11.8 小结

1. 协程同步 = 等待队列 + schedule 唤醒，绝不阻塞线程。
2. Lock/Semaphore 唤醒一个（移交），Event 唤醒全部（广播），Queue 双向唤醒。
3. 单线程下无需原子变量，但跨线程场景要重新审视。
4. 这些原语是事件循环框架的"内功"：限制并发（Semaphore）、协调状态（Event）、背压（Queue）。

## 思考题

1. 11.3 的 Lock 为什么"移交"而非"解锁后让等待者再抢"？两种实现的区别在哪？（提示：公平性）
2. `AsyncQueue::put` 挂起时，元素 `T value` 存在哪里？它的生命周期由谁保证？（提示：awaiter 在协程帧里）
3. 若一个协程在 `co_await lock` 挂起排队期间被取消（第 9 讲），队列里的句柄会怎样？给出一种清理方案。
