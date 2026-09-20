#include <coro/coro.hpp>

coro::Task<int> answer() {
    co_return 42;
}

int main() {
    return coro::run(answer()) == 42 ? 0 : 1;
}
