#include <coro/coro.hpp>

using coro::Task;

Task<> main_task() {
    co_return; // 协程体可为空, 但不能省略 co_return (MSVC C4716)
}

int main() {
#ifdef _WIN32
    SetConsoleOutputCP(65001);
    SetConsoleCP(65001);
#endif
    coro::run(main_task());
    return 0;
}
