#include <coro/coro.hpp>

using coro::Task;

Task<> main_task() {}

int main() {
#ifdef _WIN32
    SetConsoleOutputCP(65001);
    SetConsoleCP(65001);
#endif
    coro::run(main_task());
    return 0;
}
