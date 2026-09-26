cmake_minimum_required(VERSION 3.20)
project(coro_core_smoke LANGUAGES CXX)

find_package(coro CONFIG REQUIRED)

add_executable(coro_core_smoke main.cpp)
# 纯核心消费者: 只链接 coro::core, 用于证明核心不牵连任何原生 I/O 依赖。
target_link_libraries(coro_core_smoke PRIVATE coro::core)

# 解耦门禁: 即使安装包来自带 io_uring 的 native 构建, coro::core 也不得把后端
# 依赖转交给只想要协程核心的消费者。配置期直接失败, 不靠人工检查导出文件。
get_target_property(coro_core_link_libs coro::core INTERFACE_LINK_LIBRARIES)
if(coro_core_link_libs MATCHES "uring|ws2_32")
    message(FATAL_ERROR "coro::core must not carry native I/O link dependencies: ${coro_core_link_libs}")
endif()
