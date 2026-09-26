# ============================================================================
# tcp_udp — TCP/UDP 高层服务端 (header-only 组件, 依赖 coro 与平台 I/O 后端)
# ============================================================================
#
# 用法与其他组件一致:
#   target_link_libraries(your_app PRIVATE coro::tcp_udp)
#   #include <tcp_udp/tcp_server.hpp>
#
# include 路径刻意指向仓库根: 这样源码树与安装后 (include/tcp_udp/) 的引用写法
# 完全相同, 不需要每个消费者自己补一条裸 include 路径。
#
# 无网络能力的构建 (CORO_HAS_NET=OFF) 不定义本 target, 也不注册示例。
# ============================================================================
cmake_minimum_required(VERSION 3.20)

if(NOT CORO_HAS_NET)
    return()
endif()

add_library(coro_tcp_udp INTERFACE)
add_library(coro::tcp_udp ALIAS coro_tcp_udp)

target_include_directories(coro_tcp_udp INTERFACE
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/..>"
    "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>")
target_link_libraries(coro_tcp_udp INTERFACE coro::coro)

# TCP/UDP echo 示例 (服务端 + 内置测试客户端)
add_executable(tcp_udp_example example/main.cpp)
target_link_libraries(tcp_udp_example PRIVATE coro::tcp_udp)
if(MSVC)
    target_compile_options(tcp_udp_example PRIVATE /utf-8)
endif()
