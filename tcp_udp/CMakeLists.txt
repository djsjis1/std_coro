# ============================================================================
# tcp_udp — TCP/UDP 服务端框架 (基于 coro 协程库)
# ============================================================================
cmake_minimum_required(VERSION 3.20)

# TCP/UDP echo 示例 (服务端 + 内置测试客户端)
if(CORO_HAS_NET)
    add_executable(tcp_udp_example example/main.cpp)
    target_link_libraries(tcp_udp_example PRIVATE coro::coro)
    target_include_directories(tcp_udp_example PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR})
    if(MSVC)
        target_compile_options(tcp_udp_example PRIVATE /utf-8)
    endif()
endif()
