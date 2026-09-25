# ============================================================================
# Web — 基于 coro + llhttp 的 HTTP 服务器示例
# ============================================================================
# 两种使用方式:
#   1. 仓库根构建: 根 CMakeLists.txt 已 add_subdirectory(Web)
#   2. 独立构建:   cmake -S Web -B build/web && cmake --build build/web
# ============================================================================
cmake_minimum_required(VERSION 3.20)
project(coro_web LANGUAGES C CXX)

# ---- 依赖: coro (header-only) 与 http (llhttp + 封装) 与 router (动态路由) ----
# 根构建已引入则跳过, 保证两种方式都不重复定义目标
if(NOT TARGET coro::coro)
    set(CORO_WEB_BOOTSTRAP TRUE)
    add_subdirectory(.. ${CMAKE_BINARY_DIR}/coro EXCLUDE_FROM_ALL)
    unset(CORO_WEB_BOOTSTRAP)
endif()
if(NOT TARGET coro::web)
    add_subdirectory(src)
endif()

# ---- 可执行程序 ----
add_executable(web_server main.cpp)
target_link_libraries(web_server PRIVATE coro::web http::http)

# 静态文件目录绝对路径(编译期注入, 保证任何工作目录下 selftest 稳定)
target_compile_definitions(web_server PRIVATE WEB_WWW_DIR="${CMAKE_CURRENT_SOURCE_DIR}/www")

if(MSVC)
    target_compile_options(web_server PRIVATE /utf-8)  # 源码含中文注释
    # LTCG: coro 是 header-only, 热路径 (schedule/handle_connection) 分布在
    # 多个 TU, 链接时代码生成让它们跨 TU 内联
    set_property(TARGET web_server PROPERTY INTERPROCEDURAL_OPTIMIZATION TRUE)
endif()
