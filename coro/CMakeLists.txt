# ============================================================================
# coro — C++20 协程事件循环框架 (类 Python asyncio)
# ============================================================================
#
# 这是库的独立 CMake 配置。两种使用方式:
#
# 方式一: add_subdirectory (推荐, 一键引入)
#   add_subdirectory(path/to/coro)           # 或 FetchContent 拉取后同用
#   target_link_libraries(your_app PRIVATE coro::coro)
#
# 方式二: FetchContent
#   include(FetchContent)
#   FetchContent_Declare(coro GIT_REPOSITORY <url> GIT_TAG <tag>)
#   FetchContent_MakeAvailable(coro)
#   target_link_libraries(your_app PRIVATE coro::coro)
#
# 特点: header-only, 零依赖 (Linux 网络层可选 liburing);
#       目标名 coro::coro, 自动携带 include 路径与平台链接库。
# ============================================================================

cmake_minimum_required(VERSION 3.20)
project(coro LANGUAGES CXX)

# ---- 主 target: header-only INTERFACE 库 ----
add_library(coro INTERFACE)
add_library(coro::coro ALIAS coro)

target_include_directories(coro INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_compile_features(coro INTERFACE cxx_std_20)

# ---- 编译器适配 ----
if(MSVC)
    target_compile_options(coro INTERFACE /utf-8)   # 源码含中文注释
endif()
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS 14)
    target_compile_options(coro INTERFACE -fcoroutines)
endif()

# ---- 平台链接库 ----
if(WIN32)
    target_link_libraries(coro INTERFACE ws2_32)     # IOCP 网络层
endif()
if(UNIX)
    target_link_libraries(coro INTERFACE pthread)
endif()
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    # io_uring 网络层 (可选, sudo apt install liburing-dev)
    find_library(URING_LIB uring)
    if(URING_LIB)
        target_link_libraries(coro INTERFACE ${URING_LIB})
    endif()
endif()
