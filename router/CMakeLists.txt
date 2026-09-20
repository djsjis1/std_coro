# ============================================================================
# router — 高性能动态路由库 (header-only, 参考 httprouter/find-my-way)
# ============================================================================
#
# 两种使用方式 (与 coro 库一致):
#
# 方式一: add_subdirectory (推荐, 一键引入)
#   add_subdirectory(path/to/router)
#   target_link_libraries(your_app PRIVATE router::router)
#
# 特点: header-only, 零依赖 (纯标准库);
#       目标名 router::router, 自动携带 include 路径。
# ============================================================================

cmake_minimum_required(VERSION 3.20)
project(router LANGUAGES CXX)

# ---- 主 target: header-only INTERFACE 库 ----
add_library(router INTERFACE)
add_library(router::router ALIAS router)

# 头文件直接位于本目录 (radix_router.h), include 本目录即可 #include <radix_router.h>
target_include_directories(router INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})
target_compile_features(router INTERFACE cxx_std_20)

# ---- 编译器适配 ----
if(MSVC)
    target_compile_options(router INTERFACE /utf-8 /EHsc)   # 源码含中文注释
endif()
