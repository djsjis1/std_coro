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
    add_subdirectory(.. ${CMAKE_BINARY_DIR}/coro)
endif()
if(NOT TARGET http::http)
    # Web 只把 HTTP 层当作内部依赖：不构建它自带的示例，也不让
    # 子项目的 install() 规则污染 coro 的 header-only 安装包。
    set(STD_HTTP_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
    add_subdirectory(../thirdparty/http ${CMAKE_BINARY_DIR}/http EXCLUDE_FROM_ALL)
endif()
if(NOT TARGET router::router)
    add_subdirectory(../router ${CMAKE_BINARY_DIR}/router)
endif()

# ---- 可执行程序 ----
add_executable(web_server
    main.cpp
    src/http_types.cpp
    src/web_server.cpp
)
target_link_libraries(web_server PRIVATE coro::coro http::http router::router)
target_include_directories(web_server PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)

# 静态文件目录绝对路径(编译期注入, 保证任何工作目录下 selftest 稳定)
target_compile_definitions(web_server PRIVATE WEB_WWW_DIR="${CMAKE_CURRENT_SOURCE_DIR}/www")

if(MSVC)
    target_compile_options(web_server PRIVATE /utf-8)  # 源码含中文注释
    # LTCG: coro 是 header-only, 热路径 (schedule/handle_connection) 分布在
    # 多个 TU, 链接时代码生成让它们跨 TU 内联
    set_property(TARGET web_server PROPERTY INTERPROCEDURAL_OPTIMIZATION TRUE)
endif()
