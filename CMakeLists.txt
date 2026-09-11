# ============================================================================
# coro 开发仓库根 CMake — examples / 单元测试 (库本体见 coro/ 子目录)
# ============================================================================
cmake_minimum_required(VERSION 3.20)
project(coro_dev LANGUAGES CXX)

# 单配置生成器 (Ninja/Makefile) 默认 Release; 多配置 (VS) 由 --config 决定
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "" FORCE)
endif()

# 库: header-only, 目标名 coro::coro (含 include 路径 + 平台链接库)
add_subdirectory(coro)

# 路由库: header-only 基数树动态路由, 目标名 router::router
add_subdirectory(router)

# MSVC: 多处理器并行编译 (纯构建加速, 不影响产物)
if(MSVC)
    add_compile_options(/MP)
endif()

# ============================================================================
# 示例
# ============================================================================
add_executable(example_basic examples/basic.cpp)
target_link_libraries(example_basic PRIVATE coro::coro)

add_executable(example_gather examples/gather.cpp)
target_link_libraries(example_gather PRIVATE coro::coro)

add_executable(example_future examples/future.cpp)
target_link_libraries(example_future PRIVATE coro::coro)

add_executable(example_full_test examples/full_test.cpp)
target_link_libraries(example_full_test PRIVATE coro::coro)

add_executable(example_echo examples/echo_server.cpp)
target_link_libraries(example_echo PRIVATE coro::coro)

add_executable(example_file_io examples/file_io.cpp)
target_link_libraries(example_file_io PRIVATE coro::coro)

# 常驻模式优雅关停验证 (signal::handle → stop → wait_all)
add_executable(example_dir_watch examples/dir_watch.cpp)
target_link_libraries(example_dir_watch PRIVATE coro::coro)

add_executable(example_process examples/process_demo.cpp)
target_link_libraries(example_process PRIVATE coro::coro)

add_executable(web_shutdown_check
    tests/web_shutdown_check.cpp
    Web/src/http_types.cpp
    Web/src/web_server.cpp)
target_link_libraries(web_shutdown_check PRIVATE coro::coro http::http router::router)
target_include_directories(web_shutdown_check PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/Web/src)
if(MSVC)
    target_compile_options(web_shutdown_check PRIVATE /utf-8)
endif()
 
# Web 服务器示例 (coro 网络层 + llhttp 协议解析, 独立子项目)
add_subdirectory(Web)

# 协程练习场 (main.cpp 用于练习写协程代码)
add_executable(coro_practice main.cpp)
target_link_libraries(coro_practice PRIVATE coro::coro)

# 高并发压力测试 (独立程序, 不计入常规 ctest)
add_executable(coro_stress tests/stress.cpp)
target_link_libraries(coro_stress PRIVATE coro::coro)

# ============================================================================
# 单元测试 (googletest, 集成于 thirdparty/googletest-main)
# ============================================================================
option(CORO_BUILD_TESTS "构建单元测试" ON)

if(CORO_BUILD_TESTS)
    enable_testing()
    # 禁止 gtest 的示例和内部测试, 只构建库
    set(gtest_build_samples OFF CACHE BOOL "" FORCE)
    set(gtest_build_tests OFF CACHE BOOL "" FORCE)
    set(BUILD_GMOCK OFF CACHE BOOL "" FORCE)
    # gtest 用与项目一致的动态 CRT (避免 MTd/MDd 链接冲突)
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    add_subdirectory(thirdparty/googletest-main)

    file(GLOB CORO_TEST_SOURCES CONFIGURE_DEPENDS tests/test_*.cpp)
    add_executable(coro_tests ${CORO_TEST_SOURCES})
    target_link_libraries(coro_tests PRIVATE coro::coro router::router GTest::gtest_main)
    add_test(NAME coro_tests COMMAND coro_tests)
endif()
