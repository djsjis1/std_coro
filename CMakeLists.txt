# ============================================================================
# coro — C++20 协程事件循环框架 (header-only)
# ============================================================================
cmake_minimum_required(VERSION 3.20)
project(coro VERSION 0.1.0 LANGUAGES CXX)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

option(CORO_BUILD_EXAMPLES "构建示例与压力程序" ON)
option(CORO_ENABLE_URING "Linux 上启用 io_uring I/O 后端" ON)
option(CORO_REQUIRE_URING "配置时要求 Linux io_uring 依赖可用，禁止静默回退" OFF)
if(CORO_REQUIRE_URING AND (NOT CMAKE_SYSTEM_NAME STREQUAL "Linux" OR NOT CORO_ENABLE_URING))
    message(FATAL_ERROR "CORO_REQUIRE_URING requires Linux and CORO_ENABLE_URING=ON")
endif()

# 单配置生成器 (Ninja/Makefile) 默认 Release; 多配置 (VS) 由 --config 决定
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "" FORCE)
endif()

# ---- 库 target: header-only INTERFACE 库 ----
add_library(coro INTERFACE)
add_library(coro::coro ALIAS coro)

target_include_directories(coro INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
target_compile_features(coro INTERFACE cxx_std_20)

if(MSVC)
    target_compile_options(coro INTERFACE /utf-8 /EHsc)
endif()
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS 14)
    target_compile_options(coro INTERFACE -fcoroutines)
endif()

# GCC 13 的 SLP 优化可能让协程帧函数指针引用已被丢弃的 COMDAT section。
# 保留其余优化；通过生成器表达式让安装包按消费者的编译器版本应用规避。
option(CORO_GCC13_COROUTINE_WORKAROUND "规避 GCC 13 协程链接错误（禁用 SLP 向量化）" ON)
if(CORO_GCC13_COROUTINE_WORKAROUND)
    target_compile_options(coro INTERFACE
        "$<$<AND:$<COMPILE_LANG_AND_ID:CXX,GNU>,$<VERSION_GREATER_EQUAL:$<CXX_COMPILER_VERSION>,13>,$<VERSION_LESS:$<CXX_COMPILER_VERSION>,14>>:-fno-tree-slp-vectorize>")
endif()

# 平台链接库
set(CORO_HAS_URING OFF)
if(WIN32)
    target_link_libraries(coro INTERFACE ws2_32)
endif()
if(UNIX)
    target_link_libraries(coro INTERFACE pthread)
endif()
if(CMAKE_SYSTEM_NAME STREQUAL "Linux" AND CORO_ENABLE_URING)
    find_path(URING_INCLUDE_DIR liburing.h)
    find_library(URING_LIB uring)
    if(URING_INCLUDE_DIR AND URING_LIB)
        set(CORO_HAS_URING ON)
        if(NOT TARGET uring::uring)
            add_library(uring::uring UNKNOWN IMPORTED)
            set_target_properties(uring::uring PROPERTIES
                IMPORTED_LOCATION "${URING_LIB}"
                INTERFACE_INCLUDE_DIRECTORIES "${URING_INCLUDE_DIR}")
        endif()
        target_link_libraries(coro INTERFACE uring::uring)

    else()
        set(CORO_HAS_URING OFF)
        if(CORO_REQUIRE_URING)
            message(FATAL_ERROR "liburing headers/library not found; install liburing-dev or liburing-devel")
        endif()
        message(WARNING "liburing headers/library not found: Linux I/O modules are disabled; core timers/tasks use CVEventSource.")
    endif()
elseif(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    message(STATUS "io_uring disabled by CORO_ENABLE_URING=OFF; building portable core only.")
endif()
target_compile_definitions(coro INTERFACE CORO_HAS_URING=$<BOOL:${CORO_HAS_URING}>)

# 路由库: header-only 基数树动态路由, 目标名 router::router
add_subdirectory(router)

# MSVC: 限制单个项目内的编译并行度，避免与 MSBuild 并行叠加耗尽内存。
if(MSVC)
    add_compile_options(/MP2)
endif()

# ============================================================================
# 平台检测: IO 模块可用性 (与 coro.hpp 分层架构一致)
# ============================================================================
set(CORO_HAS_NET  OFF)
set(CORO_HAS_FS   OFF)
set(CORO_HAS_PIPE OFF)
set(CORO_HAS_SIGNAL OFF)
set(CORO_HAS_FSWATCH OFF)
set(CORO_HAS_PROCESS OFF)

if(WIN32)
    set(CORO_HAS_NET ON)
    set(CORO_HAS_FS ON)
    set(CORO_HAS_PIPE ON)
    set(CORO_HAS_SIGNAL ON)
    set(CORO_HAS_FSWATCH ON)
    set(CORO_HAS_PROCESS ON)
elseif(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    set(CORO_HAS_NET ${CORO_HAS_URING})
    set(CORO_HAS_FS ${CORO_HAS_URING})
    set(CORO_HAS_PIPE ${CORO_HAS_URING})
    set(CORO_HAS_SIGNAL ON)
    set(CORO_HAS_FSWATCH ${CORO_HAS_URING})
    set(CORO_HAS_PROCESS ${CORO_HAS_URING})
endif()

# ============================================================================
# 示例
# ============================================================================
if(CORO_BUILD_EXAMPLES)
add_executable(example_basic examples/basic.cpp)
target_link_libraries(example_basic PRIVATE coro::coro)

add_executable(example_gather examples/gather.cpp)
target_link_libraries(example_gather PRIVATE coro::coro)

add_executable(example_future examples/future.cpp)
target_link_libraries(example_future PRIVATE coro::coro)

add_executable(example_full_test examples/full_test.cpp)
target_link_libraries(example_full_test PRIVATE coro::coro)

if(CORO_HAS_NET)
    add_executable(example_echo examples/echo_server.cpp)
    target_link_libraries(example_echo PRIVATE coro::coro)
endif()

if(CORO_HAS_FS)
    add_executable(example_file_io examples/file_io.cpp)
    target_link_libraries(example_file_io PRIVATE coro::coro)
endif()

if(CORO_HAS_FSWATCH)
    add_executable(example_dir_watch examples/dir_watch.cpp)
    target_link_libraries(example_dir_watch PRIVATE coro::coro)
endif()

if(CORO_HAS_PROCESS)
    add_executable(example_process examples/process_demo.cpp)
    target_link_libraries(example_process PRIVATE coro::coro)
endif()

if(CORO_HAS_NET AND CORO_HAS_SIGNAL)
    add_executable(web_shutdown_check
        tests/web_shutdown_check.cpp
        Web/src/http_types.cpp
        Web/src/web_server.cpp)
    target_link_libraries(web_shutdown_check PRIVATE coro::coro http::http router::router)
    target_include_directories(web_shutdown_check PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/Web/src)
    if(MSVC)
        target_compile_options(web_shutdown_check PRIVATE /utf-8)
    endif()
endif()

# Web 服务器示例 (coro 网络层 + llhttp 协议解析, 独立子项目)
if(CORO_HAS_NET)
    add_subdirectory(Web)
    add_subdirectory(tcp_udp)
endif()

# 协程练习场 (main.cpp 用于练习写协程代码)
add_executable(coro_practice main.cpp)
target_link_libraries(coro_practice PRIVATE coro::coro)

# 高并发压力测试 (独立程序, 不计入常规 ctest)
add_executable(coro_stress tests/stress.cpp)
target_link_libraries(coro_stress PRIVATE coro::coro)
endif()

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
    set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
    # gtest 用与项目一致的动态 CRT (避免 MTd/MDd 链接冲突)
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    add_subdirectory(thirdparty/googletest-main EXCLUDE_FROM_ALL)

    file(GLOB CORO_TEST_SOURCES CONFIGURE_DEPENDS tests/test_*.cpp)
    add_executable(coro_tests ${CORO_TEST_SOURCES})
    target_link_libraries(coro_tests PRIVATE coro::coro router::router GTest::gtest_main)
    add_test(NAME coro_tests COMMAND coro_tests)
    set_tests_properties(coro_tests PROPERTIES TIMEOUT 120)
    if(TARGET web_server)
        add_test(NAME web_selftest COMMAND web_server --selftest 18081)
        set_tests_properties(web_selftest PROPERTIES
            TIMEOUT 30 LABELS integration
            FAIL_REGULAR_EXPRESSION "\\[selftest\\] FAIL:")
        add_test(NAME web_selftest_invalid_port COMMAND web_server --selftest 0)
        add_test(NAME web_selftest_malformed_port COMMAND web_server --selftest 18081junk)
        set_tests_properties(web_selftest_invalid_port web_selftest_malformed_port PROPERTIES
            TIMEOUT 5 WILL_FAIL TRUE LABELS integration)
    endif()
    if(TARGET web_shutdown_check)
        add_test(NAME web_shutdown_check COMMAND web_shutdown_check)
        set_tests_properties(web_shutdown_check PROPERTIES TIMEOUT 30 LABELS integration)
    endif()
endif()

# ============================================================================
# 安装与 find_package(coro CONFIG REQUIRED)
# ============================================================================
install(TARGETS coro EXPORT coroTargets)
install(DIRECTORY include/coro DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

install(EXPORT coroTargets
    FILE coroTargets.cmake
    NAMESPACE coro::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/coro)

configure_package_config_file(
    cmake/coroConfig.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/coroConfig.cmake
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/coro)
write_basic_package_version_file(
    ${CMAKE_CURRENT_BINARY_DIR}/coroConfigVersion.cmake
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion)
install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/coroConfig.cmake
    ${CMAKE_CURRENT_BINARY_DIR}/coroConfigVersion.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/coro)
