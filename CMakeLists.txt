# ============================================================================
# coro — C++20 协程事件循环框架 (header-only)
# ============================================================================
cmake_minimum_required(VERSION 3.20)
project(coro VERSION 0.1.0 LANGUAGES CXX)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

# 开发验证目标默认关闭: 作为 add_subdirectory 依赖时零附加构建;
# 开发/验证场景用 CMakePresets.json (dev/native/web) 或显式开启。
option(CORO_BUILD_EXAMPLES "构建示例与压力程序" OFF)
option(CORO_BUILD_TESTS "构建单元测试" OFF)
option(CORO_BUILD_BENCHMARKS "构建基准/压测程序" OFF)
# 原生 I/O 后端 (IOCP/io_uring) 与基础 IO 模块: Windows/Linux 默认开启, 其余平台只有纯协程核心
if(WIN32 OR CMAKE_SYSTEM_NAME STREQUAL "Linux")
    set(_CORO_NATIVE_IO_DEFAULT ON)
else()
    set(_CORO_NATIVE_IO_DEFAULT OFF)
endif()
option(CORO_ENABLE_NATIVE_IO "启用 Windows/Linux 原生 I/O 后端与基础 I/O 模块" ${_CORO_NATIVE_IO_DEFAULT})
option(CORO_ENABLE_URING "Linux 上启用 io_uring I/O 后端" ON)
option(CORO_REQUIRE_URING "配置时要求 Linux io_uring 依赖可用，禁止静默回退" OFF)
option(CORO_ENABLE_WEB "构建 Web 服务框架 (coro::web 与 web_server)" OFF)
if(CORO_REQUIRE_URING AND (NOT CMAKE_SYSTEM_NAME STREQUAL "Linux" OR NOT CORO_ENABLE_URING))
    message(FATAL_ERROR "CORO_REQUIRE_URING requires Linux and CORO_ENABLE_URING=ON")
endif()

# 单配置生成器 (Ninja/Makefile) 默认 Release; 多配置 (VS) 由 --config 决定
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "" FORCE)
endif()

# ============================================================================
# 库 target 分层 (header-only INTERFACE):
#   coro::core — 纯协程核心, 不携带任何第三方头与 I/O 链接依赖
#   coro::coro — 兼容入口, 在 core 之上追加原生 I/O 后端依赖
# 依赖方向单向 (coro -> coro_core), 核心不得反向感知 net/fs/tls/http。
# ============================================================================
add_library(coro_core INTERFACE)
add_library(coro::core ALIAS coro_core)
# 导出名必须显式指定: ALIAS 不参与 install(EXPORT) 命名, 否则消费者拿到的是
# coro::coro_core 而不是文档承诺的 coro::core。
set_target_properties(coro_core PROPERTIES EXPORT_NAME core)

target_include_directories(coro_core INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
target_compile_features(coro_core INTERFACE cxx_std_20)

# Scheduler 线程设施属于核心, 与是否启用原生 I/O 无关
if(UNIX)
    target_link_libraries(coro_core INTERFACE pthread)
endif()

add_library(coro INTERFACE)
add_library(coro::coro ALIAS coro)
target_link_libraries(coro INTERFACE coro_core)

if(MSVC)
    target_compile_options(coro_core INTERFACE /utf-8 /EHsc)
endif()
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS 14)
    target_compile_options(coro_core INTERFACE -fcoroutines)
endif()

# GCC 13 的 SLP 优化可能让协程帧函数指针引用已被丢弃的 COMDAT section。
# 保留其余优化；通过生成器表达式让安装包按消费者的编译器版本应用规避。
option(CORO_GCC13_COROUTINE_WORKAROUND "规避 GCC 13 协程链接错误（禁用 SLP 向量化）" ON)
if(CORO_GCC13_COROUTINE_WORKAROUND)
    target_compile_options(coro_core INTERFACE
        "$<$<AND:$<COMPILE_LANG_AND_ID:CXX,GNU>,$<VERSION_GREATER_EQUAL:$<CXX_COMPILER_VERSION>,13>,$<VERSION_LESS:$<CXX_COMPILER_VERSION>,14>>:-fno-tree-slp-vectorize>")
endif()

# 平台链接库: ws2_32 仅网络模块需要, 挂在兼容入口 coro 上
set(CORO_HAS_URING OFF)
if(WIN32)
    target_link_libraries(coro INTERFACE ws2_32)
endif()
if(CMAKE_SYSTEM_NAME STREQUAL "Linux" AND CORO_ENABLE_NATIVE_IO AND CORO_ENABLE_URING)
    # io_uring 后端统一使用仓库内源码 (thirdparty/liburing), 不查找系统预装库:
    # 避免"部分源码化 + 部分系统查找"造成两套依赖解析路径与 CI/本地行为不一致。
    if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/thirdparty/liburing/CMakeLists.txt")
        # 不用 EXCLUDE_FROM_ALL: 它会使子目录内的 install() 规则失效,
        # 导致安装导出引用到未安装的 liburing.a。
        add_subdirectory(thirdparty/liburing)
        set(CORO_HAS_URING ON)
        target_link_libraries(coro INTERFACE uring)
        message(STATUS "io_uring backend: in-tree liburing 2.9 source (thirdparty/liburing)")
    else()
        set(CORO_HAS_URING OFF)
        if(CORO_REQUIRE_URING)
            message(FATAL_ERROR
                "thirdparty/liburing source is missing; restore it or set CORO_ENABLE_URING=OFF")
        endif()
        message(WARNING "thirdparty/liburing source not found: Linux I/O modules are disabled; core timers/tasks use CVEventSource.")
    endif()
elseif(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    if(CORO_ENABLE_URING)
        message(STATUS "io_uring disabled by CORO_ENABLE_NATIVE_IO=OFF; building portable core only.")
    else()
        message(STATUS "io_uring disabled by CORO_ENABLE_URING=OFF; building portable core only.")
    endif()
endif()
# io_uring 能力单一事实源: 仅 Linux + 仓库内源码可用时为真, 由头文件据此派生
# CORO_HAS_{NET,FS,PIPE,FSWATCH,PROCESS} 等模块宏。
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

if(CORO_ENABLE_NATIVE_IO)
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
endif()

# Web 按 CORO_ENABLE_WEB 显式启用 (不再由示例/测试隐式拉起); 依赖原生网络与文件 IO。
# 属性跨独立入口的目录作用域可读 (Web 独立构建时经 CORO_WEB_BOOTSTRAP 引入)。
set_property(TARGET coro PROPERTY CORO_WEB_IO_AVAILABLE FALSE)
if(CORO_HAS_NET AND CORO_HAS_FS)
    set_property(TARGET coro PROPERTY CORO_WEB_IO_AVAILABLE TRUE)
endif()
if(CORO_ENABLE_WEB OR CORO_WEB_BOOTSTRAP)
    get_target_property(CORO_WEB_IO_AVAILABLE coro CORO_WEB_IO_AVAILABLE)
    if(NOT CORO_WEB_IO_AVAILABLE)
        message(FATAL_ERROR "Web requires native network and file I/O (Windows IOCP or Linux io_uring)")
    endif()
    add_subdirectory(Web/src)
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

if(CORO_ENABLE_WEB AND CORO_HAS_SIGNAL)
    add_executable(web_shutdown_check tests/web_shutdown_check.cpp)
    target_link_libraries(web_shutdown_check PRIVATE coro::web)
    if(MSVC)
        target_compile_options(web_shutdown_check PRIVATE /utf-8)
    endif()
endif()

# Web 服务器示例 (coro 网络层 + llhttp 协议解析, 独立子项目)
if(CORO_ENABLE_WEB AND NOT CORO_WEB_BOOTSTRAP)
    add_subdirectory(Web)
endif()
if(CORO_HAS_NET)
    add_subdirectory(tcp_udp)
endif()

# 协程练习场 (main.cpp 用于练习写协程代码)
# 练习场当前使用 tcp_udp 组件, 因此与网络能力一同注册: 否则 core-only + EXAMPLES=ON
# 的配置会因缺少 <tcp_udp/...> 而构建失败 (该组合此前从未被验证)。
if(CORO_HAS_NET)
    add_executable(coro_practice main.cpp)
    target_link_libraries(coro_practice PRIVATE coro::coro coro::tcp_udp)
endif()

# 高并发压力测试 (独立程序, 不计入常规 ctest)
if(CORO_BUILD_BENCHMARKS OR CORO_BUILD_EXAMPLES)
    add_executable(coro_stress tests/stress.cpp)
    target_link_libraries(coro_stress PRIVATE coro::coro)
endif()
endif()

# ============================================================================
# 单元测试 (googletest, 集成于 thirdparty/googletest-main)
# ============================================================================
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
    # Web 测试依赖文件和网络 I/O, 在能力可用时单独加入, 与示例开关无关。
    list(REMOVE_ITEM CORO_TEST_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/tests/test_web_layer.cpp")
    add_executable(coro_tests ${CORO_TEST_SOURCES})
    target_link_libraries(coro_tests PRIVATE coro::coro router::router GTest::gtest_main)
    # 服务器生命周期测试需要 tcp_udp 组件 (无网络能力的构建里该测试文件自身会被门控)
    if(TARGET coro_tcp_udp)
        target_link_libraries(coro_tests PRIVATE coro::tcp_udp)
    endif()
    if(CORO_ENABLE_WEB)
        target_sources(coro_tests PRIVATE tests/test_web_layer.cpp)
        target_link_libraries(coro_tests PRIVATE coro::web http::http)
        if(MSVC)
            target_compile_options(coro_tests PRIVATE /utf-8)
        endif()
    endif()
    add_test(NAME coro_tests COMMAND coro_tests)
    set_tests_properties(coro_tests PROPERTIES TIMEOUT 120)

    # ---- 公共头独立包含自检 ----
    # 每个头文件单独成一个 TU, 抽出“靠 include 顺序碰巧能过”的隐式依赖。
    # 生成物只进构建目录, 不污染源码树; 目标随 all 一起编译, 所以 CI 的
    # "Build all targets" 就是它的门禁, 无需额外构建一轮。
    file(GLOB CORO_PUBLIC_HEADERS CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/include/coro/*.hpp)
    set(CORO_HEADER_CHECK_SOURCES "")
    foreach(header IN LISTS CORO_PUBLIC_HEADERS)
        get_filename_component(header_name "${header}" NAME_WE)
        file(GENERATE
            OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/header_check/${header_name}.cpp"
            CONTENT "#include <coro/${header_name}.hpp>\n")
        list(APPEND CORO_HEADER_CHECK_SOURCES
            "${CMAKE_CURRENT_BINARY_DIR}/header_check/${header_name}.cpp")
    endforeach()
    add_library(coro_header_check OBJECT ${CORO_HEADER_CHECK_SOURCES})
    target_link_libraries(coro_header_check PRIVATE coro::coro)
    if(MSVC)
        target_compile_options(coro_header_check PRIVATE /utf-8)
    endif()
    if(TARGET web_server)
        add_test(NAME web_selftest COMMAND web_server --selftest 18081)
        set_tests_properties(web_selftest PROPERTIES
            TIMEOUT 30 LABELS integration
            FAIL_REGULAR_EXPRESSION "\\[selftest\\] FAIL:")
        add_test(NAME web_selftest_invalid_port COMMAND web_server --selftest 0)
        add_test(NAME web_selftest_malformed_port COMMAND web_server --selftest 18081junk)
        set_tests_properties(web_selftest_invalid_port web_selftest_malformed_port PROPERTIES
            TIMEOUT 5 WILL_FAIL TRUE LABELS integration)
        add_test(NAME web_stress_smoke COMMAND web_server --stress 2 3)
        set_tests_properties(web_stress_smoke PROPERTIES TIMEOUT 30 LABELS integration RESOURCE_LOCK web_port_8080)
        set(case_index 0)
        foreach(args IN ITEMS "0;1" "2junk;1" "999999999999999999;1" "1;-1" "1;1;extra")
            math(EXPR case_index "${case_index} + 1")
            add_test(NAME web_stress_invalid_${case_index}
                COMMAND ${CMAKE_COMMAND} "-DPROGRAM=$<TARGET_FILE:web_server>"
                    "-DARGS=--stress;${args}" -DEXPECTED=2
                    -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/web_cli_check.cmake)
            set_tests_properties(web_stress_invalid_${case_index} PROPERTIES TIMEOUT 15 LABELS integration)
        endforeach()
    endif()
    if(TARGET web_shutdown_check)
        add_test(NAME web_shutdown_check COMMAND web_shutdown_check)
        set_tests_properties(web_shutdown_check PROPERTIES TIMEOUT 30 LABELS integration)
    endif()
endif()

# ============================================================================
# 安装与 find_package(coro CONFIG REQUIRED)
# ============================================================================
install(TARGETS coro coro_core EXPORT coroTargets)
install(DIRECTORY include/coro DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
# tcp_udp 是正式组件: 导出 target 并安装头文件, 保持与源码树相同的 <tcp_udp/...> 写法
if(TARGET coro_tcp_udp)
    install(TARGETS coro_tcp_udp EXPORT coroTargets)
    install(FILES tcp_udp/tcp_server.hpp tcp_udp/udp_server.hpp
            DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/tcp_udp)
endif()

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
