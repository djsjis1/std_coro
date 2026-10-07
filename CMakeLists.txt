# ============================================================================
# coro — C++20 协程核心与可选 I/O、TLS、HTTP 组件
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
option(CORO_ENABLE_HTTP "构建独立 HTTP 解析与报文组件 (coro::http_protocol)" OFF)
option(CORO_ENABLE_HTTP_CLIENT "构建协程 HTTP/HTTPS 客户端 (coro::http_client)" OFF)
option(CORO_ENABLE_HTTP_SERVER "构建独立 HTTP/HTTPS 服务器 (coro::http_server)" OFF)
option(CORO_ENABLE_TLS "启用 TLS API (依赖仓库内 OpenSSL 源码)" OFF)
option(CORO_REQUIRE_TLS "配置时要求 thirdparty/openssl 源码可用, 禁止静默降级" OFF)
# 并发工具扩展 (Timer/Context/Channel/select/...): 只依赖纯核心, 零第三方
option(CORO_ENABLE_CONCURRENCY_EXT "启用新增并发工具头与对应测试" OFF)
if(CORO_REQUIRE_URING AND (NOT CMAKE_SYSTEM_NAME STREQUAL "Linux" OR NOT CORO_ENABLE_URING OR NOT CORO_ENABLE_NATIVE_IO))
    message(FATAL_ERROR "CORO_REQUIRE_URING requires Linux, CORO_ENABLE_NATIVE_IO=ON and CORO_ENABLE_URING=ON")
endif()

# 单配置生成器 (Ninja/Makefile) 默认 Release; 多配置 (VS) 由 --config 决定
if(CMAKE_SOURCE_DIR STREQUAL PROJECT_SOURCE_DIR AND NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "" FORCE)
endif()

# ============================================================================
# 库 target 分层:
#   coro::core — 纯协程核心, 不携带任何第三方头与 I/O 链接依赖
#   coro::io   — core + 原生 I/O 后端
#   coro::tls  — io + 私有 OpenSSL 实现 (可选静态库)
#   coro::coro — 兼容入口, 聚合 io 与已启用的 tls
# 核心不得反向感知 net/fs/tls/http。HTTP 协议与 router 独立于协程核心。
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

# 让 CMake 按工具链选择线程编译/链接选项, 避免硬编码 pthread。
set(THREADS_PREFER_PTHREAD_FLAG TRUE)
find_package(Threads REQUIRED)
target_link_libraries(coro_core INTERFACE Threads::Threads)

add_library(coro_io INTERFACE)
add_library(coro::io ALIAS coro_io)
set_target_properties(coro_io PROPERTIES EXPORT_NAME io)
target_link_libraries(coro_io INTERFACE coro_core)

# 兼容入口聚合已启用组件; 组件自身只能依赖更低层, 不能反向依赖此入口。
add_library(coro INTERFACE)
add_library(coro::coro ALIAS coro)
target_link_libraries(coro INTERFACE coro_io)

if(MSVC)
    target_compile_options(coro_core INTERFACE /utf-8 /EHsc)
endif()
target_compile_options(coro_core INTERFACE
    "$<$<AND:$<COMPILE_LANG_AND_ID:CXX,GNU>,$<VERSION_LESS:$<CXX_COMPILER_VERSION>,14>>:-fcoroutines>")

# GCC 13 的 SLP 优化可能让协程帧函数指针引用已被丢弃的 COMDAT section。
# 保留其余优化；通过生成器表达式让安装包按消费者的编译器版本应用规避。
option(CORO_GCC13_COROUTINE_WORKAROUND "规避 GCC 13 协程链接错误（禁用 SLP 向量化）" ON)
if(CORO_GCC13_COROUTINE_WORKAROUND)
    target_compile_options(coro_core INTERFACE
        "$<$<AND:$<COMPILE_LANG_AND_ID:CXX,GNU>,$<VERSION_GREATER_EQUAL:$<CXX_COMPILER_VERSION>,13>,$<VERSION_LESS:$<CXX_COMPILER_VERSION>,14>>:-fno-tree-slp-vectorize>")
endif()

# 平台链接库: ws2_32 仅网络模块需要, 挂在 io 组件上
set(CORO_HAS_URING OFF)
if(WIN32)
    target_link_libraries(coro_io INTERFACE ws2_32)
endif()
if(CMAKE_SYSTEM_NAME STREQUAL "Linux" AND CORO_ENABLE_NATIVE_IO AND CORO_ENABLE_URING)
    # io_uring 后端统一使用仓库内源码 (thirdparty/liburing), 不查找系统预装库:
    # 避免"部分源码化 + 部分系统查找"造成两套依赖解析路径与 CI/本地行为不一致。
    if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/thirdparty/liburing/CMakeLists.txt")
        # 不用 EXCLUDE_FROM_ALL: 它会使子目录内的 install() 规则失效,
        # 导致安装导出引用到未安装的 liburing.a。
        add_subdirectory(thirdparty/liburing)
        set(CORO_HAS_URING ON)
        target_link_libraries(coro_io INTERFACE uring)
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
target_compile_definitions(coro_io INTERFACE CORO_HAS_URING=$<BOOL:${CORO_HAS_URING}>)
# 并发扩展能力同样单点派生: 关闭时相关实现与测试整体不参与编译 (不是只屏蔽几个调用点)
target_compile_definitions(coro_core INTERFACE CORO_HAS_CONCURRENCY_EXT=$<BOOL:${CORO_ENABLE_CONCURRENCY_EXT}>)

# TLS: 与 io_uring 同一套语义 —— 只用仓库内 OpenSSL 源码, 配置期绝不联网、不查系统
# 预装开发库; 缺源码时 REQUIRE_TLS=ON 直接失败, 否则警告并降级 (CORO_HAS_TLS=0)。
# 关闭开关时下面整段不参与: 不 add_subdirectory、不探测 Perl/NASM、不注册测试与示例。
set(CORO_HAS_TLS OFF)
if(CORO_REQUIRE_TLS AND NOT CORO_ENABLE_TLS)
    message(FATAL_ERROR "CORO_REQUIRE_TLS requires CORO_ENABLE_TLS=ON")
endif()
if(CORO_ENABLE_TLS)
    if(NOT CORO_ENABLE_NATIVE_IO OR (NOT CORO_HAS_URING AND NOT WIN32))
        # TLS 需要真正的 socket 后端 (Linux 走 io_uring, Windows 走 IOCP)。
        message(WARNING "CORO_ENABLE_TLS=ON 但原生网络不可用 (需要 Linux+io_uring 或 Windows); TLS 已禁用。")
    elseif(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/thirdparty/openssl/CMakeLists.txt")
        add_subdirectory(thirdparty/openssl)
        set(CORO_HAS_TLS ON)
        add_library(coro_tls STATIC src/tls.cpp)
        add_library(coro::tls ALIAS coro_tls)
        set_target_properties(coro_tls PROPERTIES EXPORT_NAME tls POSITION_INDEPENDENT_CODE ON)
        target_link_libraries(coro_tls PUBLIC coro_io PRIVATE
            $<BUILD_INTERFACE:openssl_ssl>
            $<INSTALL_INTERFACE:coro::openssl_ssl>)
        target_compile_definitions(coro_tls PUBLIC CORO_HAS_TLS=1)
        target_link_libraries(coro INTERFACE coro_tls)
        message(STATUS "TLS backend: in-tree OpenSSL source (thirdparty/openssl)")
    else()
        if(CORO_REQUIRE_TLS)
            message(FATAL_ERROR
                "thirdparty/openssl source is missing; restore it or set CORO_ENABLE_TLS=OFF")
        endif()
        message(WARNING "thirdparty/openssl source not found: coro::tls 已禁用 (CORO_HAS_TLS=0)。")
    endif()
endif()
if(CORO_REQUIRE_TLS AND NOT CORO_HAS_TLS)
    message(FATAL_ERROR "CORO_REQUIRE_TLS requires a native socket backend and vendored OpenSSL")
endif()
# TLS 能力单点派生: 头文件与测试只用 defined(CORO_HAS_TLS) && CORO_HAS_TLS 判定。
if(NOT CORO_HAS_TLS)
    target_compile_definitions(coro INTERFACE CORO_HAS_TLS=0)
endif()

# 路由库: header-only 基数树动态路由, 目标名 router::router
add_subdirectory(router)

# HTTP 只依赖 llhttp, 可在 NATIVE_IO=OFF 的构建中独立消费。
if(CORO_ENABLE_HTTP OR CORO_ENABLE_HTTP_CLIENT OR CORO_ENABLE_HTTP_SERVER OR CORO_ENABLE_WEB OR CORO_WEB_BOOTSTRAP)
    set(CORO_HTTP_EXPORT_WITH_CORO ON)
    set(STD_HTTP_BUILD_EXAMPLES OFF)
    add_subdirectory(thirdparty/http)
endif()

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

# Transport consumers are optional and share protocol code without depending on Web.
if(CORO_ENABLE_HTTP_CLIENT OR CORO_ENABLE_HTTP_SERVER)
    if(NOT CORO_HAS_NET)
        message(FATAL_ERROR "HTTP client/server require native network I/O")
    endif()
    foreach(_http_component IN ITEMS CLIENT SERVER)
        if(CORO_ENABLE_HTTP_${_http_component})
            string(TOLOWER "${_http_component}" _http_name)
            add_library(coro_http_${_http_name} STATIC src/http_${_http_name}.cpp)
            add_library(coro::http_${_http_name} ALIAS coro_http_${_http_name})
            set_target_properties(coro_http_${_http_name} PROPERTIES
                EXPORT_NAME http_${_http_name} POSITION_INDEPENDENT_CODE ON)
            target_link_libraries(coro_http_${_http_name} PUBLIC coro::io PRIVATE coro::http_protocol)
            target_compile_definitions(coro_http_${_http_name} PUBLIC CORO_HAS_HTTP_${_http_component}=1)
            if(CORO_HAS_TLS)
                target_link_libraries(coro_http_${_http_name} PRIVATE coro::tls)
            endif()
        endif()
    endforeach()
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
    # 示例注册辅助: 默认链接 coro::coro, 特殊依赖用 LINK_LIBS 覆盖。
    function(coro_add_example name source)
        cmake_parse_arguments(ARG "" "" "LINK_LIBS" ${ARGN})
        add_executable(${name} ${source})
        if(ARG_LINK_LIBS)
            target_link_libraries(${name} PRIVATE ${ARG_LINK_LIBS})
        else()
            target_link_libraries(${name} PRIVATE coro::coro)
        endif()
    endfunction()

    if(CORO_HAS_TLS)
        coro_add_example(example_tls examples/tls_demo.cpp LINK_LIBS coro::tls)
    endif()

    # 核心并发示例: 只依赖 coro::coro, core-only 构建同样可用
    coro_add_example(example_basic examples/basic.cpp)

    coro_add_example(example_gather examples/gather.cpp)

    coro_add_example(example_future examples/future.cpp)

    coro_add_example(example_full_test examples/full_test.cpp)

    coro_add_example(example_task_group examples/task_group.cpp)

    coro_add_example(example_to_thread examples/to_thread.cpp)

    coro_add_example(example_wait_timeout examples/wait_timeout.cpp)

    coro_add_example(example_scheduler examples/scheduler.cpp)

    if(CORO_HAS_NET)
        coro_add_example(example_echo examples/echo_server.cpp)
    endif()

    if(CORO_HAS_FS)
        coro_add_example(example_file_io examples/file_io.cpp)
    endif()

    if(CORO_HAS_FSWATCH)
        coro_add_example(example_dir_watch examples/dir_watch.cpp)
    endif()

    if(CORO_HAS_PROCESS)
        coro_add_example(example_process examples/process_demo.cpp)
    endif()

    # 并发扩展示例: 受自己的开关门控, 不开关时既不编译也不拉进纯核心消费者的依赖面
    if(CORO_ENABLE_CONCURRENCY_EXT)
        coro_add_example(example_timer_context examples/timer_context.cpp)

        coro_add_example(example_channel examples/channel_pipeline.cpp)

        coro_add_example(example_select examples/select_demo.cpp)

        coro_add_example(example_rate_limit examples/rate_limit_demo.cpp)

        coro_add_example(example_shield examples/shield_demo.cpp)
    endif()

    # 流适配示例: 需要真实 socket 与 pipe, 因此与网络能力同进同出
    if(CORO_HAS_NET)
        coro_add_example(example_stream examples/stream_demo.cpp)

        coro_add_example(example_dns examples/dns_demo.cpp)
    endif()

    if(CORO_ENABLE_WEB AND CORO_HAS_SIGNAL)
        add_executable(web_shutdown_check tests/web_shutdown_check.cpp)
        target_link_libraries(web_shutdown_check PRIVATE coro::web)
        if(MSVC)
            target_compile_options(web_shutdown_check PRIVATE /utf-8)
        endif()
    endif()

    # 协程练习场 (playground/ 用于练习写协程代码, 不参与安装)
    # 练习场当前使用 tcp_udp 组件, 因此与网络能力一同注册: 否则 core-only + EXAMPLES=ON
    # 的配置会因缺少 <tcp_udp/...> 而构建失败 (该组合此前从未被验证)。
    if(CORO_HAS_NET)
        coro_add_example(coro_practice playground/main.cpp LINK_LIBS coro::coro coro::tcp_udp)
    endif()

endif() # CORO_BUILD_EXAMPLES

# 高并发压力测试可独立开启, 不依赖 CORO_BUILD_EXAMPLES。
if(CORO_BUILD_BENCHMARKS OR CORO_BUILD_EXAMPLES)
    add_executable(coro_stress tests/stress.cpp)
    target_link_libraries(coro_stress PRIVATE coro::coro)
endif()

# ============================================================================
# 组件子目录 (定义 target, 与"是否构建示例"无关)
# ============================================================================
# 这两个 add_subdirectory 原先误嵌在 if(CORO_BUILD_EXAMPLES) 块内, 于是
# EXAMPLES=OFF 的配置 (Sanitizer / Coverage 作业) 拿不到 coro::web 与
# coro::tcp_udp —— 表现为 coro_tests 里的服务器生命周期测试找不到
# <tcp_udp/tcp_server.hpp>。组件的存在性只应由能力决定, 示例注册才由开关决定。
if(CORO_ENABLE_WEB AND NOT CORO_WEB_BOOTSTRAP)
    add_subdirectory(Web)
endif()
if(CORO_HAS_NET)
    add_subdirectory(tcp_udp)
endif()
if(TARGET coro_http_server)
    target_link_libraries(coro_http_server PRIVATE coro::tcp_udp)
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
install(TARGETS coro coro_core coro_io router EXPORT coroTargets)
install(FILES router/radix_router.h DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/coro/router)
if(TARGET coro_tls)
    install(TARGETS coro_tls EXPORT coroTargets ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR})
endif()
foreach(_http_name IN ITEMS client server)
    if(TARGET coro_http_${_http_name})
        install(TARGETS coro_http_${_http_name} EXPORT coroTargets ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR})
    endif()
endforeach()
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
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/coro
    PATH_VARS CMAKE_INSTALL_LIBDIR CMAKE_INSTALL_INCLUDEDIR)
write_basic_package_version_file(
    ${CMAKE_CURRENT_BINARY_DIR}/coroConfigVersion.cmake
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion)
install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/coroConfig.cmake
    ${CMAKE_CURRENT_BINARY_DIR}/coroConfigVersion.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/coro)
