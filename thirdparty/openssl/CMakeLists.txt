# thirdparty/openssl/CMakeLists.txt
#
# 对上游 OpenSSL 3.5.8 (LTS) 源码树的 CMake 包装层。与 thirdparty/liburing 同一思路:
# CMake 只做**编排**, 构建交给上游自己的构建系统 (OpenSSL 3.x 用 perl Configure + make),
# 绝不把上游源码拆成 CMake 目标逐文件编译 —— 那等于复刻它的整个构建系统, 每次升级都会烂。
#
# 产物: 静态库 openssl_ssl (libssl) 与 openssl_crypto (libcrypto), 名字由根 CMake 的
#   target_link_libraries(coro INTERFACE openssl_ssl openssl_crypto) 约定。
# 边界: 只构建库本体 (no-tests no-docs), 不装可执行文件; NASM 缺失时统一 no-asm,
#   保证"有汇编的机器/没汇编的机器"产出同一套 C 实现, 结果可复现。
#
# 版本升级流程: 替换本目录的上游源码树 → 确认 VERSION.dat → 删构建缓存 → 跑 TLS 门禁 →
#   更新根 README 的版本记录与许可文件引用 (APACHE-2.0, 见上游 LICENSE.txt)。

cmake_minimum_required(VERSION 3.20)
project(coro_openssl_vendored LANGUAGES C)

set(_ossl_src "${CMAKE_CURRENT_SOURCE_DIR}")
set(_ossl_out "${CMAKE_CURRENT_BINARY_DIR}")

if(NOT EXISTS "${_ossl_src}/Configure")
    message(FATAL_ERROR "thirdparty/openssl: 上游源码不完整 (缺 Configure)。请恢复 openssl-3.5.8 源码树。")
endif()
if(NOT EXISTS "${_ossl_src}/VERSION.dat")
    message(FATAL_ERROR "thirdparty/openssl: 缺 VERSION.dat, 无法确认版本 (需要 3.5.8)。")
endif()
file(READ "${_ossl_src}/VERSION.dat" _ossl_version_dat)
if(NOT _ossl_version_dat MATCHES "MAJOR=3" OR NOT _ossl_version_dat MATCHES "MINOR=5")
    message(FATAL_ERROR "thirdparty/openssl: 期望 OpenSSL 3.5.x (项目决策的 LTS 基线), 源码树版本不符:\n${_ossl_version_dat}")
endif()

find_package(Perl REQUIRED)

if(MSVC)
    # Windows 的 nmake/JOM 编排是独立的工程量 (打开/关闭 /MT、Strawberry Perl 路径等),
    # 首个版本先把 Linux 通道做实; Windows 上显式报配置错误, 而不是悄悄产出坏库。
    message(FATAL_ERROR "thirdparty/openssl: Windows 构建通道尚未实现 (计划内, 待 Linux 通道验证后补齐); "
                        "请先用 CORO_ENABLE_TLS=OFF。")
endif()

# ---- 目标三元组: 与 Configure 的 target 名对齐 ----
if(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|arm64|ARM64")
    set(_ossl_conf_target "linux-aarch64")
else()
    set(_ossl_conf_target "linux-x86_64")
endif()

find_program(MAKE_EXE make REQUIRED)

# ---- 产物路径 (构建目录内, 不污染源码树) ----
set(_ossl_libssl "${_ossl_out}/libssl.a")
set(_ossl_libcrypto "${_ossl_out}/libcrypto.a")
# 副本的 include/ 同时容纳源码头与 Configure 生成头 (opensslconf.h 等)
set(_ossl_inc_all "${_ossl_out}/include")

# OpenSSL 3.x 是**源码内构建**: Configure 与 make 都把生成物写进当前工作目录。
# 若直接在 thirdparty/openssl/ 里跑, Debug/Release 两个构建目录会互相踩踏, 还会把
# configdata.pm / 生成头污染进 git。因此在配置期把源码树**复制**到本构建目录, 一切
# 都发生在副本里 —— 每个 build type 拿到独立副本, 天然隔离。
file(COPY "${_ossl_src}/" DESTINATION "${_ossl_out}"
     FILES_MATCHING
     PATTERN "*"
     PATTERN "*.git*" EXCLUDE)

# Configure 在副本根目录运行 (OpenSSL 的约定: 生成 configdata.pm 与 include/openssl/*.h)
add_custom_command(
    OUTPUT "${_ossl_out}/configdata.pm.done"
    COMMAND ${PERL_EXECUTABLE} ./Configure "${_ossl_conf_target}"
            no-shared no-asm no-tests no-docs
            "--prefix=${_ossl_out}/install"
            "--openssldir=${_ossl_out}/install/ssl"
    COMMAND ${CMAKE_COMMAND} -E touch "${_ossl_out}/configdata.pm.done"
    WORKING_DIRECTORY "${_ossl_out}"
    DEPENDS "${_ossl_src}/Configure" "${_ossl_src}/VERSION.dat"
    COMMENT "OpenSSL 3.5.8: perl Configure ${_ossl_conf_target} no-shared no-asm no-tests no-docs"
    VERBATIM)

# build_libs 只构建 libcrypto + libssl (绕过 apps/fuzz/全部测试), 显著缩短构建时间
add_custom_command(
    OUTPUT "${_ossl_libssl}" "${_ossl_libcrypto}"
    COMMAND ${MAKE_EXE} -j1 build_libs
    WORKING_DIRECTORY "${_ossl_out}"
    DEPENDS "${_ossl_out}/configdata.pm.done"
    COMMENT "OpenSSL 3.5.8: make build_libs (-j1, 低内存机器的安全档位)"
    VERBATIM)

add_custom_target(openssl_vendored_build
    DEPENDS "${_ossl_libssl}" "${_ossl_libcrypto}")

# ---- 暴露成根 CMake 约定的两个目标名 ----
# 用 IMPORTED + 全局属性而不是 add_library(...STATIC): 编译由上游 make 负责, CMake 不参与。
# add_dependencies 对 IMPORTED 目标同样生效, 用来保证链接前库已存在。
add_library(openssl_ssl STATIC IMPORTED GLOBAL)
add_library(openssl_crypto STATIC IMPORTED GLOBAL)
set_target_properties(openssl_ssl PROPERTIES
    IMPORTED_LOCATION "${_ossl_libssl}"
    INTERFACE_INCLUDE_DIRECTORIES "${_ossl_inc_all}")
set_target_properties(openssl_crypto PROPERTIES
    IMPORTED_LOCATION "${_ossl_libcrypto}"
    INTERFACE_INCLUDE_DIRECTORIES "${_ossl_inc_all}")
# 依赖链: libssl 依赖 libcrypto; libcrypto 在 Linux 还要 dl (OpenSSL 静态库的常态)
set_property(TARGET openssl_ssl APPEND PROPERTY INTERFACE_LINK_LIBRARIES openssl_crypto)
if(UNIX)
    set_property(TARGET openssl_crypto APPEND PROPERTY INTERFACE_LINK_LIBRARIES dl)
endif()
add_dependencies(openssl_ssl openssl_vendored_build)
add_dependencies(openssl_crypto openssl_vendored_build)

message(STATUS "vendored OpenSSL: ${_ossl_src} (3.5.8, ${_ossl_conf_target}, no-asm/no-tests/no-docs)")
