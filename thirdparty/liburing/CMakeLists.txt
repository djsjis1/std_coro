# ============================================================================
# liburing 2.9 上游源码 (本目录 src/) 的 CMake 包装层
#
# 上游只提供 autoconf 式构建 (configure + Makefile), 其中两个公共头
#   liburing/compat.h            —— 按内核头/glibc 能力生成兼容定义
#   liburing/io_uring_version.h  —— 版本宏
# 是 configure 的运行产物, 不在源码里。本文件用等价的内核头探测在构建目录
# 生成它们, 从而:
#   * src/ 保持上游原样, 不向源码树写入任何生成文件;
#   * 不需要 autoconf / 外部 make 步骤, 老内核头环境自动降级;
#   * 只在被启用时才编译 (根 CMake 以 CORO_ENABLE_URING 门控 add_subdirectory)。
#
# 上游许可: MIT (LICENSE) / dual-licensed 部分 (COPYING, COPYING.GPL)。
# 上游版本: tag liburing-2.9 (commit 31a41935866867cdfedb4ef5a57ce0d15b50e5b9),
#          仅收录 src/ 与许可/说明文件, 未收录 test/ examples/ man/ debian/。
# ============================================================================
cmake_minimum_required(VERSION 3.20)
project(liburing LANGUAGES C)

include(CheckCSourceCompiles)
set(CMAKE_REQUIRED_QUIET ON)
# 与上游 configure 的 CFLAGS 对齐: 缺少 _GNU_SOURCE 时 glibc 不暴露 statx 等接口,
# 探测结果会偏保守 (多生成兼容定义)。
set(CMAKE_REQUIRED_DEFINITIONS -D_GNU_SOURCE -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64)

# ---- 与上游 configure 相同的能力探测 (结果只影响生成的 compat.h) ----
check_c_source_compiles("
#include <linux/fs.h>
int main(void) { __kernel_rwf_t x; x = 0; return (int) x; }
" URING_HAVE_KERNEL_RWF_T)

check_c_source_compiles("
#include <linux/time.h>
#include <linux/time_types.h>
int main(void) { struct __kernel_timespec ts; ts.tv_sec = 0; ts.tv_nsec = 1; return 0; }
" URING_HAVE_KERNEL_TIMESPEC)

check_c_source_compiles("
#include <sys/types.h>
#include <fcntl.h>
#include <string.h>
#include <linux/openat2.h>
int main(void) { struct open_how how; how.flags = 0; how.mode = 0; how.resolve = 0; return 0; }
" URING_HAVE_OPEN_HOW)

check_c_source_compiles("
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
int main(void) { struct statx x; return memset(&x, 0, sizeof(x)) != NULL; }
" URING_HAVE_GLIBC_STATX)

check_c_source_compiles("
#include <linux/futex.h>
#include <unistd.h>
#include <string.h>
int main(void) { struct futex_waitv fw; memset(&fw, FUTEX_32, sizeof(fw)); return (int) sizeof(fw); }
" URING_HAVE_FUTEX_WAITV)

check_c_source_compiles("
#include <sys/wait.h>
int main(void) { idtype_t v; (void) v; return 0; }
" URING_HAVE_IDTYPE_T)

check_c_source_compiles("
#include <linux/blkdev.h>
int main(void) { return BLOCK_URING_CMD_DISCARD; }
" URING_HAVE_BLK_DISCARD_CMD)

set(CMAKE_REQUIRED_QUIET OFF)
set(CMAKE_REQUIRED_DEFINITIONS)

# ---- 生成 compat.h (内容与上游 configure 的输出逐项对应) ----
set(_compat "/* SPDX-License-Identifier: MIT */\n")
string(APPEND _compat "/* 由 CMake 生成, 等价于上游 configure 的探测结果 -- 请勿手工修改 */\n")
string(APPEND _compat "#ifndef LIBURING_COMPAT_H\n#define LIBURING_COMPAT_H\n\n")
if(NOT URING_HAVE_KERNEL_RWF_T)
    string(APPEND _compat "typedef int __kernel_rwf_t;\n\n")
endif()
if(URING_HAVE_KERNEL_TIMESPEC)
    string(APPEND _compat "#include <linux/time_types.h>\n")
    string(APPEND _compat "/* <linux/time_types.h> is included above and not needed again */\n")
    string(APPEND _compat "#define UAPI_LINUX_IO_URING_H_SKIP_LINUX_TIME_TYPES_H 1\n\n")
else()
    string(APPEND _compat "#include <stdint.h>\n\n")
    string(APPEND _compat "struct __kernel_timespec {\n\tint64_t\t\ttv_sec;\n\tlong long\ttv_nsec;\n};\n\n")
    string(APPEND _compat "#define UAPI_LINUX_IO_URING_H_SKIP_LINUX_TIME_TYPES_H 1\n\n")
endif()
if(URING_HAVE_OPEN_HOW)
    string(APPEND _compat "#include <linux/openat2.h>\n\n")
else()
    string(APPEND _compat "#include <inttypes.h>\n\n")
    string(APPEND _compat "struct open_how {\n\tuint64_t\tflags;\n\tuint64_t\tmode;\n\tuint64_t\tresolve;\n};\n\n")
endif()
if(NOT URING_HAVE_GLIBC_STATX)
    # glibc 未提供 statx 时, 依赖内核头里的定义, 需显式包含 sys/stat.h
    string(APPEND _compat "#include <sys/stat.h>\n\n")
endif()
if(NOT URING_HAVE_FUTEX_WAITV)
    string(APPEND _compat "#include <inttypes.h>\n\n")
    string(APPEND _compat "#define FUTEX_32\t2\n#define FUTEX_WAITV_MAX\t128\n\n")
    string(APPEND _compat "struct futex_waitv {\n\tuint64_t\tval;\n\tuint64_t\tuaddr;\n\tuint32_t\tflags;\n\tuint32_t\t__reserved;\n};\n\n")
endif()
if(NOT URING_HAVE_IDTYPE_T)
    string(APPEND _compat "typedef enum {\n\tP_ALL,\n\tP_PID,\n\tP_PGID\n} idtype_t;\n\n")
endif()
if(URING_HAVE_BLK_DISCARD_CMD)
    string(APPEND _compat "#include <linux/blkdev.h>\n\n")
else()
    string(APPEND _compat "#include <linux/ioctl.h>\n\n")
    string(APPEND _compat "#ifndef BLOCK_URING_CMD_DISCARD\n#define BLOCK_URING_CMD_DISCARD\t\t_IO(0x12, 0)\n#endif\n\n")
endif()
string(APPEND _compat "#endif\n")

# ---- 生成 io_uring_version.h (版本号需与上游发布 tag 一致) ----
set(_LIBURING_UPSTREAM_VERSION 2.9)
string(REPLACE "." ";" _ver_parts "${_LIBURING_UPSTREAM_VERSION}")
list(GET _ver_parts 0 _vmajor)
list(GET _ver_parts 1 _vminor)
set(_verh "/* SPDX-License-Identifier: MIT */\n")
string(APPEND _verh "#ifndef LIBURING_VERSION_H\n#define LIBURING_VERSION_H\n\n")
string(APPEND _verh "#define IO_URING_VERSION_MAJOR ${_vmajor}\n")
string(APPEND _verh "#define IO_URING_VERSION_MINOR ${_vminor}\n\n")
string(APPEND _verh "#endif\n")

# 生成到构建目录, 源码树不落任何产物
file(GENERATE OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/include/liburing/compat.h" CONTENT "${_compat}")
file(GENERATE OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/include/liburing/io_uring_version.h" CONTENT "${_verh}")

# ---- 静态库: 与上游 liburing.a 同一组源文件 (不含 nolibc / ffi / sanitize) ----
add_library(uring STATIC
    src/setup.c
    src/queue.c
    src/register.c
    src/syscall.c
    src/version.c
)
add_library(liburing::uring ALIAS uring)

set_target_properties(uring PROPERTIES
    OUTPUT_NAME "uring"
    POSITION_INDEPENDENT_CODE ON
    C_VISIBILITY_PRESET default
)

target_compile_definitions(uring PRIVATE
    _GNU_SOURCE
    LIBURING_INTERNAL
    _LARGEFILE_SOURCE
    _FILE_OFFSET_BITS=64
)

# src/ 供内部头 (lib.h / arch/<arch>/*.h), src/include/ 供公共头,
# 构建目录 include/ 供上面生成的 compat.h 与 io_uring_version.h
target_include_directories(uring
    PUBLIC
        "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src/include>"
        "$<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/include>"
        "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>"
    PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}/src"
)

if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(uring PRIVATE -fno-stack-protector -Wno-unused-parameter)
endif()

# ---- 安装: 头文件 (含生成头) + 静态库 ----
# 复用根项目的 coroTargets 导出集, 安装后目标名为 coro::uring,
# 随 find_package(coro) 一并带入, 消费方无需再找系统 liburing。
include(GNUInstallDirs)
install(TARGETS uring
    EXPORT coroTargets
    ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
)
install(DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/src/include/"
    DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
    FILES_MATCHING PATTERN "*.h"
)
install(DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/include/liburing"
    DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
    FILES_MATCHING PATTERN "compat.h" PATTERN "io_uring_version.h"
)
