# SQLite3 CMake 配置文件
# 使用方式：在主 CMakeLists.txt 中添加 add_subdirectory(sqlite)
# 然后链接 target_link_libraries(your_target sqlite3)

cmake_minimum_required(VERSION 3.10)

# 允许独立构建，也允许被 add_subdirectory 引入
if(NOT DEFINED PROJECT_NAME)
    project(sqlite3 C)
endif()

# SQLite3 静态库
add_library(sqlite3 STATIC sqlite3.c)

target_include_directories(sqlite3
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<INSTALL_INTERFACE:include>
)

# 平台相关链接库（Linux 需要 pthread/dl/m，Windows 不需要）
if(UNIX AND NOT APPLE)
    target_link_libraries(sqlite3 PUBLIC pthread dl m)
endif()

# 编译选项（MSVC 用 /O2，GCC/Clang 用 -O2）
target_compile_definitions(sqlite3 PRIVATE
    SQLITE_THREADSAFE=1
    SQLITE_ENABLE_FTS5
    SQLITE_ENABLE_JSON1
    SQLITE_ENABLE_RTREE
)

# SQLite3 Shell 可执行文件（默认关闭，需要时手动开启）
option(SQLITE_BUILD_SHELL "Build sqlite3 shell executable" OFF)
if(SQLITE_BUILD_SHELL)
    add_executable(sqlite3_shell shell.c)
    target_link_libraries(sqlite3_shell sqlite3)
    set_target_properties(sqlite3_shell PROPERTIES OUTPUT_NAME sqlite3)
endif()
