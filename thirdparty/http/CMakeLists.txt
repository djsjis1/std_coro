cmake_minimum_required(VERSION 3.14)

project(std_http VERSION 1.0.0 LANGUAGES C CXX)

include(GNUInstallDirs)

option(STD_HTTP_BUILD_EXAMPLES "Build example programs" ON)

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# MSVC: 源文件为 UTF-8 无 BOM(含中文注释),指定按 UTF-8 解析
if(MSVC)
  add_compile_options(/utf-8)
endif()

# llhttp 官方源码作为子项目引入,默认只构建静态库
if(NOT DEFINED BUILD_SHARED_LIBS)
  set(BUILD_SHARED_LIBS OFF)
endif()
if(NOT DEFINED BUILD_STATIC_LIBS)
  set(BUILD_STATIC_LIBS ON)
endif()
add_subdirectory(llhttp-release-v9.3.0)

# ---------------- 封装库 ----------------
add_library(http
  src/http_parse.cpp
  src/http_protocol.cpp
)
add_library(http::http ALIAS http)

target_include_directories(http PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_link_libraries(http PUBLIC llhttp::llhttp)

set_target_properties(http PROPERTIES
  OUTPUT_NAME stdhttp
  VERSION ${PROJECT_VERSION}
)

# ---------------- install ----------------
install(TARGETS http
  EXPORT httpTargets
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
)

install(FILES
  src/http_parse.h
  src/http_protocol.h
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

install(EXPORT httpTargets
  FILE http-config.cmake
  NAMESPACE http::
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/http
)

# ---------------- example ----------------
if(STD_HTTP_BUILD_EXAMPLES)
  add_executable(http_example examples/main.cpp)
  target_link_libraries(http_example PRIVATE http::http)
endif()
