cmake_minimum_required(VERSION 3.20)
project(std_http VERSION 1.0.0 LANGUAGES C CXX)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

set(_http_examples_default OFF)
if(CMAKE_SOURCE_DIR STREQUAL PROJECT_SOURCE_DIR)
    set(_http_examples_default ON)
endif()
option(STD_HTTP_BUILD_EXAMPLES "Build HTTP example programs" ${_http_examples_default})

# Compile the vendored generated C sources directly. The upstream CMake project
# changes global shared/static options and writes pkg-config files into its source
# tree; neither behavior belongs in an embedded dependency.
add_library(coro_llhttp STATIC
    llhttp-release-v9.3.0/src/llhttp.c
    llhttp-release-v9.3.0/src/http.c
    llhttp-release-v9.3.0/src/api.c)
if(NOT TARGET llhttp::llhttp)
    add_library(llhttp::llhttp ALIAS coro_llhttp)
endif()
set_target_properties(coro_llhttp PROPERTIES
    EXPORT_NAME llhttp POSITION_INDEPENDENT_CODE ON)
target_compile_features(coro_llhttp PRIVATE c_std_99)
target_include_directories(coro_llhttp PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/llhttp-release-v9.3.0/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/coro/http>)

add_library(http STATIC src/http_parse.cpp src/http_protocol.cpp)
add_library(http::http ALIAS http)
add_library(coro::http_protocol ALIAS http)
set_target_properties(http PROPERTIES
    EXPORT_NAME http_protocol OUTPUT_NAME stdhttp POSITION_INDEPENDENT_CODE ON)
target_compile_features(http PUBLIC cxx_std_17)
target_include_directories(http PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/coro/http>)
target_link_libraries(http PUBLIC coro_llhttp)
if(MSVC)
    target_compile_options(http PRIVATE /utf-8)
endif()

if(CORO_HTTP_EXPORT_WITH_CORO)
    set(_http_export coroTargets)
else()
    set(_http_export httpTargets)
endif()
install(TARGETS http coro_llhttp EXPORT ${_http_export}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(FILES src/http_parse.h src/http_protocol.h
    llhttp-release-v9.3.0/include/llhttp.h
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/coro/http)

if(NOT CORO_HTTP_EXPORT_WITH_CORO)
    install(EXPORT httpTargets FILE httpTargets.cmake NAMESPACE http::
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/http)
    configure_package_config_file(cmake/httpConfig.cmake.in
        ${CMAKE_CURRENT_BINARY_DIR}/httpConfig.cmake
        INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/http)
    write_basic_package_version_file(${CMAKE_CURRENT_BINARY_DIR}/httpConfigVersion.cmake
        VERSION ${PROJECT_VERSION} COMPATIBILITY SameMajorVersion)
    install(FILES ${CMAKE_CURRENT_BINARY_DIR}/httpConfig.cmake
        ${CMAKE_CURRENT_BINARY_DIR}/httpConfigVersion.cmake
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/http)
endif()

if(STD_HTTP_BUILD_EXAMPLES)
    add_executable(http_example examples/main.cpp)
    target_link_libraries(http_example PRIVATE http::http)
endif()
