cmake_minimum_required(VERSION 3.20)
project(coro_package_smoke LANGUAGES CXX)

find_package(coro CONFIG REQUIRED)

add_executable(coro_package_smoke main.cpp)
target_link_libraries(coro_package_smoke PRIVATE coro::coro)
