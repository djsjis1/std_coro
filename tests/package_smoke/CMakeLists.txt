cmake_minimum_required(VERSION 3.20)
project(coro_package_smoke LANGUAGES CXX)

# 要验证的安装组件。默认 coro (聚合入口); 传 http_client 等可单独验证"组件粒度安装"。
# 消费者只链接点名的那个组件 —— 这正是本测试要证明的事。
set(CORO_SMOKE_COMPONENT "coro" CACHE STRING "Installed component to link (coro, http_client, ...)")
# 期望的 TLS 能力 (0/1)。由调用方传入, 消费者不得自己伪造库能力宏。
set(CORO_SMOKE_EXPECT_TLS "" CACHE STRING "Expected HttpClient::supports_tls() (empty = skip)")

find_package(coro CONFIG REQUIRED COMPONENTS ${CORO_SMOKE_COMPONENT})

add_executable(coro_package_smoke main.cpp)
target_link_libraries(coro_package_smoke PRIVATE coro::${CORO_SMOKE_COMPONENT})
if(CORO_SMOKE_COMPONENT STREQUAL "http_client")
    target_compile_definitions(coro_package_smoke PRIVATE CORO_SMOKE_HTTP_CLIENT=1)
endif()
if(NOT CORO_SMOKE_EXPECT_TLS STREQUAL "")
    target_compile_definitions(coro_package_smoke PRIVATE CORO_SMOKE_EXPECT_TLS=${CORO_SMOKE_EXPECT_TLS})
endif()
