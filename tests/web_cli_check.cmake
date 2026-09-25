# 检查真实可执行程序的退出码, 不把任意非零退出当作参数校验成功。
execute_process(
    COMMAND "${PROGRAM}" ${ARGS}
    RESULT_VARIABLE result
    OUTPUT_VARIABLE output
    ERROR_VARIABLE error
    TIMEOUT 10)
if(NOT "${result}" STREQUAL "${EXPECTED}")
    message(FATAL_ERROR "Expected exit ${EXPECTED}, got ${result}\n${output}\n${error}")
endif()
if(NOT "${error}" MATCHES "usage:")
    message(FATAL_ERROR "Missing usage diagnostic: ${output}\n${error}")
endif()
