# 创建库目标
add_library(parameter STATIC
  src/ParameterContext.cpp
)

# 设置包含目录
target_include_directories(parameter
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../workflow/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../actions/config/inc
)

# 链接依赖库
target_link_libraries(parameter
  PUBLIC
    actions_config
    ${YAML_CPP_LIBRARIES}
)

# 启用测试
if(CMAKE_ENABLE_TEST)
  add_subdirectory(test)
endif()