# 创建库目标
add_library(components_formatter STATIC 
  src/FormatterFactory.cpp
)

# 设置包含目录
target_include_directories(components_formatter
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../config/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../utils/inc
)

# 链接依赖库
target_link_libraries(components_formatter
  PUBLIC
    actions_config
    utils
    ${TAOS_LIBRARIES}
)

# 启用测试
if(CMAKE_ENABLE_TEST)
  add_subdirectory(test)
endif()
