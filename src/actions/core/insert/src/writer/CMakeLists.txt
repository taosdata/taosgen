# 创建库目标
add_library(insert_writer STATIC 
  src/TimeIntervalStrategy.cpp
  src/TDengineWriter.cpp
)

# 设置包含目录
target_include_directories(insert_writer
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../../../utils/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../../config/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../../components/metrics/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../../components/connector/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../../components/formatter/inc
)

# 链接依赖库
target_link_libraries(insert_writer
  PUBLIC
    actions_config
    components_metrics
    components_connector
    utils
    ${TAOS_LIBRARIES}
)

# 启用测试
if(CMAKE_ENABLE_TEST)
  add_subdirectory(test)
endif()
