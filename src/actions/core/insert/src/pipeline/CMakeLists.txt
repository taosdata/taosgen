# 创建库目标
add_library(insert_pipeline STATIC 
  src/DataPipeline.cpp
)

# 设置包含目录
target_include_directories(insert_pipeline
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../../config/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../../../utils/inc
)

# 链接依赖库
target_link_libraries(insert_pipeline
  PUBLIC
    actions_config
    utils
)

# 启用测试
if(CMAKE_ENABLE_TEST)
  add_subdirectory(test)
endif()
