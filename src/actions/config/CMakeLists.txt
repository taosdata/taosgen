# 创建库目标
add_library(actions_config STATIC 
  src/ConnectionInfo.cpp
  src/ColumnType.cpp
  src/ColumnConfig.cpp
)

# 设置包含目录
target_include_directories(actions_config
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../utils/inc
)

# 链接依赖库
target_link_libraries(actions_config
  PUBLIC 
    utils
)

# 启用测试
if(CMAKE_TESTING_ENABLED)
  add_subdirectory(test)
endif()
