# 创建库目标
add_library(components_generator STATIC 
  src/TableNameGenerator.cpp
  src/TimestampGenerator.cpp
  src/ColumnGeneratorFactory.cpp
  src/RandomColumnGenerator.cpp
  src/OrderColumnGenerator.cpp
  src/FunctionColumnGenerator.cpp
  src/RowGenerator.cpp
)

# 设置包含目录
target_include_directories(components_generator
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../config/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../utils/inc
)

# 链接依赖库
target_link_libraries(components_generator
  PUBLIC
    actions_config
    utils
)

# 启用测试
if(CMAKE_ENABLE_TEST)
  add_subdirectory(test)
endif()
