# 创建库目标
add_library(core_create STATIC 
  src/CreateDatabaseAction.cpp
  src/CreateSuperTableAction.cpp
  src/CreateChildTableAction.cpp
)

# 设置包含目录
target_include_directories(core_create
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../utils/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../base
    ${CMAKE_CURRENT_SOURCE_DIR}/../../config/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../components/connector/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../components/formatter/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../components/generator/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../components/reader/csv/inc
)

# 链接依赖库
target_link_libraries(core_create
  PUBLIC
    actions_config
    components_connector
    components_formatter
    components_generator
    components_reader
    reader_csv
    utils
)

# 启用测试
if(CMAKE_ENABLE_TEST)
  add_subdirectory(test)
endif()
