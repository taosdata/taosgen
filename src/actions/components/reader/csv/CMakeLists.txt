# 创建库目标
add_library(reader_csv STATIC
  src/CSVUtils.cpp
  src/CSVReader.cpp
  src/TableNameCSV.cpp
  src/TagsCSV.cpp
  src/ColumnsCSV.cpp
)

# 设置包含目录
target_include_directories(reader_csv
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../../utils/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../config/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../generator/inc
)

# 链接依赖库
target_link_libraries(reader_csv
  PUBLIC
    actions_config
    components_generator
    utils
)

# 启用测试
if(CMAKE_ENABLE_TEST)
  add_subdirectory(test)
endif()
