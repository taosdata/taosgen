# 添加子目录
add_subdirectory(csv)
# add_subdirectory(kafka)

# 创建库目标
add_library(components_reader STATIC 
  $<TARGET_OBJECTS:reader_csv>
)

# 设置包含目录
target_include_directories(components_reader
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/csv/inc
)

# 链接子模块
target_link_libraries(components_reader
  PUBLIC
    reader_csv
)