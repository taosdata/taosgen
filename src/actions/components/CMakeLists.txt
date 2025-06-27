# 添加子组件目录
add_subdirectory(connector)
# add_subdirectory(formatter)
# add_subdirectory(generator)
# add_subdirectory(reader)

# 创建统一的组件库
add_library(actions_components STATIC
  $<TARGET_OBJECTS:components_connector>
  # $<TARGET_OBJECTS:components_formatter>
  # $<TARGET_OBJECTS:components_generator>
  # $<TARGET_OBJECTS:components_reader>
)

# 设置包含目录
target_include_directories(actions_components
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/connector/inc
    # ${CMAKE_CURRENT_SOURCE_DIR}/formatter/inc
    # ${CMAKE_CURRENT_SOURCE_DIR}/generator/inc
    # ${CMAKE_CURRENT_SOURCE_DIR}/reader/inc
)

# 链接子模块
target_link_libraries(actions_components
  PUBLIC
    components_connector
    # components_formatter
    # components_generator
    # components_reader
)