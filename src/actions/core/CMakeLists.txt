# 添加子组件目录
add_subdirectory(create)
add_subdirectory(insert)
# add_subdirectory(query)
# add_subdirectory(subscribe)

# 创建统一的组件库
add_library(actions_core STATIC
  $<TARGET_OBJECTS:core_create>
  $<TARGET_OBJECTS:core_insert>
#   $<TARGET_OBJECTS:core_query>
#   $<TARGET_OBJECTS:core_subscribe>
)

# 设置包含目录
target_include_directories(actions_core
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/create/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/insert/inc
    # ${CMAKE_CURRENT_SOURCE_DIR}/query/inc
    # ${CMAKE_CURRENT_SOURCE_DIR}/subscribe/inc
)

# 链接子模块
target_link_libraries(actions_core
  PUBLIC
    core_create
    core_insert
    # core_query
    # core_subscribe
)