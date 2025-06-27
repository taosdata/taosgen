# 添加接口库
add_library(actions_base INTERFACE)
target_include_directories(actions_base 
  INTERFACE 
    ${CMAKE_CURRENT_SOURCE_DIR}/base
)

# 添加配置模块
add_subdirectory(config)

# 添加基础组件目录
add_subdirectory(components)

# 添加核心功能模块
add_subdirectory(core)

# 创建统一的 actions 库
add_library(actions STATIC
  $<TARGET_OBJECTS:actions_config>
  $<TARGET_OBJECTS:actions_components>
  $<TARGET_OBJECTS:actions_core>
)

# 设置 actions 库的包含目录
target_include_directories(actions
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/base
    ${CMAKE_CURRENT_SOURCE_DIR}/config/inc
)

# 链接子模块
target_link_libraries(actions
  PUBLIC
    actions_base
    actions_config
    actions_components
    actions_core
)
