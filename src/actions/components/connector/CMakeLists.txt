# 创建库目标
add_library(components_connector STATIC 
  src/DatabaseConnector.cpp
  src/NativeConnector.cpp
  src/WebsocketConnector.cpp
  src/RestfulConnector.cpp
)

# 设置包含目录
target_include_directories(components_connector
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../utils/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../config/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../formatter/inc
)

# 链接依赖库
target_link_libraries(components_connector
  PUBLIC 
    actions_config
    ${TAOS_LIBRARIES}
)

# 启用测试
if(CMAKE_ENABLE_TEST)
  add_subdirectory(test)
endif()
