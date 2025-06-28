# 包含目录设置
include_directories(
  ${CMAKE_SOURCE_DIR}/inc
  ${YAML_CPP_INCLUDE_DIRS}
  ${TAOS_INCLUDE_DIRS}
)

# 添加子目录（按依赖顺序）
add_subdirectory(utils)
add_subdirectory(actions)
add_subdirectory(parameter)
add_subdirectory(workflow)
add_subdirectory(engine)

# 主可执行文件
add_executable(taosbench benchmark.cpp)

# 设置包含目录
target_include_directories(parameter
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/parameter/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/engine/inc
)

# 链接所有依赖库
target_link_libraries(taosbench
  PRIVATE
    actions
    engine
    workflow
    parameter
    utils
    ${YAML_CPP_LIBRARIES}
    ${TAOS_LIBRARIES}
    Threads::Threads
)

# 安装目标
install(TARGETS taosbench 
  RUNTIME DESTINATION bin
)