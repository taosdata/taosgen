# 创建库目标
add_library(workflow STATIC
  src/Job.cpp
)

# 设置包含目录
target_include_directories(workflow
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../actions/config/inc
)

# 链接依赖库
target_link_libraries(workflow
  PUBLIC
    yaml-cpp
)

# 启用测试
# if(CMAKE_ENABLE_TEST)
#   add_subdirectory(test)
# endif()