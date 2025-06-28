# 创建库目标
add_library(engine STATIC 
  src/JobDAG.cpp
  src/ThreadSafeQueue.cpp
  src/StepExecutionStrategy.cpp
  src/JobScheduler.cpp
)

# 设置包含目录
target_include_directories(engine
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../workflow/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../actions/config/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../actions/base
    ${CMAKE_CURRENT_SOURCE_DIR}/../actions/core/create/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../actions/core/insert/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../actions/core/query/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../actions/core/subscribe/inc
)

# 链接依赖库
target_link_libraries(engine
  PUBLIC
    parameter
    actions
    workflow
)

# 启用测试
if(CMAKE_ENABLE_TEST)
  add_subdirectory(test)
endif()

