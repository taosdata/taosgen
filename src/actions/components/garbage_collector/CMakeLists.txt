# 创建库目标
add_library(components_gc STATIC 
  src/GarbageCollector.cpp
)

# 设置包含目录
target_include_directories(components_gc
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
)

# 链接依赖库
target_link_libraries(components_gc
  PUBLIC
)

# 启用测试
if(CMAKE_ENABLE_TEST)
  add_subdirectory(test)
endif()
