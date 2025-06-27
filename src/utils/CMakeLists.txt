# 创建库目标
add_library(utils STATIC
  src/StringUtils.cpp
  src/TimestampUtils.cpp
)

# 设置包含目录
target_include_directories(utils
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
)

# 链接依赖库
target_link_libraries(utils
  PUBLIC
    Threads::Threads
)

# 启用测试
# if(CMAKE_TESTING_ENABLED)
#   add_subdirectory(test)
# endif()