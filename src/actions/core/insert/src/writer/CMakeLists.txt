cmake_minimum_required(VERSION 3.10)

project(insert_writer LANGUAGES CXX)

# 设置 C++ 标准
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 启用测试支持
enable_testing()

# 添加调试符号信息
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g")

# 添加第三方库

include_directories(C:/TDengine/include)
include_directories(/usr/include)

# 添加主库
add_library(insert_writer STATIC 
    src/TimeIntervalStrategy.cpp
    src/TDengineWriter.cpp
)

# 包含头文件目录
target_include_directories(insert_writer PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../../../utils/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../../config/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../../components/connector/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../../components/formatter/inc
)

# 链接依赖库
link_directories(${CMAKE_CURRENT_SOURCE_DIR}/../../../../../utils/build)
link_directories(${CMAKE_CURRENT_SOURCE_DIR}/../../../../config/build)
link_directories(${CMAKE_CURRENT_SOURCE_DIR}/../../../../components/connector/build)
target_link_libraries(insert_writer PUBLIC
    actions_config
    components_connector
    utils
    taos
)

# 添加测试
# Test TimeIntervalStrategy
add_executable(TestTimeIntervalStrategy
    test/TestTimeIntervalStrategy.cpp
)
target_link_libraries(TestTimeIntervalStrategy PRIVATE 
    insert_writer
)
add_test(NAME TestTimeIntervalStrategy COMMAND TestTimeIntervalStrategy)

# Test TDengineWriter
add_executable(TestTDengineWriter
    test/TestTDengineWriter.cpp
)
target_link_libraries(TestTDengineWriter PRIVATE 
    insert_writer
)
add_test(NAME TestTDengineWriter COMMAND TestTDengineWriter)
