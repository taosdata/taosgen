cmake_minimum_required(VERSION 3.10)
project(taosbenchmark LANGUAGES CXX)

# 全局设置
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# 编译类型设置
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Release" CACHE STRING "Build type" FORCE)
endif()
string(TOUPPER ${CMAKE_BUILD_TYPE} BUILD_TYPE_UPPER)

# 全局编译选项
add_compile_options(
  -Wall
  -Wextra
  -Werror
)

# 根据构建类型设置优化选项
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
  add_compile_options(-O0 -g)
else()
  add_compile_options(-O3)
endif()

# Add cmake modules path
list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake")

# Load utilities
include(Utils)
setup_colored_output()

# 查找依赖库
find_package(Threads REQUIRED)
find_package(YAML_CPP REQUIRED)
find_package(Taos REQUIRED)

# 添加主程序和测试目录
add_subdirectory(src)
# add_subdirectory(test)

# 测试支持
enable_testing()