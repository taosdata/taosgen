cmake_minimum_required(VERSION 3.10)
project(taosbenchmark LANGUAGES CXX)

# Options
option(CMAKE_ENABLE_TEST "Enable testing" ON)
option(CMAKE_ENABLE_COVERAGE "Enable code coverage" OFF)
option(CMAKE_ENABLE_SANITIZER "Enable sanitizers" OFF)

# Global settings
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_FIND_LIBRARY_SUFFIXES .a .so)

# Build type settings
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Release" CACHE STRING "Build type" FORCE)
endif()
string(TOUPPER ${CMAKE_BUILD_TYPE} BUILD_TYPE_UPPER)

# Global compile options
add_compile_options(
  -Wall
  -Wextra
  -Werror
)

# Optimization options by build type
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
  add_compile_options(-O0 -g)
else()
  add_compile_options(
    -O3
    -flto=auto
  )
endif()

# Add cmake modules path
list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake")

# Load utilities
include(Utils)
setup_colored_output()

# Find dependencies
find_package(Threads REQUIRED)
find_package(YAML_CPP REQUIRED)
find_package(LUAJIT REQUIRED)
find_package(TAOS REQUIRED)

# Test support
enable_testing()

# Add main program and test directories
add_subdirectory(src)
# add_subdirectory(test)