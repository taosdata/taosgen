cmake_minimum_required(VERSION 3.19)
project(taosbenchmark LANGUAGES CXX)

# Conan 2.x integration
include(${CMAKE_BINARY_DIR}/conan/conan_toolchain.cmake)
list(APPEND CMAKE_PREFIX_PATH ${CMAKE_BINARY_DIR}/conan)
set(CMAKE_FIND_PACKAGE_PREFER_CONFIG ON)

# Options
option(CMAKE_ENABLE_TEST "Enable testing" ON)
option(CMAKE_ENABLE_COVERAGE "Enable code coverage" OFF)
option(CMAKE_ENABLE_SANITIZER "Enable sanitizers" OFF)

# Global settings
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_FIND_LIBRARY_SUFFIXES .a .so)

# System type settings
message(STATUS "CMAKE_SYSTEM_NAME: ${CMAKE_SYSTEM_NAME}")
if(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
  set(TSGEN_LINUX TRUE)
  set(TSGEN_BUILD_TARGET_OSTYPE "Linux")
elseif(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  set(TSGEN_DARWIN TRUE)
  set(TSGEN_BUILD_TARGET_OSTYPE "Darwin")
elseif(${CMAKE_SYSTEM_NAME} MATCHES "Windows")
  set(TSGEN_WINDOWS TRUE)
  set(TSGEN_BUILD_TARGET_OSTYPE "Windows")
else()
  message(FATAL_ERROR "Not ported on `${CMAKE_SYSTEM_NAME}`")
  set(TSGEN_BUILD_TARGET_OSTYPE "Unknow OS")
endif()

# CPU architecture settings
message(STATUS "CMAKE_SYSTEM_PROCESSOR: ${CMAKE_SYSTEM_PROCESSOR}")
string(TOLOWER "${CMAKE_SYSTEM_PROCESSOR}" TSGEN_BUILD_TARGET_CPUTYPE)
if(TSGEN_BUILD_TARGET_CPUTYPE STREQUAL "aarch64")
  set(TSGEN_BUILD_TARGET_CPUTYPE "arm64")
endif()
message(STATUS "CPU: ${TSGEN_BUILD_TARGET_CPUTYPE}")

# Build type settings
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Release" CACHE STRING "Build type" FORCE)
endif()
string(TOUPPER ${CMAKE_BUILD_TYPE} BUILD_TYPE_UPPER)

string(TIMESTAMP TSGEN_BUILD_DATE "%Y-%m-%d %H:%M:%S %z")
message(STATUS "Sys Info: ${TSGEN_BUILD_TARGET_OSTYPE}-${TSGEN_BUILD_TARGET_CPUTYPE} ${TSGEN_BUILD_DATE}")

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
  add_compile_options(-O3)
endif()

# Add cmake modules path
list(APPEND CMAKE_MODULE_PATH ${CMAKE_BINARY_DIR})
list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake")

# Load utilities
include(Utils)
setup_colored_output()
check_requirements()

# Find dependencies
find_package(ZLIB REQUIRED)
find_package(LZ4 REQUIRED)
find_package(zstd REQUIRED)
find_package(Iconv REQUIRED)
find_package(yaml-cpp REQUIRED)
find_package(luajit REQUIRED)
find_package(PahoMqttCpp REQUIRED)
find_package(nlohmann_json REQUIRED)
find_package(TAOS REQUIRED)
find_package(Threads REQUIRED)
find_package(Iconv REQUIRED)

if(HAVE_GIT)
  execute_process(COMMAND git log -1 --format=%H WORKING_DIRECTORY ${PROJECT_SOURCE_DIR} OUTPUT_VARIABLE GIT_COMMITID)
  # message(STATUS "git log result:${GIT_COMMITID}")
  if(GIT_COMMITID)
    string (REGEX REPLACE "[\n\t\r]" "" GIT_COMMITID ${GIT_COMMITID})
    SET(TSGEN_BUILD_GIT ${GIT_COMMITID})
  else()
    message(STATUS "not a git repository")
    SET(TSGEN_BUILD_GIT "no git commit id")
  endif()
else()
  message(STATUS "no git found")
  SET(TSGEN_BUILD_GIT  "no git commit id")
endif()
message(STATUS "Git Info: ${TSGEN_BUILD_GIT}")

# Test support
if(CMAKE_ENABLE_TEST)
  enable_testing()
endif()

# Add main program and test directories
add_subdirectory(src)
# if(CMAKE_ENABLE_TEST)
#   add_subdirectory(test)
# endif()