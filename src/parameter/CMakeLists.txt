# Copyright (c) 2025 TAOS Data, Inc.
#
# SPDX-License-Identifier: MIT
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

# Create library target
add_library(parameter STATIC
  src/ParameterContext.cpp
  src/StepParserRegistry.cpp
)

target_compile_definitions(parameter PRIVATE
  TSGEN_BUILD_TARGET_OSTYPE="${TSGEN_BUILD_TARGET_OSTYPE}"
  TSGEN_BUILD_TARGET_CPUTYPE="${TSGEN_BUILD_TARGET_CPUTYPE}"
  TSGEN_BUILD_DATE="${TSGEN_BUILD_DATE}"
  TSGEN_BUILD_GIT="${TSGEN_BUILD_GIT}"
)

# Set include directories
target_include_directories(parameter
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../workflow/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../actions/config/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../actions/core/checkpoint/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../actions/base
    ${PROJECT_SOURCE_DIR}/src
  PRIVATE
    ${PROJECT_SOURCE_DIR}/src/utils/inc
)

# Link dependent libraries
target_link_libraries(parameter
  PUBLIC
    actions_config
    core_checkpoint
    yaml-cpp::yaml-cpp
)

# Enable tests
if(TSGEN_ENABLE_TEST)
  add_subdirectory(test)
endif()