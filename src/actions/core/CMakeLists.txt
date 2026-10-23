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

# Add subcomponent directories
add_subdirectory(create)
add_subdirectory(insert)
add_subdirectory(checkpoint)
add_subdirectory(query)
add_subdirectory(subscribe)
add_subdirectory(ingress)

# Create unified component library
add_library(actions_core STATIC
  $<TARGET_OBJECTS:core_create>
  $<TARGET_OBJECTS:core_insert>
  $<TARGET_OBJECTS:core_checkpoint>
  $<TARGET_OBJECTS:core_query>
  $<TARGET_OBJECTS:core_subscribe>
  $<TARGET_OBJECTS:core_ingress>
)

# Set include directories
target_include_directories(actions_core
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/create/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/insert/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/query/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/subscribe/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/ingress/inc
)

# Link submodules
target_link_libraries(actions_core
  PUBLIC
    core_create
    core_insert
    core_checkpoint
    core_query
    core_subscribe
    core_ingress
)
