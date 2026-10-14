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
add_subdirectory(metrics)
add_subdirectory(memory_pool)
add_subdirectory(garbage_collector)
add_subdirectory(compressor)
add_subdirectory(encoding)
add_subdirectory(expression)
add_subdirectory(connector)
add_subdirectory(formatter)
add_subdirectory(generator)
add_subdirectory(reader)

# Create unified components library
add_library(actions_components STATIC
  $<TARGET_OBJECTS:components_metrics>
  $<TARGET_OBJECTS:components_memory_pool>
  $<TARGET_OBJECTS:components_gc>
  $<TARGET_OBJECTS:components_expression>
  $<TARGET_OBJECTS:components_connector>
  $<TARGET_OBJECTS:components_formatter>
  $<TARGET_OBJECTS:components_generator>
  $<TARGET_OBJECTS:components_reader>
)

# Set include directories
target_include_directories(actions_components
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/metrics/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/memory_pool/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/garbage_collector/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/compressor/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/encoding/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/expression/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/connector/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/formatter/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/generator/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/reader/inc
)

# Link submodules
target_link_libraries(actions_components
  PUBLIC
    components_metrics
    components_memory_pool
    components_gc
    components_compressor
    components_encoding
    components_expression
    components_connector
    components_formatter
    components_generator
    components_reader
)