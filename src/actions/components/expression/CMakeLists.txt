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
add_library(components_expression STATIC
  src/MathFunctions.cpp
  src/NetworkFunctions.cpp
  src/ExpressionEngine.cpp
)

# Set include directories
target_include_directories(components_expression
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../config/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../utils/inc
)

# Link dependencies
target_link_libraries(components_expression
  PUBLIC
    actions_config
    luajit::luajit
    Threads::Threads
)

if(TSGEN_LINUX)
  target_link_libraries(components_expression PUBLIC m dl)
endif()

# Set compilation options
if(TSGEN_WINDOWS)
  # MSVC does not support -fno-strict-aliasing
else()
  target_compile_options(components_expression PRIVATE
      -fno-strict-aliasing
  )
endif()

# Set link options
if(TSGEN_LINUX)
  set_target_properties(components_expression PROPERTIES
      LINK_FLAGS "-Wl,--export-dynamic"
  )
endif()

# Enable tests
if(TSGEN_ENABLE_TEST)
  add_subdirectory(test)
endif()
