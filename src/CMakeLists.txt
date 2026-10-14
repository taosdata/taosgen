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

# Include directories
include_directories(
  ${CMAKE_SOURCE_DIR}/inc
)

# Add subdirectories (in dependency order)
add_subdirectory(utils)
add_subdirectory(actions)
add_subdirectory(parameter)
add_subdirectory(workflow)
add_subdirectory(engine)
add_subdirectory(plugins)

# Main executable
add_executable(taosgen benchmark.cpp)

# Set include directories
target_include_directories(taosgen
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/parameter/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/engine/inc
)

# Link all dependent libraries
target_link_libraries(taosgen
  PRIVATE
    actions
    plugins
    engine
    workflow
    parameter
    utils
    yaml-cpp::yaml-cpp
    luajit::luajit
    # TAOS::TAOS
    Threads::Threads
)

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  target_link_libraries(taosgen PRIVATE -static-libstdc++ -static-libgcc ${ALLOCATOR_LIB})
elseif(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
  target_link_libraries(taosgen PRIVATE ${ALLOCATOR_LIB})
elseif(CMAKE_SYSTEM_NAME STREQUAL "Windows")
  target_link_libraries(taosgen PRIVATE ${ALLOCATOR_LIB} psapi)
endif()

# Install target
install(TARGETS taosgen
  RUNTIME DESTINATION bin
)

if(TSGEN_LINUX OR TSGEN_DARWIN)
  set_target_properties(taosgen PROPERTIES
    INSTALL_RPATH "/usr/local/lib"
  )
endif()