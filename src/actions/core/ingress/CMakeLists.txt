# Copyright (c) 2026 TAOS Data, Inc.
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
add_library(core_ingress STATIC
  src/IngestionGrpcService.cpp
  src/IngestionMemoryBlockBuilder.cpp
  src/IngestionRuntime.cpp
  src/IngestionServer.cpp
  src/IngestionValidator.cpp
)

target_include_directories(core_ingress
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../utils/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../config/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../components/memory_pool/inc
)

target_link_libraries(core_ingress
  PUBLIC
    actions_config
    components_memory_pool
    taosgen_ingress_proto
    utils
)

if(TSGEN_ENABLE_TEST)
  add_subdirectory(test)
endif()
