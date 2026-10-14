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
add_library(utils STATIC
  src/LogUtils.cpp
  src/StringUtils.cpp
  src/TimestampUtils.cpp
  src/ProcessUtils.cpp
  src/Barrier.cpp
  src/Latch.cpp
  src/SignalManager.cpp
  src/ScopedEnvVar.cpp
)

# Set include directories
target_include_directories(utils
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
)

# Find cctz package
find_package(cctz REQUIRED)

# GCC < 8 ships <filesystem> under <experimental/filesystem> and requires
# explicit linking with -lstdc++fs.
set(_fs_lib "")
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS 8)
  set(_fs_lib stdc++fs)
endif()

# Link dependent libraries
target_link_libraries(utils
  PUBLIC
    spdlog::spdlog
    Threads::Threads
    Iconv::Iconv
    fmt::fmt
    cctz::cctz
    ${_fs_lib}
)

# Enable tests
if(TSGEN_ENABLE_TEST)
  add_subdirectory(test)
endif()