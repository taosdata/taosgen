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

# Find dependent packages
find_package(CURL REQUIRED)

# Create library target
add_library(plugin_influxdb STATIC
  src/InfluxDBRegistrar.cpp
  src/InfluxDBInsertDataFormatter.cpp
  src/InfluxDBClient.cpp
  src/InfluxDBSinkPlugin.cpp
)

# Set include directories
target_include_directories(plugin_influxdb
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
)

# Link dependent libraries
target_link_libraries(plugin_influxdb
  PUBLIC
    parameter
    core_insert
    CURL::libcurl
    ZLIB::ZLIB
)

# Enable tests
if(TSGEN_ENABLE_TEST)
  add_subdirectory(test)
endif()
