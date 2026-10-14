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

# Test TimeIntervalStrategy
add_executable(TestTimeIntervalStrategy
  TestTimeIntervalStrategy.cpp
)
target_link_libraries(TestTimeIntervalStrategy
  PRIVATE
    insert_writer
)
add_test(NAME TestTimeIntervalStrategy COMMAND TestTimeIntervalStrategy)

# Test SinkPluginFactory
add_executable(TestSinkPluginFactory
  TestSinkPluginFactory.cpp
)
target_link_libraries(TestSinkPluginFactory
  PRIVATE
    insert_writer
)
add_test(NAME TestSinkPluginFactory COMMAND TestSinkPluginFactory)

add_executable(TestSinkRetryLogging
  TestSinkRetryLogging.cpp
)
target_link_libraries(TestSinkRetryLogging
  PRIVATE
    insert_writer
)
add_test(NAME TestSinkRetryLogging COMMAND TestSinkRetryLogging)
