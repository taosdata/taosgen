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

# Test LogUtils
add_executable(TestLogUtils
  TestLogUtils.cpp
)
target_link_libraries(TestLogUtils
  PRIVATE
    utils
)
add_test(NAME TestLogUtils COMMAND TestLogUtils)

# Test TimestampUtils
add_executable(TestTimestampUtils
  TestTimestampUtils.cpp
)
target_link_libraries(TestTimestampUtils
  PRIVATE
    utils
)
add_test(NAME TestTimestampUtils COMMAND TestTimestampUtils)

# Test ProcessUtils
add_executable(TestProcessUtils
  TestProcessUtils.cpp
)
target_link_libraries(TestProcessUtils
  PRIVATE
    utils
)
add_test(NAME TestProcessUtils COMMAND TestProcessUtils)

# Test SignalManager
add_executable(TestSignalManager
  TestSignalManager.cpp
)
target_link_libraries(TestSignalManager
  PRIVATE
    utils
)
add_test(NAME TestSignalManager COMMAND TestSignalManager)

# Test Barrier
add_executable(TestBarrier
  TestBarrier.cpp
)
target_link_libraries(TestBarrier
  PRIVATE
    utils
)
add_test(NAME TestBarrier COMMAND TestBarrier)

# Test Latch
add_executable(TestLatch
  TestLatch.cpp
)
target_link_libraries(TestLatch
  PRIVATE
    utils
)
add_test(NAME TestLatch COMMAND TestLatch)

# Test ScopedEnvVar
add_executable(TestScopedEnvVar
  TestScopedEnvVar.cpp
)
target_link_libraries(TestScopedEnvVar
  PRIVATE
    utils
)
add_test(NAME TestScopedEnvVar COMMAND TestScopedEnvVar)

# Test ConcurrentQueue
add_executable(TestConcurrentQueue
  TestConcurrentQueue.cpp
)
target_link_libraries(TestConcurrentQueue
  PRIVATE
    utils
)
add_test(NAME TestConcurrentQueue COMMAND TestConcurrentQueue)

# Test StringUtils
add_executable(TestStringUtils
  TestStringUtils.cpp
)
target_link_libraries(TestStringUtils
  PRIVATE
    utils
)
add_test(NAME TestStringUtils COMMAND TestStringUtils)