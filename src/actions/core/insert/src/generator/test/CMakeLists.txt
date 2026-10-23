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

# Test RowDataGenerator
add_executable(TestRowDataGenerator
  TestRowDataGenerator.cpp
)
target_link_libraries(TestRowDataGenerator
  PRIVATE
    insert_generator
)
add_test(NAME TestRowDataGenerator COMMAND TestRowDataGenerator)

# Test RowDataGenerator Streaming/Supplemental
add_executable(TestRowDataGeneratorStreaming
  TestRowDataGeneratorStreaming.cpp
)
target_link_libraries(TestRowDataGeneratorStreaming
  PRIVATE
    insert_generator
)
add_test(NAME TestRowDataGeneratorStreaming COMMAND TestRowDataGeneratorStreaming)

# Test TableDataManager
add_executable(TestTableDataManager
  TestTableDataManager.cpp
)
target_link_libraries(TestTableDataManager
  PRIVATE
    insert_generator
)
add_test(NAME TestTableDataManager COMMAND TestTableDataManager)

# Test TableNameManager
add_executable(TestTableNameManager
  TestTableNameManager.cpp
)
target_link_libraries(TestTableNameManager
  PRIVATE
    insert_generator
)
add_test(NAME TestTableNameManager COMMAND TestTableNameManager)