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

# test SqlDatabaseFormatter
add_executable(TestSqlDatabaseFormatter
  TestSqlDatabaseFormatter.cpp
)
target_link_libraries(TestSqlDatabaseFormatter
  PRIVATE
    components_formatter
)
add_test(NAME TestSqlDatabaseFormatter COMMAND TestSqlDatabaseFormatter)

# test SqlSuperTableFormatter
add_executable(TestSqlSuperTableFormatter
  TestSqlSuperTableFormatter.cpp
)
target_link_libraries(TestSqlSuperTableFormatter
  PRIVATE
    components_formatter
)
add_test(NAME TestSqlSuperTableFormatter COMMAND TestSqlSuperTableFormatter)

# test SqlChildTableFormatter
add_executable(TestSqlChildTableFormatter
  TestSqlChildTableFormatter.cpp
)
target_link_libraries(TestSqlChildTableFormatter
  PRIVATE
    components_formatter
)
add_test(NAME TestSqlChildTableFormatter COMMAND TestSqlChildTableFormatter)

# test FormatResult
add_executable(TestFormatResult
  TestFormatResult.cpp
)
target_link_libraries(TestFormatResult
  PRIVATE
    components_formatter
)
add_test(NAME TestFormatResult COMMAND TestFormatResult)

# Test PatternGenerator
add_executable(TestPatternGenerator
  TestPatternGenerator.cpp
)
target_link_libraries(TestPatternGenerator
  PRIVATE
    insert_writer
)
add_test(NAME TestPatternGenerator COMMAND TestPatternGenerator)

# Test TopicGenerator
add_executable(TestTopicGenerator
  TestTopicGenerator.cpp
)
target_link_libraries(TestTopicGenerator
  PRIVATE
    insert_writer
)
add_test(NAME TestTopicGenerator COMMAND TestTopicGenerator)

# Test KeyGenerator
add_executable(TestKeyGenerator
  TestKeyGenerator.cpp
)
target_link_libraries(TestKeyGenerator
  PRIVATE
    insert_writer
)
add_test(NAME TestKeyGenerator COMMAND TestKeyGenerator)

# Test RowSerializer
add_executable(TestRowSerializer
  TestRowSerializer.cpp
)
target_link_libraries(TestRowSerializer
  PRIVATE
    insert_writer
)
add_test(NAME TestRowSerializer COMMAND TestRowSerializer)

# Test StmtV2Data
add_executable(TestStmtV2Data
  TestStmtV2Data.cpp
)
target_link_libraries(TestStmtV2Data
  PRIVATE
    components_formatter
)
add_test(NAME TestStmtV2Data COMMAND TestStmtV2Data)
