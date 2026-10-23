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

# test TDengineConfig
add_executable(TestTDengineConfig
  TestTDengineConfig.cpp
)
target_link_libraries(TestTDengineConfig
  PRIVATE
    actions_config
)
add_test(NAME TestTDengineConfig COMMAND TestTDengineConfig)

# test ColumnConfig
add_executable(TestColumnConfig
  TestColumnConfig.cpp
)
target_link_libraries(TestColumnConfig
  PRIVATE
    actions_config
)
add_test(NAME TestColumnConfig COMMAND TestColumnConfig)

# test ColumnType
add_executable(TestColumnType
  TestColumnType.cpp
)
target_link_libraries(TestColumnType
  PRIVATE
    actions_config
)
add_test(NAME TestColumnType COMMAND TestColumnType)

# test ColumnConfigInstance
add_executable(TestColumnConfigInstance
  TestColumnConfigInstance.cpp
)
target_link_libraries(TestColumnConfigInstance
  PRIVATE
    actions_config
)
add_test(NAME TestColumnConfigInstance COMMAND TestColumnConfigInstance)

# Test TypeConverter
add_executable(TestTypeConverter
  TestTypeConverter.cpp
)
target_link_libraries(TestTypeConverter
  PRIVATE
    actions_config
)
add_test(NAME TestTypeConverter COMMAND TestTypeConverter)

# Test TimestampCSVConfig
add_executable(TestTimestampCSVConfig
  TestTimestampCSVConfig.cpp
)
target_link_libraries(TestTimestampCSVConfig
  PRIVATE
    actions_config
)
add_test(NAME TestTimestampCSVConfig COMMAND TestTimestampCSVConfig)

# Test SchemaConfig
add_executable(TestSchemaConfig
  TestSchemaConfig.cpp
)
target_link_libraries(TestSchemaConfig
  PRIVATE
    actions_config
)
add_test(NAME TestSchemaConfig COMMAND TestSchemaConfig)