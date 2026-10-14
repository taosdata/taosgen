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

# Test TDengineConfigParser
add_executable(TestTDengineConfigParser
  TestTDengineConfigParser.cpp
)
target_link_libraries(TestTDengineConfigParser
  PRIVATE
    plugin_tdengine
)
add_test(NAME TestTDengineConfigParser COMMAND TestTDengineConfigParser)

# test SqlInsertDataFormatter
add_executable(TestSqlInsertDataFormatter
  TestSqlInsertDataFormatter.cpp
)
target_link_libraries(TestSqlInsertDataFormatter
  PRIVATE
    plugin_tdengine
)
add_test(NAME TestSqlInsertDataFormatter COMMAND TestSqlInsertDataFormatter)

# test StmtInsertDataFormatter
add_executable(TestStmtInsertDataFormatter
  TestStmtInsertDataFormatter.cpp
)
target_link_libraries(TestStmtInsertDataFormatter
  PRIVATE
    plugin_tdengine
)
add_test(NAME TestStmtInsertDataFormatter COMMAND TestStmtInsertDataFormatter)

# Test TDengineSinkPlugin
add_executable(TestTDengineSinkPlugin
  TestTDengineSinkPlugin.cpp
)
target_link_libraries(TestTDengineSinkPlugin
  PRIVATE
    plugin_tdengine
)
add_test(NAME TestTDengineSinkPlugin COMMAND TestTDengineSinkPlugin)

# Test ingress TDengine writer
add_executable(TestIngestionTDengineWriter
  TestIngestionTDengineWriter.cpp
)
target_link_libraries(TestIngestionTDengineWriter
  PRIVATE
    plugin_tdengine
)
add_test(NAME TestIngestionTDengineWriter COMMAND TestIngestionTDengineWriter)
add_executable(TestTDengineRetryLogging
  TestTDengineRetryLogging.cpp
)
target_link_libraries(TestTDengineRetryLogging
  PRIVATE
    plugin_tdengine
)
add_test(NAME TestTDengineRetryLogging COMMAND TestTDengineRetryLogging)

# Test SchemalessInsertDataFormatter
add_executable(TestSchemalessInsertDataFormatter
  TestSchemalessInsertDataFormatter.cpp
)
target_link_libraries(TestSchemalessInsertDataFormatter
  PRIVATE
    plugin_tdengine
)
add_test(NAME TestSchemalessInsertDataFormatter COMMAND TestSchemalessInsertDataFormatter)
