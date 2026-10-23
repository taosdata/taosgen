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

# Test KafkaConfigParser
add_executable(TestKafkaConfigParser
  TestKafkaConfigParser.cpp
)
target_link_libraries(TestKafkaConfigParser
  PRIVATE
    plugin_kafka
)
add_test(NAME TestKafkaConfigParser COMMAND TestKafkaConfigParser)

# Test KafkaParameterContext
add_executable(TestKafkaParameterContext
  TestKafkaParameterContext.cpp
)
target_link_libraries(TestKafkaParameterContext
  PRIVATE
    plugins
)
add_test(NAME TestKafkaParameterContext COMMAND TestKafkaParameterContext)

# Test KafkaInsertData
add_executable(TestKafkaInsertData
  TestKafkaInsertData.cpp
)
target_link_libraries(TestKafkaInsertData
  PRIVATE
    plugin_kafka
)
add_test(NAME TestKafkaInsertData COMMAND TestKafkaInsertData)

# Test KafkaInsertDataFormatter
add_executable(TestKafkaInsertDataFormatter
  TestKafkaInsertDataFormatter.cpp
)
target_link_libraries(TestKafkaInsertDataFormatter
  PRIVATE
    plugin_kafka
)
add_test(NAME TestKafkaInsertDataFormatter COMMAND TestKafkaInsertDataFormatter)

# Test KafkaClient
add_executable(TestKafkaClient
  TestKafkaClient.cpp
)
target_link_libraries(TestKafkaClient
  PRIVATE
    plugin_kafka
)
add_test(NAME TestKafkaClient COMMAND TestKafkaClient)

# Test KafkaSinkPlugin
add_executable(TestKafkaSinkPlugin
  TestKafkaSinkPlugin.cpp
)
target_link_libraries(TestKafkaSinkPlugin
  PRIVATE
    plugin_kafka
)
add_test(NAME TestKafkaSinkPlugin COMMAND TestKafkaSinkPlugin)
