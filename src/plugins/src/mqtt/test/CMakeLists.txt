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

# Test MqttConfigParser
add_executable(TestMqttConfigParser
  TestMqttConfigParser.cpp
)
target_link_libraries(TestMqttConfigParser
  PRIVATE
    plugin_mqtt
)
add_test(NAME TestMqttConfigParser COMMAND TestMqttConfigParser)

# Test MqttParameterContext
add_executable(TestMqttParameterContext
  TestMqttParameterContext.cpp
)
target_link_libraries(TestMqttParameterContext
  PRIVATE
    plugins
)
add_test(NAME TestMqttParameterContext COMMAND TestMqttParameterContext)

# Test MqttInsertData
add_executable(TestMqttInsertData
  TestMqttInsertData.cpp
)
target_link_libraries(TestMqttInsertData
  PRIVATE
    plugin_mqtt
)
add_test(NAME TestMqttInsertData COMMAND TestMqttInsertData)

# Test MqttInsertDataFormatter
add_executable(TestMqttInsertDataFormatter
  TestMqttInsertDataFormatter.cpp
)
target_link_libraries(TestMqttInsertDataFormatter
  PRIVATE
    plugin_mqtt
)
add_test(NAME TestMqttInsertDataFormatter COMMAND TestMqttInsertDataFormatter)

# Test MqttClient
add_executable(TestMqttClient
  TestMqttClient.cpp
)
target_link_libraries(TestMqttClient
  PRIVATE
    plugin_mqtt
)
add_test(NAME TestMqttClient COMMAND TestMqttClient)

# Test MqttSinkPlugin
add_executable(TestMqttSinkPlugin
  TestMqttSinkPlugin.cpp
)
target_link_libraries(TestMqttSinkPlugin
  PRIVATE
    plugin_mqtt
)
add_test(NAME TestMqttSinkPlugin COMMAND TestMqttSinkPlugin)
