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

# Test MqttWriter
add_executable(TestMqttWriter
  TestMqttWriter.cpp
)
target_link_libraries(TestMqttWriter
  PRIVATE
    plugin_mqtt
)
add_test(NAME TestMqttWriter COMMAND TestMqttWriter)
