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
