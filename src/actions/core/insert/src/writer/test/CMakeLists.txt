# Test TimeIntervalStrategy
add_executable(TestTimeIntervalStrategy
  TestTimeIntervalStrategy.cpp
)
target_link_libraries(TestTimeIntervalStrategy
  PRIVATE
    insert_writer
)
add_test(NAME TestTimeIntervalStrategy COMMAND TestTimeIntervalStrategy)

# Test TDengineWriter
add_executable(TestTDengineWriter
  TestTDengineWriter.cpp
)
target_link_libraries(TestTDengineWriter
  PRIVATE
    insert_writer
)
add_test(NAME TestTDengineWriter COMMAND TestTDengineWriter)

# Test TopicGenerator
add_executable(TestTopicGenerator
  TestTopicGenerator.cpp
)
target_link_libraries(TestTopicGenerator
  PRIVATE
    insert_writer
)
add_test(NAME TestTopicGenerator COMMAND TestTopicGenerator)

# Test MqttClient
add_executable(TestMqttClient
  TestMqttClient.cpp
)
target_link_libraries(TestMqttClient
  PRIVATE
    insert_writer
)
add_test(NAME TestMqttClient COMMAND TestMqttClient)

# Test MqttWriter
add_executable(TestMqttWriter
  TestMqttWriter.cpp
)
target_link_libraries(TestMqttWriter
  PRIVATE
    insert_writer
)
add_test(NAME TestMqttWriter COMMAND TestMqttWriter)