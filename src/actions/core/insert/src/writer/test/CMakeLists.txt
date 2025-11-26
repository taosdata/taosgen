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

# Test KafkaClient
add_executable(TestKafkaClient
  TestKafkaClient.cpp
)
target_link_libraries(TestKafkaClient
  PRIVATE
    insert_writer
)
add_test(NAME TestKafkaClient COMMAND TestKafkaClient)

# Test KafkaWriter
add_executable(TestKafkaWriter
  TestKafkaWriter.cpp
)
target_link_libraries(TestKafkaWriter
  PRIVATE
    insert_writer
)
add_test(NAME TestKafkaWriter COMMAND TestKafkaWriter)