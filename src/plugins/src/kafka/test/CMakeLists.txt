# Test KafkaClient
add_executable(TestKafkaClient
  TestKafkaClient.cpp
)
target_link_libraries(TestKafkaClient
  PRIVATE
    plugin_kafka
)
add_test(NAME TestKafkaClient COMMAND TestKafkaClient)

# Test KafkaWriter
add_executable(TestKafkaWriter
  TestKafkaWriter.cpp
)
target_link_libraries(TestKafkaWriter
  PRIVATE
    plugin_kafka
)
add_test(NAME TestKafkaWriter COMMAND TestKafkaWriter)
