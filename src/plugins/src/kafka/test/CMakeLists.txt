# Test KafkaConfigParser
add_executable(TestKafkaConfigParser
  TestKafkaConfigParser.cpp
)
target_link_libraries(TestKafkaConfigParser
  PRIVATE
    plugin_kafka
)
add_test(NAME TestKafkaConfigParser COMMAND TestKafkaConfigParser)

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

# Test KafkaWriter
add_executable(TestKafkaWriter
  TestKafkaWriter.cpp
)
target_link_libraries(TestKafkaWriter
  PRIVATE
    plugin_kafka
)
add_test(NAME TestKafkaWriter COMMAND TestKafkaWriter)
