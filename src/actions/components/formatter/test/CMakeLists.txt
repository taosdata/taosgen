# test SqlDatabaseFormatter
add_executable(TestSqlDatabaseFormatter
  TestSqlDatabaseFormatter.cpp
)
target_link_libraries(TestSqlDatabaseFormatter
  PRIVATE
    components_formatter
)
add_test(NAME TestSqlDatabaseFormatter COMMAND TestSqlDatabaseFormatter)

# test SqlSuperTableFormatter
add_executable(TestSqlSuperTableFormatter
  TestSqlSuperTableFormatter.cpp
)
target_link_libraries(TestSqlSuperTableFormatter
  PRIVATE
    components_formatter
)
add_test(NAME TestSqlSuperTableFormatter COMMAND TestSqlSuperTableFormatter)

# test SqlChildTableFormatter
add_executable(TestSqlChildTableFormatter
  TestSqlChildTableFormatter.cpp
)
target_link_libraries(TestSqlChildTableFormatter
  PRIVATE
    components_formatter
)
add_test(NAME TestSqlChildTableFormatter COMMAND TestSqlChildTableFormatter)

# test SqlInsertDataFormatter
add_executable(TestSqlInsertDataFormatter
  TestSqlInsertDataFormatter.cpp
)
target_link_libraries(TestSqlInsertDataFormatter
  PRIVATE
    components_formatter
)
add_test(NAME TestSqlInsertDataFormatter COMMAND TestSqlInsertDataFormatter)

# test StmtInsertDataFormatter
add_executable(TestStmtInsertDataFormatter
  TestStmtInsertDataFormatter.cpp
)
target_link_libraries(TestStmtInsertDataFormatter
  PRIVATE
    components_formatter
)
add_test(NAME TestStmtInsertDataFormatter COMMAND TestStmtInsertDataFormatter)

# test FormatResult
add_executable(TestFormatResult
  TestFormatResult.cpp
)
target_link_libraries(TestFormatResult
  PRIVATE
    components_formatter
)
add_test(NAME TestFormatResult COMMAND TestFormatResult)

# Test PatternGenerator
add_executable(TestPatternGenerator
  TestPatternGenerator.cpp
)
target_link_libraries(TestPatternGenerator
  PRIVATE
    insert_writer
)
add_test(NAME TestPatternGenerator COMMAND TestPatternGenerator)

# Test TopicGenerator
add_executable(TestTopicGenerator
  TestTopicGenerator.cpp
)
target_link_libraries(TestTopicGenerator
  PRIVATE
    insert_writer
)
add_test(NAME TestTopicGenerator COMMAND TestTopicGenerator)

# Test KeyGenerator
add_executable(TestKeyGenerator
  TestKeyGenerator.cpp
)
target_link_libraries(TestKeyGenerator
  PRIVATE
    insert_writer
)
add_test(NAME TestKeyGenerator COMMAND TestKeyGenerator)

# Test MqttInsertDataFormatter
add_executable(TestMqttInsertDataFormatter
  TestMqttInsertDataFormatter.cpp
)
target_link_libraries(TestMqttInsertDataFormatter
  PRIVATE
    insert_writer
)
add_test(NAME TestMqttInsertDataFormatter COMMAND TestMqttInsertDataFormatter)

# Test KafkaInsertDataFormatter
add_executable(TestKafkaInsertDataFormatter
  TestKafkaInsertDataFormatter.cpp
)
target_link_libraries(TestKafkaInsertDataFormatter
  PRIVATE
    insert_writer
)
add_test(NAME TestKafkaInsertDataFormatter COMMAND TestKafkaInsertDataFormatter)
