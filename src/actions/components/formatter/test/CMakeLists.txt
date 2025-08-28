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

# Test TopicGenerator
add_executable(TestTopicGenerator
  TestTopicGenerator.cpp
)
target_link_libraries(TestTopicGenerator
  PRIVATE
    insert_writer
)
add_test(NAME TestTopicGenerator COMMAND TestTopicGenerator)

# Test MsgInsertDataFormatter
add_executable(TestMsgInsertDataFormatter
  TestMsgInsertDataFormatter.cpp
)
target_link_libraries(TestMsgInsertDataFormatter
  PRIVATE
    insert_writer
)
add_test(NAME TestMsgInsertDataFormatter COMMAND TestMsgInsertDataFormatter)