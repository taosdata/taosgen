# Test TDengineConfigParser
add_executable(TestTDengineConfigParser
  TestTDengineConfigParser.cpp
)
target_link_libraries(TestTDengineConfigParser
  PRIVATE
    plugin_tdengine
)
add_test(NAME TestTDengineConfigParser COMMAND TestTDengineConfigParser)

# test SqlInsertDataFormatter
add_executable(TestSqlInsertDataFormatter
  TestSqlInsertDataFormatter.cpp
)
target_link_libraries(TestSqlInsertDataFormatter
  PRIVATE
    plugin_tdengine
)
add_test(NAME TestSqlInsertDataFormatter COMMAND TestSqlInsertDataFormatter)

# test StmtInsertDataFormatter
add_executable(TestStmtInsertDataFormatter
  TestStmtInsertDataFormatter.cpp
)
target_link_libraries(TestStmtInsertDataFormatter
  PRIVATE
    plugin_tdengine
)
add_test(NAME TestStmtInsertDataFormatter COMMAND TestStmtInsertDataFormatter)

# Test TDengineWriter
add_executable(TestTDengineWriter
  TestTDengineWriter.cpp
)
target_link_libraries(TestTDengineWriter
  PRIVATE
    plugin_tdengine
)
add_test(NAME TestTDengineWriter COMMAND TestTDengineWriter)
