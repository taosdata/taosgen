# Test TDengineConfigParser
add_executable(TestTDengineConfigParser
  TestTDengineConfigParser.cpp
)
target_link_libraries(TestTDengineConfigParser
  PRIVATE
    plugin_tdengine
)
add_test(NAME TestTDengineConfigParser COMMAND TestTDengineConfigParser)

# Test TDengineWriter
add_executable(TestTDengineWriter
  TestTDengineWriter.cpp
)
target_link_libraries(TestTDengineWriter
  PRIVATE
    plugin_tdengine
)
add_test(NAME TestTDengineWriter COMMAND TestTDengineWriter)
