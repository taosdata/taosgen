# Test TDengineWriter
add_executable(TestTDengineWriter
  TestTDengineWriter.cpp
)
target_link_libraries(TestTDengineWriter
  PRIVATE
    plugin_tdengine
)
add_test(NAME TestTDengineWriter COMMAND TestTDengineWriter)
