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