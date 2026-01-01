# Test TimeIntervalStrategy
add_executable(TestTimeIntervalStrategy
  TestTimeIntervalStrategy.cpp
)
target_link_libraries(TestTimeIntervalStrategy
  PRIVATE
    insert_writer
)
add_test(NAME TestTimeIntervalStrategy COMMAND TestTimeIntervalStrategy)

# Test WriterFactory
add_executable(TestWriterFactory
  TestWriterFactory.cpp
)
target_link_libraries(TestWriterFactory
  PRIVATE
    insert_writer
)
add_test(NAME TestWriterFactory COMMAND TestWriterFactory)