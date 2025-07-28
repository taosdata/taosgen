# Test TimestampUtils
add_executable(TestTimestampUtils
  TestTimestampUtils.cpp
)
target_link_libraries(TestTimestampUtils
  PRIVATE 
    utils
)
add_test(NAME TestTimestampUtils COMMAND TestTimestampUtils)

# Test ProcessUtils
add_executable(TestProcessUtils
  TestProcessUtils.cpp
)
target_link_libraries(TestProcessUtils
  PRIVATE 
    utils
)
add_test(NAME TestProcessUtils COMMAND TestProcessUtils)
