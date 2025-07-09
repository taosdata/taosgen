# Test TimestampUtils
add_executable(TestTimestampUtils
  TestTimestampUtils.cpp
)
target_link_libraries(TestTimestampUtils
  PRIVATE 
    utils
)
add_test(NAME TestTimestampUtils COMMAND TestTimestampUtils)
