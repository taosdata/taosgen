# test TDengineConfig
add_executable(TestTDengineConfig
  TestTDengineConfig.cpp
)
target_link_libraries(TestTDengineConfig
  PRIVATE
    actions_config
)
add_test(NAME TestTDengineConfig COMMAND TestTDengineConfig)

# test ColumnConfig
add_executable(TestColumnConfig
  TestColumnConfig.cpp
)
target_link_libraries(TestColumnConfig
  PRIVATE
    actions_config
)
add_test(NAME TestColumnConfig COMMAND TestColumnConfig)

# test ColumnConfigInstance
add_executable(TestColumnConfigInstance
  TestColumnConfigInstance.cpp
)
target_link_libraries(TestColumnConfigInstance
  PRIVATE
    actions_config
)
add_test(NAME TestColumnConfigInstance COMMAND TestColumnConfigInstance)

# Test TypeConverter
add_executable(TestTypeConverter
  TestTypeConverter.cpp
)
target_link_libraries(TestTypeConverter
  PRIVATE
    actions_config
)
add_test(NAME TestTypeConverter COMMAND TestTypeConverter)