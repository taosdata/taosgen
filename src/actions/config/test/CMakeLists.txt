# test ConnectionInfo
add_executable(TestConnectionInfo
  TestConnectionInfo.cpp
)
target_link_libraries(TestConnectionInfo
  PRIVATE 
    actions_config
)
add_test(NAME TestConnectionInfo COMMAND TestConnectionInfo)

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