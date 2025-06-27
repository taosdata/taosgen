# test ConnectionInfo
add_executable(TestConnectionInfo
test/TestConnectionInfo.cpp
)
target_link_libraries(TestConnectionInfo
PRIVATE 
    actions_config
)
add_test(NAME TestConnectionInfo COMMAND TestConnectionInfo)

# test ColumnConfig
add_executable(TestColumnConfig
test/TestColumnConfig.cpp
)
target_link_libraries(TestColumnConfig
PRIVATE 
    actions_config
)
add_test(NAME TestColumnConfig COMMAND TestColumnConfig)

# test ColumnConfigInstance
add_executable(TestColumnConfigInstance
test/TestColumnConfigInstance.cpp
)
target_link_libraries(TestColumnConfigInstance
PRIVATE 
    actions_config
)
add_test(NAME TestColumnConfigInstance COMMAND TestColumnConfigInstance)