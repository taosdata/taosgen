# Test InsertDataAction
add_executable(TestInsertDataAction
  TestInsertDataAction.cpp
)
target_link_libraries(TestInsertDataAction
  PRIVATE 
    core_insert
    ${TAOS_LIBRARIES}
)
add_test(NAME TestInsertDataAction COMMAND TestInsertDataAction)