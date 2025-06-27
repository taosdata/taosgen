# Test RowDataGenerator
add_executable(TestRowDataGenerator
  TestRowDataGenerator.cpp
)
target_link_libraries(TestRowDataGenerator
  PRIVATE 
    insert_generator
)
add_test(NAME TestRowDataGenerator COMMAND TestRowDataGenerator)

# Test TableDataManager
add_executable(TestTableDataManager
  TestTableDataManager.cpp
)
target_link_libraries(TestTableDataManager
  PRIVATE 
    insert_generator
)
add_test(NAME TestTableDataManager COMMAND TestTableDataManager)

# Test TableNameManager
add_executable(TestTableNameManager
  TestTableNameManager.cpp
)
target_link_libraries(TestTableNameManager
  PRIVATE 
    insert_generator
)
add_test(NAME TestTableNameManager COMMAND TestTableNameManager)