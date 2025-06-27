# Test CSVReader
add_executable(TestCSVReader
  TestCSVReader.cpp
)
target_link_libraries(TestCSVReader
  PRIVATE 
    reader_csv
)
add_test(NAME TestCSVReader COMMAND TestCSVReader)

# Test TableNameCSV
add_executable(TestTableNameCSV
  TestTableNameCSV.cpp
)
target_link_libraries(TestTableNameCSV
  PRIVATE 
    reader_csv
)
add_test(NAME TestTableNameCSV COMMAND TestTableNameCSV)

# Test TagsCSV
add_executable(TestTagsCSV
  TestTagsCSV.cpp
)
target_link_libraries(TestTagsCSV
  PRIVATE 
    reader_csv
)
add_test(NAME TestTagsCSV COMMAND TestTagsCSV)

# Test ColumnsCSV
add_executable(TestColumnsCSV
  TestColumnsCSV.cpp
)
target_link_libraries(TestColumnsCSV
  PRIVATE 
    reader_csv
)
add_test(NAME TestColumnsCSV COMMAND TestColumnsCSV)