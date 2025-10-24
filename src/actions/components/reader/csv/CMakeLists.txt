# Create library target
add_library(reader_csv STATIC
  src/CSVReader.cpp
  src/TableNameCSVReader.cpp
  src/TagsCSVReader.cpp
  src/ColumnsCSVReader.cpp
)

# Set include directories
target_include_directories(reader_csv
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../../utils/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../config/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../generator/inc
)

# Link dependencies
target_link_libraries(reader_csv
  PUBLIC
    actions_config
    components_generator
    utils
)

# Enable tests
if(TSGEN_ENABLE_TEST)
  add_subdirectory(test)
endif()