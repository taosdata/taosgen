# Create library target
add_library(insert_generator STATIC 
  src/RowDataGenerator.cpp
  src/RateLimiter.cpp
  src/TableDataManager.cpp
  src/TableNameManager.cpp
)

# Set include directories
target_include_directories(insert_generator
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../../../utils/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../../config/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../../components/generator/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../../components/reader/csv/inc
)

# Link dependent libraries
target_link_libraries(insert_generator
  PUBLIC
    actions_config
    components_generator
    components_reader
    utils
)

# Enable tests
if(CMAKE_ENABLE_TEST)
  add_subdirectory(test)
endif()