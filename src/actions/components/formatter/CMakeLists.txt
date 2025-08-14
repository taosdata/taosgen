# Create library target
add_library(components_formatter STATIC
  src/FormatterFactory.cpp
  src/StmtV2Data.cpp
)

# Set include directories
target_include_directories(components_formatter
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../config/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../utils/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../memory_pool/inc
)

# Link dependencies
target_link_libraries(components_formatter
  PUBLIC
    components_memory_pool
    actions_config
    utils
    TAOS::TAOS
)

# Enable tests
if(CMAKE_ENABLE_TEST)
  add_subdirectory(test)
endif()