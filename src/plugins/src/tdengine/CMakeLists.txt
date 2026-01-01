# Create library target
add_library(plugin_tdengine STATIC
  # src/SqlInsertFormater.cpp
  # src/StmtInsertFormater.cpp
  src/TDengineWriter.cpp
)

# Set include directories
target_include_directories(plugin_tdengine
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
)

# Link dependent libraries
target_link_libraries(plugin_tdengine
  PUBLIC
    insert_writer
)

# Enable tests
if(TSGEN_ENABLE_TEST)
  add_subdirectory(test)
endif()