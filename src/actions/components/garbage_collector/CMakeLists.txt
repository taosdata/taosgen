# Create library target
add_library(components_gc STATIC 
  src/GarbageCollector.cpp
)

# Set include directories
target_include_directories(components_gc
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
)

# Link dependencies
target_link_libraries(components_gc
  PUBLIC
)

# Enable tests
if(CMAKE_ENABLE_TEST)
  add_subdirectory(test)
endif()