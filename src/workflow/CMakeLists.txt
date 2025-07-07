# Create library target
add_library(workflow STATIC
  src/Job.cpp
)

# Set include directories
target_include_directories(workflow
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../actions/config/inc
)

# Link dependent libraries
target_link_libraries(workflow
  PUBLIC
    ${YAML_CPP_LIBRARIES}
)

# Enable tests
# if(CMAKE_ENABLE_TEST)
#   add_subdirectory(test)
# endif()