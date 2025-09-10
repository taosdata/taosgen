# Create library target
add_library(parameter STATIC
  src/ParameterContext.cpp
)

# Set include directories
target_include_directories(parameter
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../workflow/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../actions/config/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../actions/core/checkpoint/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../actions/base
)

# Link dependent libraries
target_link_libraries(parameter
  PUBLIC
    actions_config
    core_checkpoint
    yaml-cpp::yaml-cpp
)

# Enable tests
if(CMAKE_ENABLE_TEST)
  add_subdirectory(test)
endif()