# Create library target
add_library(utils STATIC
  src/StringUtils.cpp
  src/TimestampUtils.cpp
  src/ProcessUtils.cpp
  src/Barrier.cpp
)

# Set include directories
target_include_directories(utils
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
)

# Link dependent libraries
target_link_libraries(utils
  PUBLIC
    Threads::Threads
)

# Enable tests
if(CMAKE_ENABLE_TEST)
  add_subdirectory(test)
endif()