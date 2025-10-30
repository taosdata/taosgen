# Create library target
add_library(utils STATIC
  src/LogUtils.cpp
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
    spdlog::spdlog
    Threads::Threads
    Iconv::Iconv
)

# Enable tests
if(TSGEN_ENABLE_TEST)
  add_subdirectory(test)
endif()