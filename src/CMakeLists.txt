# Include directories
include_directories(
  ${CMAKE_SOURCE_DIR}/inc
)

# Add subdirectories (in dependency order)
add_subdirectory(utils)
add_subdirectory(actions)
add_subdirectory(parameter)
add_subdirectory(workflow)
add_subdirectory(engine)

# Main executable
add_executable(tsgen benchmark.cpp)

# Set include directories
target_include_directories(tsgen
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/parameter/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/engine/inc
)

# Link all dependent libraries
target_link_libraries(tsgen
  PRIVATE
    actions
    engine
    workflow
    parameter
    utils
    yaml-cpp::yaml-cpp
    luajit::luajit
    TAOS::TAOS
    Threads::Threads
)

# Install target
install(TARGETS tsgen
  RUNTIME DESTINATION bin
)