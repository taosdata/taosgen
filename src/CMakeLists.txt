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
add_executable(taosgen benchmark.cpp)

# Set include directories
target_include_directories(taosgen
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/parameter/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/engine/inc
)

# Link all dependent libraries
target_link_libraries(taosgen
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
install(TARGETS taosgen
  RUNTIME DESTINATION bin
)