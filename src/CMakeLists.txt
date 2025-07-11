# Include directories
include_directories(
  ${CMAKE_SOURCE_DIR}/inc
  ${YAML_CPP_INCLUDE_DIRS}
  ${TAOS_INCLUDE_DIRS}
)

# Add subdirectories (in dependency order)
add_subdirectory(utils)
add_subdirectory(actions)
add_subdirectory(parameter)
add_subdirectory(workflow)
add_subdirectory(engine)

# Main executable
add_executable(taosbench benchmark.cpp)

# Set include directories
target_include_directories(parameter
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/parameter/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/engine/inc
)

# Link all dependent libraries
target_link_libraries(taosbench
  PRIVATE
    actions
    engine
    workflow
    parameter
    utils
    ${YAML_CPP_LIBRARIES}
    ${TAOS_LIBRARIES}
    Threads::Threads
)

# Install target
install(TARGETS taosbench 
  RUNTIME DESTINATION bin
)