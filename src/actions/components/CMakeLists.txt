# Add subcomponent directories
add_subdirectory(metrics)
add_subdirectory(garbage_collector)
add_subdirectory(connector)
add_subdirectory(formatter)
add_subdirectory(generator)
add_subdirectory(reader)

# Create unified components library
add_library(actions_components STATIC
  $<TARGET_OBJECTS:components_metrics>
  $<TARGET_OBJECTS:components_gc>
  $<TARGET_OBJECTS:components_connector>
  $<TARGET_OBJECTS:components_formatter>
  $<TARGET_OBJECTS:components_generator>
  $<TARGET_OBJECTS:components_reader>
)

# Set include directories
target_include_directories(actions_components
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/metrics/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/garbage_collector/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/connector/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/formatter/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/generator/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/reader/inc
)

# Link submodules
target_link_libraries(actions_components
  PUBLIC
    components_metrics
    components_gc
    components_connector
    components_formatter
    components_generator
    components_reader
)