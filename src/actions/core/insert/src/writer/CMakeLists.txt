# Create library target
add_library(insert_writer STATIC 
  src/TimeIntervalStrategy.cpp
  src/TDengineWriter.cpp
)

# Set include directories
target_include_directories(insert_writer
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../../../utils/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../../config/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../../components/metrics/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../../components/connector/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../../components/formatter/inc
)

# Link dependent libraries
target_link_libraries(insert_writer
  PUBLIC
    actions_config
    components_metrics
    components_connector
    utils
    ${TAOS_LIBRARIES}
)

# Enable tests
if(CMAKE_ENABLE_TEST)
  add_subdirectory(test)
endif()