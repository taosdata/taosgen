# Create library target
add_library(plugin_kafka STATIC
  src/KafkaClient.cpp
  src/KafkaWriter.cpp
)

# Set include directories
target_include_directories(plugin_kafka
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
)

# Link dependent libraries
target_link_libraries(plugin_kafka
  PUBLIC
    insert_writer
    RdKafka::rdkafka++
)

# Enable tests
if(TSGEN_ENABLE_TEST)
  add_subdirectory(test)
endif()