# Create library target
add_library(plugin_mqtt STATIC
  # src/MqttInsertFormater.cpp
  src/MqttClient.cpp
  src/MqttWriter.cpp
)

# Set include directories
target_include_directories(plugin_mqtt
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
)

# Link dependent libraries
target_link_libraries(plugin_mqtt
  PUBLIC
    insert_writer
    PahoMqttCpp::paho-mqttpp3-static
)

# Enable tests
if(TSGEN_ENABLE_TEST)
  add_subdirectory(test)
endif()