# Test InfluxDBInsertDataFormatter
add_executable(TestInfluxDBInsertDataFormatter
  TestInfluxDBInsertDataFormatter.cpp
)
target_link_libraries(TestInfluxDBInsertDataFormatter
  PRIVATE
    plugin_influxdb
)
add_test(NAME TestInfluxDBInsertDataFormatter COMMAND TestInfluxDBInsertDataFormatter)

# Test InfluxDBClient
add_executable(TestInfluxDBClient
  TestInfluxDBClient.cpp
)
target_link_libraries(TestInfluxDBClient
  PRIVATE
    plugin_influxdb
)
add_test(NAME TestInfluxDBClient COMMAND TestInfluxDBClient)
