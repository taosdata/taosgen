# Test JobScheduler
add_executable(TestJobScheduler
  TestJobScheduler.cpp
)

target_link_libraries(TestJobScheduler
  PRIVATE 
    engine
    parameter
    ${YAML_CPP_LIBRARIES}
)

add_test(NAME TestJobScheduler COMMAND TestJobScheduler)