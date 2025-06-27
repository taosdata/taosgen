# test ParseYAML
add_executable(TestParseYAML 
  TestParseYAML.cpp
)
target_link_libraries(TestParseYAML 
  PRIVATE 
    yaml-cpp
)
add_test(NAME TestParseYAML COMMAND TestParseYAML)

# test ParameterContext
add_executable(TestParameterContext 
  TestParameterContext.cpp
)
target_link_libraries(TestParameterContext 
  PRIVATE 
    parameter
    nlohmann_json::nlohmann_json
    yaml-cpp
)
add_test(NAME TestParameterContext COMMAND TestParameterContext)