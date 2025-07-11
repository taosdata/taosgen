# Test ActionMetrics
add_executable(TestActionMetrics
  TestActionMetrics.cpp
)
target_link_libraries(TestActionMetrics
  PRIVATE 
    components_metrics
)
add_test(NAME TestActionMetrics COMMAND TestActionMetrics)
