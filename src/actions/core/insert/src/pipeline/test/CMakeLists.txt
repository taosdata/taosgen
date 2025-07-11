# Test DataQueue
add_executable(TestDataQueue
  TestDataQueue.cpp
)
target_link_libraries(TestDataQueue
  PRIVATE 
    insert_pipeline
)
add_test(NAME TestDataQueue COMMAND TestDataQueue)

# Test DataPipeline
add_executable(TestDataPipeline
  TestDataPipeline.cpp
)
target_link_libraries(TestDataPipeline
  PRIVATE 
    insert_pipeline
)
add_test(NAME TestDataPipeline COMMAND TestDataPipeline)