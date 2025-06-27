# test NativeConnector
add_executable(TestNativeConnector
  TestNativeConnector.cpp
)
target_link_libraries(TestNativeConnector
  PRIVATE 
    components_connector
)
add_test(NAME TestNativeConnector COMMAND TestNativeConnector)
