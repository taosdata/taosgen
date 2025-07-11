# test GarbageCollector
add_executable(TestGarbageCollector
TestGarbageCollector.cpp
)
target_link_libraries(TestGarbageCollector
  PRIVATE 
    components_gc
)
add_test(NAME TestGarbageCollector COMMAND TestGarbageCollector)
