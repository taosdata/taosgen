# Create library target
add_library(components_expression STATIC
  src/MathFunctions.cpp
  src/NetworkFunctions.cpp
  src/ExpressionEngine.cpp
)

# Set include directories
target_include_directories(components_expression
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../config/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../utils/inc
)

# Link dependencies
target_link_libraries(components_expression
  PUBLIC
    luajit::luajit
    m
    dl
)

# Set compilation options
target_compile_options(components_expression PRIVATE
    -fno-strict-aliasing
)

# Set link options
set_target_properties(components_expression PROPERTIES
    LINK_FLAGS "-Wl,--export-dynamic"
)

# Enable tests
if(CMAKE_ENABLE_TEST)
  add_subdirectory(test)
endif()