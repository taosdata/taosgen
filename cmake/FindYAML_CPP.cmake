include(${CMAKE_CURRENT_LIST_DIR}/Utils.cmake)
setup_colored_output()

# Set search paths based on platform
if(WIN32)
    set(YAML_CPP_SEARCH_PATHS
        "C:/Program Files/yaml-cpp"
        "C:/yaml-cpp"
    )
else()
    set(YAML_CPP_SEARCH_PATHS
        "/usr/local"
        "/usr"
    )
endif()

# Find yaml-cpp header
find_path(YAML_CPP_INCLUDE_DIR
    NAMES yaml-cpp/yaml.h
    PATHS ${YAML_CPP_SEARCH_PATHS}
    PATH_SUFFIXES include
    DOC "yaml-cpp header directory"
)

# Find yaml-cpp library
find_library(YAML_CPP_LIBRARY
    NAMES yaml-cpp
    PATHS ${YAML_CPP_SEARCH_PATHS}
    PATH_SUFFIXES lib lib64
    DOC "yaml-cpp library"
)

# Version check (optional, yaml-cpp doesn't expose version in headers easily)
if(YAML_CPP_INCLUDE_DIR AND YAML_CPP_LIBRARY)
    set(YAML_CPP_VERSION_OK TRUE)
endif()

# Handle REQUIRED and QUIET arguments
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(YAML_CPP
    REQUIRED_VARS 
        YAML_CPP_LIBRARY
        YAML_CPP_INCLUDE_DIR
        YAML_CPP_VERSION_OK
    FAIL_MESSAGE "${Red}Could not find yaml-cpp library. Please install libyaml-cpp-dev${ColorReset}"
)

if(YAML_CPP_FOUND)
    set(YAML_CPP_LIBRARIES ${YAML_CPP_LIBRARY})
    set(YAML_CPP_INCLUDE_DIRS ${YAML_CPP_INCLUDE_DIR})
    message(STATUS "${Green}Found yaml-cpp: ${YAML_CPP_LIBRARY}${ColorReset}")
endif()

mark_as_advanced(YAML_CPP_INCLUDE_DIR YAML_CPP_LIBRARY)