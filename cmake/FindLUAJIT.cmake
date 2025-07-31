include(${CMAKE_CURRENT_LIST_DIR}/Utils.cmake)
setup_colored_output()

# Set search paths based on platform
if(WIN32)
    set(LUAJIT_SEARCH_PATHS
        "C:/Program Files/LuaJIT"
        "C:/LuaJIT"
    )
else()
    set(LUAJIT_SEARCH_PATHS
        "/usr/local"
        "/usr"
    )
endif()

# Find LuaJIT header
find_path(LUAJIT_INCLUDE_DIR
    NAMES lua.hpp
    PATHS ${LUAJIT_SEARCH_PATHS}
    PATH_SUFFIXES include include/luajit-2.1 include/luajit-2.0 luajit-2.1 luajit-2.0
    DOC "LuaJIT header directory"
)

# Find LuaJIT library
find_library(LUAJIT_LIBRARY
    NAMES luajit-5.1 luajit
    PATHS ${LUAJIT_SEARCH_PATHS}
    PATH_SUFFIXES lib lib64
    DOC "LuaJIT library"
)

# Version check (optional, LuaJIT doesn't expose version in headers easily)
if(LUAJIT_INCLUDE_DIR AND LUAJIT_LIBRARY)
    set(LUAJIT_VERSION_OK TRUE)
endif()

# Handle REQUIRED and QUIET arguments
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(LUAJIT
    REQUIRED_VARS 
        LUAJIT_LIBRARY
        LUAJIT_INCLUDE_DIR
        LUAJIT_VERSION_OK
    FAIL_MESSAGE "${Red}Could not find LuaJIT library. Please install luajit or libluajit-dev${ColorReset}"
)

if(LUAJIT_FOUND)
    set(LUAJIT_LIBRARIES ${LUAJIT_LIBRARY})
    set(LUAJIT_INCLUDE_DIRS ${LUAJIT_INCLUDE_DIR})
    message(STATUS "${Green}Found LuaJIT: ${LUAJIT_LIBRARY}${ColorReset}")
    message(STATUS "LUAJIT_INCLUDE_DIRS = ${LUAJIT_INCLUDE_DIRS}")
endif()

mark_as_advanced(LUAJIT_INCLUDE_DIR LUAJIT_LIBRARY)