# Set up colored output
macro(setup_colored_output)
    string(ASCII 27 Esc)
    set(ColorReset  "${Esc}[m")
    set(Red         "${Esc}[31m")
    set(Green       "${Esc}[32m")
    set(Yellow      "${Esc}[33m")
    set(Blue        "${Esc}[34m")
endmacro()

# Version check helper
macro(check_program_version name version prog ver_arg ver_regex)
    find_program(${name}_EXECUTABLE ${prog})
    if(${name}_EXECUTABLE)
        execute_process(
            COMMAND ${${name}_EXECUTABLE} ${ver_arg}
            OUTPUT_VARIABLE ${name}_VERSION_OUTPUT
            ERROR_VARIABLE ${name}_VERSION_ERROR
            OUTPUT_STRIP_TRAILING_WHITESPACE
            ERROR_STRIP_TRAILING_WHITESPACE
        )
        
        string(REGEX MATCH "${ver_regex}" ${name}_VERSION_MATCH "${${name}_VERSION_OUTPUT}${${name}_VERSION_ERROR}")
        if(${name}_VERSION_MATCH)
            string(REGEX REPLACE "${ver_regex}" "\\1" ${name}_VERSION "${${name}_VERSION_MATCH}")
            if(${name}_VERSION VERSION_GREATER_EQUAL ${version})
                set(${name}_FOUND TRUE)
                message(STATUS "${Green}Found ${prog} version ${${name}_VERSION}${ColorReset}")
            else()
                message(WARNING "${Yellow}${prog} version ${${name}_VERSION} found, but ${version} or higher required${ColorReset}")
            endif()
        endif()
    endif()
endmacro()