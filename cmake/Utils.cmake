# Copyright (c) 2025 TAOS Data, Inc.
#
# SPDX-License-Identifier: MIT
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

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
macro(check_program_version name version prog ver_arg ver_regex var_type)
  find_program(${name} NO_CACHE NAMES ${prog})
  if (${name})
    set(${name}_NAME ${prog})
    if ("${var_type}" STREQUAL "OUTPUT_VARIABLE")
      execute_process(COMMAND ${prog} ${ver_arg} ERROR_QUIET OUTPUT_STRIP_TRAILING_WHITESPACE OUTPUT_VARIABLE ${name}_VERSION)
    else ()
      execute_process(COMMAND ${prog} ${ver_arg} OUTPUT_QUIET ERROR_STRIP_TRAILING_WHITESPACE ERROR_VARIABLE ${name}_VERSION)
    endif ()
    if(NOT "${${name}_VERSION}" STREQUAL "")
      string(REGEX REPLACE ${ver_regex} "\\1" ${name}_VERSION_ONLY ${${name}_VERSION})

      if(NOT "${${name}_VERSION_ONLY}" STREQUAL "")
        if (${${name}_VERSION_ONLY} VERSION_GREATER_EQUAL ${version})
          set(HAVE_${name} ON)
          message(STATUS "${Green}${${name}_VERSION} found: `${${name}}`, please be noted, ${prog} v${version} and above are expected compatible${ColorReset}")
        endif()
      endif()
    endif()
  endif ()
endmacro()

# Check requirements
macro(check_requirements)
  ## check `git`
  check_program_version(GIT 2.0.0 git "--version" "git version (.*)" OUTPUT_VARIABLE)
  if (NOT HAVE_GIT)
    message(STATUS "${Yellow}"
                   "`git 2.0.0 or above` not found, git info is unavailable"
                   "${ColorReset}")
  endif ()
endmacro()