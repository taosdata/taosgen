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

# test CreateDatabaseAction
add_executable(TestCreateDatabaseAction
  TestCreateDatabaseAction.cpp
)
target_link_libraries(TestCreateDatabaseAction
  PRIVATE 
    core_create
    ${TAOS_LIBRARIES}
)
add_test(NAME TestCreateDatabaseAction COMMAND TestCreateDatabaseAction)

# test CreateSuperTableAction
add_executable(TestCreateSuperTableAction
  TestCreateSuperTableAction.cpp
)
target_link_libraries(TestCreateSuperTableAction
  PRIVATE 
    core_create
    ${TAOS_LIBRARIES}
)
add_test(NAME TestCreateSuperTableAction COMMAND TestCreateSuperTableAction)

# test CreateChildTableAction
add_executable(TestCreateChildTableAction
  TestCreateChildTableAction.cpp
)
target_link_libraries(TestCreateChildTableAction
  PRIVATE
    core_create
    ${TAOS_LIBRARIES}
)
add_test(NAME TestCreateChildTableAction COMMAND TestCreateChildTableAction)