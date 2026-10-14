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

# Test CSVReader
add_executable(TestCSVReader
  TestCSVReader.cpp
)
target_link_libraries(TestCSVReader
  PRIVATE
    reader_csv
)
add_test(NAME TestCSVReader COMMAND TestCSVReader)

# Test FilePathResolver
add_executable(TestFilePathResolver
  TestFilePathResolver.cpp
)
target_link_libraries(TestFilePathResolver
  PRIVATE
    reader_csv
)
add_test(NAME TestFilePathResolver COMMAND TestFilePathResolver)

# Test TableNameCSVReader
add_executable(TestTableNameCSVReader
  TestTableNameCSVReader.cpp
)
target_link_libraries(TestTableNameCSVReader
  PRIVATE
    reader_csv
)
add_test(NAME TestTableNameCSVReader COMMAND TestTableNameCSVReader)

# Test TagsCSVReader
add_executable(TestTagsCSVReader
  TestTagsCSVReader.cpp
)
target_link_libraries(TestTagsCSVReader
  PRIVATE
    reader_csv
)
add_test(NAME TestTagsCSVReader COMMAND TestTagsCSVReader)

# Test ColumnsCSVReader
add_executable(TestColumnsCSVReader
  TestColumnsCSVReader.cpp
)
target_link_libraries(TestColumnsCSVReader
  PRIVATE
    reader_csv
)
add_test(NAME TestColumnsCSVReader COMMAND TestColumnsCSVReader)

# Test CSVDataManager
add_executable(TestCSVDataManager
  TestCSVDataManager.cpp
)
target_link_libraries(TestCSVDataManager
  PRIVATE
    reader_csv
)
add_test(NAME TestCSVDataManager COMMAND TestCSVDataManager)

# Test StreamingCSVRowSource
add_executable(TestStreamingCSVRowSource
  TestStreamingCSVRowSource.cpp
)
target_link_libraries(TestStreamingCSVRowSource
  PRIVATE
    reader_csv
)
add_test(NAME TestStreamingCSVRowSource COMMAND TestStreamingCSVRowSource)

# Test PreloadCSVRowSource
add_executable(TestPreloadCSVRowSource
  TestPreloadCSVRowSource.cpp
)
target_link_libraries(TestPreloadCSVRowSource
  PRIVATE
    reader_csv
)
add_test(NAME TestPreloadCSVRowSource COMMAND TestPreloadCSVRowSource)