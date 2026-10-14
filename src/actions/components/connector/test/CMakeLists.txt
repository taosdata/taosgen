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

# Test TDengineConnector
add_executable(TestTDengineConnector
  TestTDengineConnector.cpp
)
target_link_libraries(TestTDengineConnector
  PRIVATE
    components_connector
)
add_test(NAME TestTDengineConnector COMMAND TestTDengineConnector)

# Test NativeConnector
add_executable(TestNativeConnector
  TestNativeConnector.cpp
)
target_link_libraries(TestNativeConnector
  PRIVATE
    components_connector
)
add_test(NAME TestNativeConnector COMMAND TestNativeConnector)

# Test WebsocketConnector
add_executable(TestWebsocketConnector
  TestWebsocketConnector.cpp
)
target_link_libraries(TestWebsocketConnector
  PRIVATE
    components_connector
)
add_test(NAME TestWebsocketConnector COMMAND TestWebsocketConnector)

# Test ConnectionPoolImpl
add_executable(TestConnectionPoolImpl
  TestConnectionPoolImpl.cpp
)
target_link_libraries(TestConnectionPoolImpl
  PRIVATE
    components_connector
)
add_test(NAME TestConnectionPoolImpl COMMAND TestConnectionPoolImpl)

# Test ConnectorSource
add_executable(TestConnectorSource
  TestConnectorSource.cpp
)
target_link_libraries(TestConnectorSource
  PRIVATE
    components_connector
)
add_test(NAME TestConnectorSource COMMAND TestConnectorSource)

# Test PooledConnector
add_executable(TestPooledConnector
  TestPooledConnector.cpp
)
target_link_libraries(TestPooledConnector
  PRIVATE
    components_connector
)
add_test(NAME TestPooledConnector COMMAND TestPooledConnector)

# Test RestfulConnector
add_executable(TestRestfulConnector
  TestRestfulConnector.cpp
)
target_link_libraries(TestRestfulConnector
  PRIVATE
    components_connector
)
add_test(NAME TestRestfulConnector COMMAND TestRestfulConnector)