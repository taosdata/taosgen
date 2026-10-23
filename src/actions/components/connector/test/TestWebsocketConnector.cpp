/*
 * Copyright (c) 2025 TAOS Data, Inc.
 *
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#include "ConnectorFactory.hpp"
#include "WebsocketConnector.hpp"
#include <iostream>

void test_websocket_connector_create_database() {
    // Set connection info
    TDengineConfig conn_info;
    conn_info.database.clear();

    // Create connector instance using ConnectorFactory::create
    auto connector = ConnectorFactory::create(conn_info);

    // Test connection
    if (!connector->connect()) {
        std::cerr << "Failed to connect to TDengine." << std::endl;
        return;
    }
    std::cout << "Connected to TDengine successfully." << std::endl;

    // Test SQL execution
    std::string sql = "DROP DATABASE IF EXISTS `test_connector`";
    if (!connector->execute(sql)) {
        std::cerr << "Failed to execute SQL: " << sql << std::endl;
        connector->close();
        return;
    }
    std::cout << "SQL executed successfully: " << sql << std::endl;

    sql = "CREATE DATABASE IF NOT EXISTS `test_connector`";
    if (!connector->execute(sql)) {
        std::cerr << "Failed to execute SQL: " << sql << std::endl;
        connector->close();
        return;
    }
    std::cout << "SQL executed successfully: " << sql << std::endl;

    sql = "DROP DATABASE IF EXISTS `test_connector`";
    if (!connector->execute(sql)) {
        std::cerr << "Failed to execute SQL: " << sql << std::endl;
        connector->close();
        return;
    }
    std::cout << "SQL executed successfully: " << sql << std::endl;

    // Close connection
    connector->close();
    std::cout << "Connection closed successfully." << std::endl;
}

int main() {
    std::cout << "Running WebsocketConnector tests..." << std::endl;

    // Call test function
    test_websocket_connector_create_database();

    std::cout << "WebsocketConnector tests completed." << std::endl;
    return 0;
}