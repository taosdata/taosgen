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

#include "ConnectorSource.hpp"
#include <cassert>
#include <iostream>

void test_connector_source_with_pool() {
    TDengineConfig info;
    info.database.clear();
    info.pool.enabled = true;
    info.pool.min_size = 2;
    info.pool.max_size = 4;
    info.pool.timeout = 100;

    ConnectorSource source(info);

    assert(source.total_connections() == info.pool.min_size);
    assert(source.available_connections() == info.pool.min_size);
    assert(source.active_connections() == 0);

    auto conn = source.get_connector();
    assert(conn != nullptr);
    assert(source.available_connections() == info.pool.min_size - 1);
    assert(source.active_connections() == 1);

    conn->close();

    std::cout << "test_connector_source_with_pool passed\n";
}

void test_connector_source_without_pool() {
    TDengineConfig info;
    info.database.clear();
    info.pool.enabled = false;

    ConnectorSource source(info);

    assert(source.total_connections() == 0);
    assert(source.available_connections() == 0);
    assert(source.active_connections() == 0);

    auto conn = source.get_connector();
    assert(conn != nullptr);

    std::cout << "test_connector_source_without_pool passed\n";
}

int main() {
    test_connector_source_with_pool();
    test_connector_source_without_pool();

    std::cout << "All ConnectorSource tests passed!" << std::endl;
    return 0;
}