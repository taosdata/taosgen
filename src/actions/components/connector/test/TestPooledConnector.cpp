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

#include "PooledConnector.hpp"
#include <cassert>
#include <iostream>
#include <memory>
#include <stdexcept>

struct DummyConnector : public DatabaseConnector {
    bool valid = true;
    bool connected = true;
    bool connect() override { return true; }
    void close() noexcept override {}
    bool select_db(const std::string&) override { return true; }
    bool prepare(const std::string&) override { return true; }
    bool execute(const std::string&) override { return true; }
    bool execute(const SqlInsertData&) override { return true; }
    bool execute(const StmtV2InsertData&) override { return true; }
    bool execute(const SchemalessInsertData&) override { return true; }
    bool is_connected() const override { return connected; }
    bool is_valid() const override { return valid; }
    void reset_state() noexcept override {}
    ~DummyConnector() override = default;
};

namespace ConnectorFactory {
    std::unique_ptr<DatabaseConnector> create(const DataChannel&, const TDengineConfig&) {
        return std::make_unique<DummyConnector>();
    }
}

void test_pooled_connector_constructor_valid() {
    TDengineConfig info;
    ConnectionPoolImpl pool(info);
    auto real_conn = std::make_unique<DummyConnector>();
    PooledConnector pooled(std::move(real_conn), pool);
    // No exception, object constructed
    assert(true);
    std::cout << "test_pooled_connector_constructor_valid passed\n";
}

void test_pooled_connector_constructor_nullptr() {
    TDengineConfig info;
    ConnectionPoolImpl pool(info);
    bool exception_thrown = false;
    try {
        PooledConnector pooled(nullptr, pool);
    } catch (const std::invalid_argument&) {
        exception_thrown = true;
    }
    (void)exception_thrown;
    assert(exception_thrown);
    std::cout << "test_pooled_connector_constructor_nullptr passed\n";
}

int main() {
    test_pooled_connector_constructor_valid();
    test_pooled_connector_constructor_nullptr();
    std::cout << "All PooledConnector constructor tests passed!" << std::endl;
    return 0;
}