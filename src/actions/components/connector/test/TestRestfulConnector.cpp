/*
 * Copyright (c) 2026 TAOS Data, Inc.
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

#include "RestfulConnector.hpp"
#include <iostream>
#include <cassert>
#include <stdexcept>
#include <string>

void test_restful_constructor() {
    TDengineConfig cfg;
    cfg.host = "localhost";
    cfg.port = 6041;

    RestfulConnector connector(cfg);
    std::cout << "test_restful_constructor PASSED\n";
}

void test_restful_destructor_no_crash() {
    {
        TDengineConfig cfg;
        RestfulConnector connector(cfg);
    }  // destructor calls close(), should be a safe no-op
    std::cout << "test_restful_destructor_no_crash PASSED\n";
}

void test_restful_close_no_crash() {
    TDengineConfig cfg;
    RestfulConnector connector(cfg);
    connector.close();  // explicit close should be safe no-op
    std::cout << "test_restful_close_no_crash PASSED\n";
}

void test_restful_reset_state_no_crash() {
    TDengineConfig cfg;
    RestfulConnector connector(cfg);
    connector.reset_state();  // should be safe no-op
    std::cout << "test_restful_reset_state_no_crash PASSED\n";
}

void test_restful_connect_throws() {
    TDengineConfig cfg;
    RestfulConnector connector(cfg);

    try {
        connector.connect();
        assert(false && "Should throw");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("not implemented") != std::string::npos);
    }
    std::cout << "test_restful_connect_throws PASSED\n";
}

void test_restful_is_connected_throws() {
    TDengineConfig cfg;
    RestfulConnector connector(cfg);

    try {
        connector.is_connected();
        assert(false && "Should throw");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("not implemented") != std::string::npos);
    }
    std::cout << "test_restful_is_connected_throws PASSED\n";
}

void test_restful_is_valid_throws() {
    TDengineConfig cfg;
    RestfulConnector connector(cfg);

    try {
        connector.is_valid();
        assert(false && "Should throw");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("not implemented") != std::string::npos);
    }
    std::cout << "test_restful_is_valid_throws PASSED\n";
}

void test_restful_select_db_throws() {
    TDengineConfig cfg;
    RestfulConnector connector(cfg);

    try {
        connector.select_db("test_db");
        assert(false && "Should throw");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("not implemented") != std::string::npos);
    }
    std::cout << "test_restful_select_db_throws PASSED\n";
}

void test_restful_prepare_throws() {
    TDengineConfig cfg;
    RestfulConnector connector(cfg);

    try {
        connector.prepare("SELECT 1");
        assert(false && "Should throw");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("not implemented") != std::string::npos);
    }
    std::cout << "test_restful_prepare_throws PASSED\n";
}

void test_restful_execute_sql_throws() {
    TDengineConfig cfg;
    RestfulConnector connector(cfg);

    try {
        connector.execute(std::string("SELECT 1"));
        assert(false && "Should throw");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("not implemented") != std::string::npos);
    }
    std::cout << "test_restful_execute_sql_throws PASSED\n";
}

void test_restful_execute_sql_insert_data_throws() {
    TDengineConfig cfg;
    RestfulConnector connector(cfg);

    SqlInsertData data(std::string("INSERT INTO test VALUES(1)"));
    try {
        connector.execute(data);
        assert(false && "Should throw");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("not implemented") != std::string::npos);
    }
    std::cout << "test_restful_execute_sql_insert_data_throws PASSED\n";
}

void test_restful_execute_schemaless_throws() {
    TDengineConfig cfg;
    RestfulConnector connector(cfg);

    SchemalessInsertData data;
    data.lines = "cpu,host=h1 usage=50.0 1000000000";
    data.total_rows = 1;
    try {
        connector.execute(data);
        assert(false && "Should throw");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("not supported") != std::string::npos ||
               std::string(e.what()).find("not implemented") != std::string::npos);
    }
    std::cout << "test_restful_execute_schemaless_throws PASSED\n";
}

int main() {
    std::cout << "Running RestfulConnector tests..." << std::endl;

    test_restful_constructor();
    test_restful_destructor_no_crash();
    test_restful_close_no_crash();
    test_restful_reset_state_no_crash();
    test_restful_connect_throws();
    test_restful_is_connected_throws();
    test_restful_is_valid_throws();
    test_restful_select_db_throws();
    test_restful_prepare_throws();
    test_restful_execute_sql_throws();
    test_restful_execute_sql_insert_data_throws();
    test_restful_execute_schemaless_throws();

    std::cout << "\nAll RestfulConnector tests PASSED\n";
    return 0;
}
