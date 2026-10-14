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

#include "RestfulConnector.hpp"
#include "LogUtils.hpp"
#include <iostream>


RestfulConnector::RestfulConnector(const TDengineConfig& conn_info)
    : conn_info_(conn_info) {}

RestfulConnector::~RestfulConnector() {
    close();
}

bool RestfulConnector::connect() {
    throw std::runtime_error("RestfulConnector::connect is not implemented.");
}

bool RestfulConnector::is_connected() const {
    throw std::runtime_error("RestfulConnector::is_connected is not implemented.");
}

bool RestfulConnector::is_valid() const {
    throw std::runtime_error("RestfulConnector::is_valid is not implemented.");
}

bool RestfulConnector::select_db(const std::string& db_name) {
    (void)db_name;
    throw std::runtime_error("RestfulConnector::select_db is not implemented.");
}

bool RestfulConnector::prepare(const std::string& sql) {
    (void)sql;
    throw std::runtime_error("RestfulConnector::prepare is not implemented.");
}

bool RestfulConnector::execute(const std::string& sql) {
    (void)sql;
    throw std::runtime_error("RestfulConnector::execute is not implemented.");
}

bool RestfulConnector::execute(const SqlInsertData& data) {
    (void)data;
    throw std::runtime_error("RestfulConnector::execute is not implemented.");
}

bool RestfulConnector::execute(const StmtV2InsertData& data) {
    (void)data;
    throw std::runtime_error("RestfulConnector::execute is not implemented.");
}

bool RestfulConnector::execute(const SchemalessInsertData& data) {
    (void)data;
    throw std::runtime_error("RestfulConnector::execute(SchemalessInsertData) is not supported. Schemaless insert requires native connection.");
}

void RestfulConnector::reset_state() noexcept {
    // no-op: stub class has no state to reset
}

void RestfulConnector::close() noexcept {
    // no-op: stub class has no connection to close
}