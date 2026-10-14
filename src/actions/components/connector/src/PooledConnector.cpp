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

PooledConnector::PooledConnector(
    std::unique_ptr<DatabaseConnector> real_conn,
    ConnectionPoolImpl& pool)
    : real_conn_(std::move(real_conn)), pool_(pool)
{
    if (!real_conn_) {
        throw std::invalid_argument("Real connection cannot be null");
    }
}

PooledConnector::~PooledConnector() {
    if (!is_closed_) {
        close();
    }
}

bool PooledConnector::connect() {
    return true;
}

void PooledConnector::close() noexcept {
    if (!is_closed_) {
        is_closed_ = true;
        pool_.return_connection(std::move(real_conn_));
    }
}

bool PooledConnector::select_db(const std::string& db_name) {
    return real_conn_->select_db(db_name);
}

bool PooledConnector::prepare(const std::string& sql) {
    return real_conn_->prepare(sql);
}

bool PooledConnector::execute(const std::string& sql) {
    return real_conn_->execute(sql);
}

bool PooledConnector::execute(const SqlInsertData& data) {
    return real_conn_->execute(data);
}

bool PooledConnector::execute(const StmtV2InsertData& data) {
    return real_conn_->execute(data);
}

bool PooledConnector::execute(const SchemalessInsertData& data) {
    return real_conn_->execute(data);
}

bool PooledConnector::is_connected() const {
    return real_conn_->is_connected();
}

bool PooledConnector::is_valid() const {
    return real_conn_->is_valid();
}

void PooledConnector::reset_state() noexcept {
    real_conn_->reset_state();
}

const ConnectorError& PooledConnector::last_error() const noexcept {
    if (real_conn_) {
        return real_conn_->last_error();
    }
    return DatabaseConnector::last_error();
}
