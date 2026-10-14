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
#include "ConnectorFactory.hpp"
#include "PooledConnector.hpp"
#include <iostream>
#include <stdexcept>

ConnectorSource::ConnectorSource(const TDengineConfig& conn_info) : conn_info_(conn_info)
{
    if (conn_info_.pool.enabled) {
        pool_impl_ = std::make_unique<ConnectionPoolImpl>(conn_info);
    }
}

ConnectorSource::~ConnectorSource() {
}

std::unique_ptr<DatabaseConnector> ConnectorSource::get_connector() {
    if (conn_info_.pool.enabled && pool_impl_) {
        // Get connection from the connection pool
        return pool_impl_->get_connector();
    }

    // Create a new connection
    auto conn = create_raw_connection();
    if (conn->connect()) {
        return conn;
    }
    throw std::runtime_error("Failed to connect to database");
}

std::unique_ptr<DatabaseConnector> ConnectorSource::create_raw_connection() const {
    return ConnectorFactory::create(conn_info_);
}

size_t ConnectorSource::total_connections() const {
    return pool_impl_ ? pool_impl_->total_connections() : 0;
}

size_t ConnectorSource::available_connections() const {
    return pool_impl_ ? pool_impl_->available_connections() : 0;
}

size_t ConnectorSource::active_connections() const {
    return pool_impl_ ? pool_impl_->active_connections() : 0;
}