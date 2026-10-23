#pragma once

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

#include "DatabaseConnector.hpp"
#include "ConnectionPoolImpl.hpp"
#include <memory>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <atomic>

class ConnectorSource {
public:
    ConnectorSource(const TDengineConfig& conn_info);
    ~ConnectorSource();

    std::unique_ptr<DatabaseConnector> get_connector();

    size_t total_connections() const;
    size_t available_connections() const;
    size_t active_connections() const;

private:
    std::unique_ptr<ConnectionPoolImpl> pool_impl_;

    std::unique_ptr<DatabaseConnector> create_raw_connection() const;

    const TDengineConfig& conn_info_;

    ConnectorSource(const ConnectorSource&) = delete;
    ConnectorSource& operator=(const ConnectorSource&) = delete;
};