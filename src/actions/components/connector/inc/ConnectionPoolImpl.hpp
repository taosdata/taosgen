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

#include "DataChannel.hpp"
#include "DatabaseConnector.hpp"
#include <memory>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <atomic>


class ConnectionPoolImpl {
public:
    ConnectionPoolImpl(const TDengineConfig& conn_info);
    ~ConnectionPoolImpl();

    std::unique_ptr<DatabaseConnector> get_connector();
    void return_connection(std::unique_ptr<DatabaseConnector> conn);

    size_t total_connections() const;
    size_t available_connections() const;
    size_t active_connections() const;

private:
    void initialize();
    void create_connections_locked(size_t count);
    void create_connections(size_t count);
    void close_all_connections();

    const TDengineConfig& conn_info_;

    std::queue<std::unique_ptr<DatabaseConnector>> available_connections_;
    std::atomic<size_t> total_count_{0};

    mutable std::mutex mutex_;
    std::condition_variable condition_;
    bool is_shutting_down_ = false;
};