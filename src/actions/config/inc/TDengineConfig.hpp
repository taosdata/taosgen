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

#include "ISinkConfig.hpp"
#include <string>
#include <optional>

struct TDengineConfig : ISinkConfig {
    bool enabled = false;
    std::string dsn = "taos+ws://root:taosdata@localhost:6041/tsbench";
    std::string host = "localhost";
    int port = 6041;
    std::string user = "root";
    std::string password = "taosdata";
    std::string database = "tsbench";
    std::string precision = "ms";
    bool drop_if_exists = false;
    std::optional<std::string> properties;

    struct ConnectionPool {
        bool enabled = true;
        size_t max_size = 100;
        size_t min_size = 2;
        size_t timeout = 1000;       // unit: ms
    } pool;

    enum class ProtocolType {
        Native,
        WebSocket,
        RESTful
    } protocol_type = ProtocolType::WebSocket;

    TDengineConfig();
    TDengineConfig(std::string input_dsn);

    void init();
    void parse_dsn();
    static bool is_valid_precision(const std::string& value);
    void set_precision(const std::string& value);
    void apply_properties_precision();

    std::string get_sink_info() const override {
        return "TDengine(" + host + ":" + std::to_string(port) + ")";
    }

    std::string get_sink_type() const override { return "TDengine"; }

    bool is_enabled() const override { return enabled; }
};
