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

struct MqttConfig : ISinkConfig {
    bool enabled = false;
    std::string uri = "tcp://localhost:1883";
    std::string host = "localhost";
    int port = 1883;
    std::string user = "root";
    std::string password = "taosdata";
    std::string client_id = "taosgen";

    bool clean_session = true;
    size_t keep_alive = 120;
    size_t max_buffered_messages = 10000;

    void update_uri_from_host_port() {
        uri = "tcp://" + host + ":" + std::to_string(port);
    }

    std::string generate_client_id(size_t no = 0) const {
        return client_id + "-" + std::to_string(no);
    }

    MqttConfig() = default;

    MqttConfig(
        std::string uri_,
        std::string user_,
        std::string password_
    )
        : uri(std::move(uri_))
        , user(std::move(user_))
        , password(std::move(password_))
    {}

    std::string get_sink_info() const override {
        return "MQTT(" + uri + ")";
    }

    std::string get_sink_type() const override { return "MQTT"; }

    bool is_enabled() const override { return enabled; }

private:
    std::string topic_;
};
