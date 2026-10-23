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

#include "TDengineConfig.hpp"

#include <vector>
#include <stdexcept>
#include <algorithm>
#include <regex>
#include "StringUtils.hpp"

TDengineConfig::TDengineConfig() {
    init();
}

TDengineConfig::TDengineConfig(std::string input_dsn) : dsn(input_dsn) {
    init();
}

void TDengineConfig::init() {
    parse_dsn();
}

void TDengineConfig::parse_dsn() {
    // 1. Check protocol "://"
    const size_t protocol_pos = dsn.find("://");
    if (protocol_pos == std::string::npos) {
        throw std::runtime_error("DSN invalid: missing '://' protocol separator");
    }

    std::string protocol = dsn.substr(0, protocol_pos);
    if (protocol == "taos") {
        protocol_type = ProtocolType::Native;
    } else if (protocol == "taos+ws") {
        protocol_type = ProtocolType::WebSocket;
    } else {
        throw std::runtime_error("DSN invalid: unsupported protocol '" + protocol + "'");
    }

    // 2. Extract user:password@host:port part (after protocol)
    const std::string post_protocol = dsn.substr(protocol_pos + 3);
    const size_t path_start = post_protocol.find('/');
    const std::string user_host_port = post_protocol.substr(0, path_start);

    // 3. Parse user:password@host:port
    size_t at_pos = user_host_port.find('@');
    std::string host_port;
    if (at_pos != std::string::npos) {
        // user:password present
        std::string user_pass = user_host_port.substr(0, at_pos);
        host_port = user_host_port.substr(at_pos + 1);

        size_t colon_pos = user_pass.find(':');
        if (colon_pos != std::string::npos) {
            user = user_pass.substr(0, colon_pos);
            password = user_pass.substr(colon_pos + 1);
        } else {
            user = user_pass;
            password.clear();
        }
    } else {
        // no user:password
        host_port = user_host_port;
    }

    // 4. Parse host and port
    size_t colon_pos = host_port.find(':');
    host = host_port.substr(0, colon_pos);
    if (colon_pos != std::string::npos) {
        const std::string port_str = host_port.substr(colon_pos + 1);
        try {
            port = std::stoi(port_str);
            if (port <= 0 || port > 65535) {
                throw std::runtime_error("Invalid port number: " + port_str);
            }
        } catch (const std::exception& e) {
            throw std::runtime_error("Port parse error: " + std::string(e.what()));
        }
    } else {
        // No port specified, use default
        if (protocol_type == ProtocolType::Native) {
            port = 6030;
        } else if (protocol_type == ProtocolType::WebSocket) {
            port = 6041;
        }
    }

    // 5. Parse database name
    if (path_start != std::string::npos) {
        database = post_protocol.substr(path_start + 1);
    }
}

bool TDengineConfig::is_valid_precision(const std::string& value) {
    return value == "ms" || value == "us" || value == "ns";
}

void TDengineConfig::set_precision(const std::string& value) {
    std::string lower = StringUtils::to_lower(value);
    if (!is_valid_precision(lower)) {
        throw std::runtime_error("Invalid TDengine database precision: " + value);
    }
    precision = lower;
}

void TDengineConfig::apply_properties_precision() {
    if (!properties.has_value()) {
        return;
    }

    static const std::regex precision_regex(R"(\bprecision\s+['"]?(ms|us|ns)['"]?)",
                                            std::regex_constants::icase);
    std::smatch match;
    if (std::regex_search(properties.value(), match, precision_regex)) {
        set_precision(match[1].str());
    }
}
