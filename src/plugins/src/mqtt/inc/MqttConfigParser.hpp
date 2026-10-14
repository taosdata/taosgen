#pragma once

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

#include "ConfigParser.hpp"
#include "MqttConfig.hpp"
#include <yaml-cpp/yaml.h>
#include <set>
#include <stdexcept>

namespace YAML {
    template<>
    struct convert<MqttConfig> {
        static bool decode(const Node& node, MqttConfig& rhs) {
            // Detect unknown configuration keys
            static const std::set<std::string> valid_keys = {
                "uri", "user", "password", "client_id",
                "keep_alive", "clean_session", "max_buffered_messages"
            };
            check_unknown_keys(node, valid_keys, "mqtt config");

            if (node["uri"]) {
                rhs.uri = node["uri"].as<std::string>();
            }
            if (node["user"]) {
                rhs.user = node["user"].as<std::string>();
            }
            if (node["password"]) {
                rhs.password = node["password"].as<std::string>();
            }

            if (node["client_id"]) {
                rhs.client_id = node["client_id"].as<std::string>();
            }

            if (node["keep_alive"]) {
                rhs.keep_alive = node["keep_alive"].as<size_t>();
            }

            if (node["clean_session"]) {
                rhs.clean_session = node["clean_session"].as<bool>();
            }

            if (node["max_buffered_messages"]) {
                rhs.max_buffered_messages = node["max_buffered_messages"].as<size_t>();
                if (rhs.max_buffered_messages == 0) {
                    throw std::runtime_error("max_buffered_messages must be greater than 0 in mqtt config.");
                }
            }

            rhs.enabled = true;
            return true;
        }
    };
} // namespace YAML