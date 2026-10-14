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
#include "KafkaConfig.hpp"
#include <yaml-cpp/yaml.h>
#include <set>
#include <stdexcept>

namespace YAML {
    template<>
    struct convert<KafkaConfig> {
        static bool decode(const Node& node, KafkaConfig& rhs) {
            // Detect unknown configuration keys
            static const std::set<std::string> valid_keys = {
                "topic", "bootstrap_servers", "client_id", "rdkafka_options"
            };
            check_unknown_keys(node, valid_keys, "kafka config");

            if (node["topic"]) {
                rhs.topic = node["topic"].as<std::string>();
            }

            if (node["bootstrap_servers"]) {
                rhs.bootstrap_servers = node["bootstrap_servers"].as<std::string>();
            }

            if (node["client_id"]) {
                rhs.client_id = node["client_id"].as<std::string>();
            }

            if (node["rdkafka_options"]) {
                if (node["rdkafka_options"].IsMap()) {
                    for (const auto& it : node["rdkafka_options"]) {
                        rhs.rdkafka_options[it.first.as<std::string>()] = it.second.as<std::string>();
                    }
                } else {
                    throw std::runtime_error("rdkafka_options must be a map of string key-value pairs in kafka config.");
                }
            }

            rhs.enabled = true;
            return true;
        }
    };
} // namespace YAML