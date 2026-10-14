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
#include "InfluxDBConfig.hpp"
#include <yaml-cpp/yaml.h>
#include <set>

namespace YAML {
    template<>
    struct convert<InfluxDBConfig> {
        static bool decode(const Node& node, InfluxDBConfig& rhs) {
            static const std::set<std::string> valid_keys = {
                "url", "token", "org", "bucket"
            };
            check_unknown_keys(node, valid_keys, "influxdb config");

            if (node["url"]) {
                rhs.url = node["url"].as<std::string>();
            }
            if (node["token"]) {
                rhs.token = node["token"].as<std::string>();
            }
            if (node["org"]) {
                rhs.org = node["org"].as<std::string>();
            }
            if (node["bucket"]) {
                rhs.bucket = node["bucket"].as<std::string>();
            }

            rhs.enabled = true;
            return true;
        }
    };
} // namespace YAML
