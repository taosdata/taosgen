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

#include <string>
#include <vector>
#include <map>
#include <optional>
#include "TDengineConfig.hpp"
#include "DataFormat.hpp"
#include "DataChannel.hpp"

struct SubscribeDataConfig {
    struct Source {
        TDengineConfig connection_info; // Database connection info
    } source;

    struct Control {
        DataFormat data_format;   // Data format config
        DataChannel data_channel; // Data channel config

        struct SubscribeControl {
            std::string log_path = "result.txt"; // Log file path
            bool enable_dryrun = false;         // Enable dry run

            struct Execution {
                int consumer_concurrency = 1; // Number of concurrent consumers
                int poll_timeout = 1000;      // Poll timeout (ms)
            } execution;

            struct Topic {
                std::string name; // Topic name
                std::string sql;  // SQL to create topic
            };
            std::vector<Topic> topics; // Subscribed topics list

            struct Commit {
                std::string mode = "auto"; // Commit mode (auto or manual)
            } commit;

            struct GroupID {
                std::string strategy;       // Group ID strategy (shared, independent, custom)
                std::optional<std::string> custom_id; // Custom Group ID (required if strategy is custom)
            } group_id;

            struct Output {
                std::string path;         // Output file path
                std::string file_prefix;  // Output file prefix
                std::optional<int> expected_rows; // Expected rows per consumer
            } output;

            std::map<std::string, std::string> advanced; // Advanced config (key-value map)
        } subscribe_control;
    } control;
};
