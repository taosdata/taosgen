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

#include "TableNameConfig.hpp"
#include "TagsConfig.hpp"
#include "ColumnsConfig.hpp"
#include "CheckpointInfo.hpp"
#include "DatabaseInfo.hpp"
#include "SuperTableInfo.hpp"
#include "DataFormat.hpp"
#include "DataChannel.hpp"
#include "SchemaConfig.hpp"
#include "PluginExtensions.hpp"
#include <string>
#include <vector>

struct InsertDataConfig {
    SchemaConfig schema;
    PluginExtensions extensions;

    std::string target_type = "tdengine";
    std::string timestamp_precision = "ms";
    DataFormat data_format;

    size_t insert_threads = 8;
    size_t queue_capacity = 10;
    double queue_warmup_ratio = 0.0;
    bool shared_queue = false;
    std::string thread_allocation = "index_range";
    bool thread_affinity = false;
    bool thread_realtime = false;

    struct FailureHandling {
        size_t max_retries = 0;
        int retry_interval_ms = 1000;
        std::string on_failure = "exit";            // exit or warn_and_continue
    } failure_handling;

    struct TimeInterval {
        bool enabled = false;                       // Enable interval control
        std::string interval_strategy = "fixed";    // Time interval strategy type
        std::string wait_strategy = "sleep";        // Wait strategy type: sleep or busy_wait

        struct FixedInterval {
            int base_interval = 1000;               // Fixed interval value in milliseconds
            int random_deviation = 0;               // Random deviation
        } fixed_interval;

        struct DynamicInterval {
            int min_interval = -1;                  // Minimum interval threshold
            int max_interval = -1;                  // Maximum interval threshold
        } dynamic_interval;
    } time_interval;

    CheckpointInfo checkpoint_info;
};