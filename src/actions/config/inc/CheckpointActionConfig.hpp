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

#include <cstdint>
#include "InsertDataConfig.hpp"

struct CheckpointActionConfig {
    bool enabled = false;
    size_t interval_sec = 60; // Checkpoint interval in seconds
    int64_t start_timestamp = 0;
    int tableCount = 0;
    std::string timestamp_precision = "ms";
    int timestamp_step = 1;

    CheckpointActionConfig() = default;
    CheckpointActionConfig(const InsertDataConfig& config){
        enabled = config.checkpoint_info.enabled;
        interval_sec = config.checkpoint_info.interval_sec;
        if (enabled) {
            TimestampGeneratorConfig ts_config = config.schema.columns_cfg.generator.timestamp_strategy.timestamp_config;
            this->start_timestamp = TimestampUtils::parse_timestamp(ts_config.start_timestamp, ts_config.timestamp_precision);
            this->timestamp_step = static_cast<int>(std::get<Timestamp>(ts_config.timestamp_step));
            this->timestamp_precision = ts_config.timestamp_precision;
            this->tableCount = config.schema.tbname.generator.count - config.schema.tbname.generator.from;
        }
    }
};
