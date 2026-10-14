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
#include <string>
#include <variant>
#include <vector>
#include <optional>

struct GenerationConfig {
    struct InterlaceMode {
        bool enabled = false;
        size_t rows = 1;
    } interlace_mode;

    struct DataCache {
        bool enabled = true;
        size_t num_cached_batches = 10000;
    } data_cache;

    struct FlowControl {
        bool enabled = false;
        int64_t rate_limit = 0;
    } flow_control;

    struct DataDisorder {
        bool enabled = false;

        struct Interval {
            std::variant<int64_t, std::string> time_start;
            std::variant<int64_t, std::string> time_end;
            double ratio = 0.0;
            int latency_range = 0;
        };
        std::vector<Interval> intervals;
    } data_disorder;

    std::optional<size_t> generate_threads;
    int64_t rows_per_table = 10000;
    size_t rows_per_batch = 10000;
    bool tables_reuse_data = true;

    GenerationConfig() {
        if (data_cache.enabled) {
            size_t batches_needed = 0;
            if (interlace_mode.enabled) {
                batches_needed = (static_cast<size_t>(rows_per_table) + interlace_mode.rows - 1) / interlace_mode.rows;
            } else {
                batches_needed = (static_cast<size_t>(rows_per_table) + rows_per_batch - 1) / rows_per_batch;
            }

            data_cache.num_cached_batches = std::min<size_t>(batches_needed, data_cache.num_cached_batches);
        }
    }
};