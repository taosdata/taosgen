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

#include "ColumnType.hpp"
#include <limits>

struct RowData {
    int64_t timestamp;
    RowType columns;
};

struct TableData {
    std::string table_name;
    std::vector<int64_t> timestamps;
    std::vector<RowType> rows;
};

struct MultiBatch {
    std::vector<std::pair<std::string, std::vector<RowData>>> table_batches;
    int64_t start_time{0};
    int64_t end_time{0};
    size_t total_rows{0};

    MultiBatch() = default;
    MultiBatch(MultiBatch&& other) noexcept
        : table_batches(std::move(other.table_batches))
        , start_time(other.start_time)
        , end_time(other.end_time)
        , total_rows(other.total_rows)
    {
        other.start_time = 0;
        other.end_time = 0;
        other.total_rows = 0;
    }

    MultiBatch& operator=(MultiBatch&& other) noexcept {
        if (this != &other) {
            table_batches = std::move(other.table_batches);
            start_time = other.start_time;
            end_time = other.end_time;
            total_rows = other.total_rows;

            other.start_time = 0;
            other.end_time = 0;
            other.total_rows = 0;
        }
        return *this;
    }

    void reserve(size_t table_count) {
        table_batches.reserve(table_count);
    }

    void update_metadata() {
        start_time = std::numeric_limits<int64_t>::max();
        end_time = std::numeric_limits<int64_t>::min();
        total_rows = 0;

        bool has_data = false;

        for (const auto& [table_name, rows] : table_batches) {
            (void)table_name;
            for (const auto& row : rows) {
                start_time = std::min(start_time, row.timestamp);
                end_time = std::max(end_time, row.timestamp);
                ++total_rows;
                has_data = true;
            }
        }

        if (!has_data) {
            start_time = 0;
            end_time = 0;
            total_rows = 0;
        }
    }
};