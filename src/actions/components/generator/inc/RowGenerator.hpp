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

#include <vector>
#include <memory>
#include <random>
#include "TimestampGenerator.hpp"
#include "ColumnGenerator.hpp"


class RowGenerator {
public:

    explicit RowGenerator(const ColumnConfigInstanceVector& col_instances);
    RowGenerator(const std::string& table_name, const ColumnConfigInstanceVector& col_instances);

    RowGenerator(const TimestampGeneratorConfig& ts_config, const ColumnConfigInstanceVector& col_instances);

    RowType generate() const;

    void generate(RowType& columns) const;

    std::vector<RowType> generate(size_t count) const;

private:
    void apply_null_none(RowType& row, size_t col_offset) const;

    std::string table_name_;
    std::unique_ptr<TimestampGenerator> timestamp_gen_;
    std::vector<std::unique_ptr<ColumnGenerator>> column_gens_;

    struct NullNoneRatio {
        float null_ratio = 0.0f;
        float none_ratio = 0.0f;
        bool has_ratio() const { return null_ratio > 0.0f || none_ratio > 0.0f; }
    };
    std::vector<NullNoneRatio> ratios_;
    bool has_any_ratio_ = false;
};