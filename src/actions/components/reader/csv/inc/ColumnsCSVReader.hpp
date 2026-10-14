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
#include <string>
#include <unordered_map>
#include <variant>
#include <optional>
#include "TableData.hpp"
#include "ColumnsCSV.hpp"
#include "CSVReader.hpp"
#include "TimestampGenerator.hpp"
#include "TimestampCSVConfig.hpp"
#include "ColumnConfigInstance.hpp"

// Default table name used when tbname_index is -1 (no table name column in CSV)
constexpr const char* DEFAULT_TABLE_NAME = "default_table";

class ColumnsCSVReader {
public:
    ColumnsCSVReader(const ColumnsCSV& config, std::optional<ColumnConfigInstanceVector> instances);

    ColumnsCSVReader(const ColumnsCSVReader&) = delete;
    ColumnsCSVReader& operator=(const ColumnsCSVReader&) = delete;
    ColumnsCSVReader(ColumnsCSVReader&&) = delete;
    ColumnsCSVReader& operator=(ColumnsCSVReader&&) = delete;

    ~ColumnsCSVReader() = default;

    std::unordered_map<std::string, TableData> generate() const;

private:
    ColumnsCSV config_;
    std::optional<ColumnConfigInstanceVector> instances_;
    std::vector<std::string> resolved_paths_;
    size_t total_columns_ = 0;
    size_t actual_columns_ = 0;

    void validate_config();

    template <typename T>
    T convert_value(const std::string& value) const;

    ColumnType convert_to_type(const std::string& value, ColumnTypeTag target_type) const;
};
