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
#include "ColumnType.hpp"
#include "TagsCSV.hpp"
#include "CSVReader.hpp"
#include "ColumnConfigInstance.hpp"

class TagsCSVReader {
public:
    TagsCSVReader(const TagsCSV& config, std::optional<ColumnConfigInstanceVector> instances);

    // Disable copy and move operations
    TagsCSVReader(const TagsCSVReader&) = delete;
    TagsCSVReader& operator=(const TagsCSVReader&) = delete;
    TagsCSVReader(TagsCSVReader&&) = delete;
    TagsCSVReader& operator=(TagsCSVReader&&) = delete;

    ~TagsCSVReader() = default;

    std::vector<ColumnTypeVector> generate() const;

private:
    TagsCSV config_;
    std::optional<ColumnConfigInstanceVector> instances_;
    std::vector<std::string> resolved_paths_;
    size_t total_columns_ = 0;
    std::vector<std::pair<size_t, ColumnTypeTag>> column_type_map_;

    void validate_config();

    template <typename T>
    T convert_value(const std::string& value) const;

    ColumnType convert_to_type(const std::string& value, ColumnTypeTag target_type) const;
};