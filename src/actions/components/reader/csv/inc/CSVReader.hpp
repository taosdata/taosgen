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
#include <optional>
#include <stdexcept>
#include <memory>

using CSVRow = std::vector<std::string>;

class CSVReader {
public:
    CSVReader(const std::string& file_path, bool has_header = true, char delimiter = ',');
    CSVReader(const std::vector<std::string>& file_paths, bool has_header = true, char delimiter = ',');

    CSVReader(const CSVReader&) = delete;
    CSVReader& operator=(const CSVReader&) = delete;
    CSVReader(CSVReader&&) noexcept;
    CSVReader& operator=(CSVReader&&) noexcept;

    ~CSVReader();

    // Read the entire file (all files if multi-file)
    std::vector<CSVRow> read_all();

    // Read line by line (auto-advances across files)
    std::optional<CSVRow> read_next();

    // Get the number of columns (based on first file)
    size_t column_count() const;

    // Reset the reading position to the beginning
    void reset();

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};