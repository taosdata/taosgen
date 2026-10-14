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

#include "TableNameCSVReader.hpp"
#include "FilePathResolver.hpp"
#include <stdexcept>
#include <algorithm>
#include <sstream>
#include "StringUtils.hpp"


TableNameCSVReader::TableNameCSVReader(const TableNameConfig::CSV& config) : config_(config) {
    validate_config();
}

void TableNameCSVReader::validate_config() const {
    if (config_.file_path.empty()) {
        throw std::invalid_argument("CSV file path is empty for table names");
    }

    if (config_.tbname_index < 0) {
        throw std::invalid_argument("tbname_index must be non-negative");
    }
}

std::vector<std::string> TableNameCSVReader::generate() const {
    try {
        // Resolve file paths and create a CSV reader
        auto resolved_paths = FilePathResolver::resolve(config_.file_path);
        CSVReader reader(
            resolved_paths,
            config_.has_header,
            config_.delimiter.empty() ? ',' : config_.delimiter[0]
        );

        // Read all rows
        auto rows = reader.read_all();

        // Get the number of columns
        const size_t column_count = reader.column_count();
        const size_t name_index = static_cast<size_t>(config_.tbname_index);

        // Validate that the column index is within bounds
        if (name_index >= column_count) {
            std::stringstream ss;
            ss << "tbname_index (" << name_index << ") exceeds column count ("
               << column_count << ") in CSV file: " << config_.file_path;
            throw std::out_of_range(ss.str());
        }

        // Extract table names
        std::vector<std::string> names;
        names.reserve(rows.size());

        for (size_t i = 0; i < rows.size(); ++i) {
            const auto& row = rows[i];

            // Validate that the row has enough columns
            if (row.size() <= name_index) {
                std::stringstream ss;
                ss << "Row " << (i + 1) << " has only " << row.size()
                   << " columns, but tbname_index is " << name_index
                   << " in file: " << config_.file_path;
                throw std::out_of_range(ss.str());
            }

            // Add table name
            std::string name = row[name_index];
            StringUtils::trim(name);
            names.push_back(std::move(name));
        }

        // Validate that no table name is empty
        if (std::any_of(names.begin(), names.end(), [](const std::string& name) {
            return name.empty();
        })) {
            throw std::runtime_error("Found empty table name in CSV file: " + config_.file_path);
        }

        return names;
    } catch (const std::out_of_range& e) {
        throw;
    } catch (const std::exception& e) {
        std::stringstream ss;
        ss << "Failed to generate table names from CSV: " << config_.file_path
           << " - " << e.what();
        throw std::runtime_error(ss.str());
    }
}