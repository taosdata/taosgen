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
#include <memory>
#include <stdexcept>
#include "InsertDataConfig.hpp"
#include "TableNameGenerator.hpp"
#include "TableNameCSVReader.hpp"

class TableNameManager {
public:
    explicit TableNameManager(const InsertDataConfig& config);

    // Generate all table names based on config
    std::vector<std::string> generate_table_names();

    // Split table names based on thread allocation strategy
    std::vector<std::vector<std::string>> split_for_threads();

    size_t chunk_size() const { return chunk_size_; }

private:
    const InsertDataConfig& config_;
    std::vector<std::string> table_names_;
    size_t chunk_size_;

    // Split methods for different strategies
    std::vector<std::vector<std::string>> split_by_index_range();
    std::vector<std::vector<std::string>> split_by_vgroup_binding();

    // Helper method to perform equal splits
    std::vector<std::vector<std::string>> split_equally(size_t thread_count);
};