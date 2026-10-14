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

#include "ColumnConfigInstance.hpp"
#include "MemoryPool.hpp"

#include <fmt/core.h>
#include <fmt/format.h>
#include <vector>
#include <string>
#include <unordered_map>
#include <sstream>
#include <regex>

class PatternGenerator {
public:
    virtual std::string generate(const MemoryPool::TableBlock& data, size_t row_index) const = 0;

    virtual ~PatternGenerator() = default;

protected:
    struct Token {
        enum Type { TEXT, PLACEHOLDER } type;
        std::string value;
    };

    static std::vector<Token> parse_pattern(const std::string& pattern);
    std::string get_value_as_string(const std::string& key, const MemoryPool::TableBlock& data, size_t row_index) const;
    void build_mapping(const ColumnConfigInstanceVector& col_instances, const ColumnConfigInstanceVector& tag_instances);

    std::vector<Token> tokens_;
    std::unordered_map<std::string, size_t> col_index_map_;
    std::unordered_map<std::string, size_t> tag_index_map_;
};