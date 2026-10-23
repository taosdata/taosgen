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

#include "TopicGenerator.hpp"

TopicGenerator::TopicGenerator(const std::string& pattern,
                               const ColumnConfigInstanceVector& col_instances,
                               const ColumnConfigInstanceVector& tag_instances) {
    // Parse the pattern
    tokens_ = parse_pattern(pattern);

    // Build mapping
    build_mapping(col_instances, tag_instances);
}

std::string TopicGenerator::generate(const MemoryPool::TableBlock& data,
                                     size_t row_index) const {
    fmt::memory_buffer buffer;

    for (const auto& token : tokens_) {
        if (token.type == Token::TEXT) {
            fmt::format_to(std::back_inserter(buffer), "{}", token.value);
        } else {
            fmt::format_to(std::back_inserter(buffer), "{}", get_value_as_string(token.value, data, row_index));
        }
    }
    return fmt::to_string(buffer);
}