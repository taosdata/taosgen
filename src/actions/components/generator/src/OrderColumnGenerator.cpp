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

#include "OrderColumnGenerator.hpp"
#include <stdexcept>


OrderColumnGenerator::OrderColumnGenerator(const ColumnConfigInstance& instance)
    : ColumnGenerator(instance), current_(0)
{
    min_ = instance.config().order_min.value_or(0);
    max_ = instance.config().order_max.value_or(10000);
    current_ = min_;

    if (max_ <= min_) {
        throw std::runtime_error("Invalid order range");
    }

    // Type conversion function
    switch (instance.config().type_tag) {
        case ColumnTypeTag::BOOL:
            convert_ = [](int64_t v) -> ColumnType { return v != 0; };
            break;
        case ColumnTypeTag::TINYINT:
            convert_ = [](int64_t v) -> ColumnType { return static_cast<int8_t>(v); };
            break;
        case ColumnTypeTag::TINYINT_UNSIGNED:
            convert_ = [](int64_t v) -> ColumnType { return static_cast<uint8_t>(v); };
            break;
        case ColumnTypeTag::SMALLINT:
            convert_ = [](int64_t v) -> ColumnType { return static_cast<int16_t>(v); };
            break;
        case ColumnTypeTag::SMALLINT_UNSIGNED:
            convert_ = [](int64_t v) -> ColumnType { return static_cast<uint16_t>(v); };
            break;
        case ColumnTypeTag::INT:
            convert_ = [](int64_t v) -> ColumnType { return static_cast<int32_t>(v); };
            break;
        case ColumnTypeTag::INT_UNSIGNED:
            convert_ = [](int64_t v) -> ColumnType { return static_cast<uint32_t>(v); };
            break;
        case ColumnTypeTag::TIMESTAMP:
        case ColumnTypeTag::BIGINT:
            convert_ = [](int64_t v) -> ColumnType { return static_cast<int64_t>(v); };
            break;
        case ColumnTypeTag::BIGINT_UNSIGNED:
            convert_ = [](int64_t v) -> ColumnType { return static_cast<uint64_t>(v); };
            break;
        default:
            throw std::runtime_error("OrderColumnGenerator: unsupported type_tag for order column");
    }
}

ColumnType OrderColumnGenerator::generate() const {
    if (current_ >= max_) {
        current_ = min_;
    }

    int64_t value = current_;
    current_++;
    return convert_(value);
}

ColumnTypeVector OrderColumnGenerator::generate(size_t count) const {
    ColumnTypeVector values;
    values.reserve(count);

    for (size_t i = 0; i < count; ++i) {
        if (current_ >= max_) {
            current_ = min_;
        }

        values.push_back(convert_(current_));
        current_++;
    }

    return values;
}
