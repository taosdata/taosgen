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

#include "PatternGenerator.hpp"

class KeyGenerator final : public PatternGenerator {
public:
    KeyGenerator(const std::string& pattern,
                 const std::string& serializer,
                 const ColumnConfigInstanceVector& col_instances,
                 const ColumnConfigInstanceVector& tag_instances);

    std::string generate(const MemoryPool::TableBlock& data, size_t row_index) const override;

private:
    enum class SerializerType {
        STRING_UTF8,
        INT8, UINT8,
        INT16, UINT16,
        INT32, UINT32,
        INT64, UINT64
    };

    SerializerType serializer_type_;
    std::string single_placeholder_key_;    // Used for integer serializers

    static SerializerType string_to_serializer(const std::string& s);

    template<typename T>
    std::string serialize_to_big_endian(T value) const;

    static const bool is_little_endian_;
};