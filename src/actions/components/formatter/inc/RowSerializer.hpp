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
#include <nlohmann/json.hpp>

enum class IntSuffixMode {
    STANDARD,   // InfluxDB/Kafka: i, u
    TDENGINE    // TDengine schemaless: i8, u8, i16, u16, i32, u32, i64, u64
};

class RowSerializer {
public:
    static nlohmann::ordered_json to_json(
        const ColumnConfigInstanceVector& col_instances,
        const ColumnConfigInstanceVector& tag_instances,
        const MemoryPool::TableBlock& table,
        size_t row_index,
        const std::string& tbname_key
    );

    static void to_json_inplace(
        const ColumnConfigInstanceVector& col_instances,
        const ColumnConfigInstanceVector& tag_instances,
        const MemoryPool::TableBlock& table,
        size_t row_index,
        const std::string& tbname_key,
        nlohmann::ordered_json& out
    );

    // Returns false if the row was skipped (all fields are NULL/NONE).
    static bool to_influx_inplace(
        const ColumnConfigInstanceVector& col_instances,
        const ColumnConfigInstanceVector& tag_instances,
        const MemoryPool::TableBlock& table,
        size_t row_index,
        const std::string& measurement,
        const std::string& id_tag_key,
        fmt::memory_buffer& out,
        IntSuffixMode suffix_mode = IntSuffixMode::STANDARD);
};
