#pragma once

/*
 * Copyright (c) 2026 TAOS Data, Inc.
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

#include "ColumnType.hpp"
#include "IngressConfig.hpp"
#include <cstdint>
#include <string>
#include <type_traits>
#include <variant>
#include <vector>

struct IngestTimestampValues {
    std::vector<int64_t> values;
    std::vector<std::string> string_values;

    size_t row_count() const {
        return !values.empty() ? values.size() : string_values.size();
    }
};

struct IngestColumnValues {
    using BinaryArray = std::vector<std::vector<uint8_t>>;
    using Data = std::variant<
        std::vector<bool>,
        std::vector<int32_t>,
        std::vector<int64_t>,
        std::vector<float>,
        std::vector<double>,
        std::vector<std::string>,
        BinaryArray,
        IngestTimestampValues
    >;

    Data data;
    std::vector<bool> nulls;

    size_t row_count() const {
        return std::visit([](const auto& values) -> size_t {
            using T = std::decay_t<decltype(values)>;
            if constexpr (std::is_same_v<T, IngestTimestampValues>) {
                return values.row_count();
            } else {
                return values.size();
            }
        }, data);
    }
};

struct IngestTableBatch {
    std::string table_name;
    IngestTimestampValues timestamps;
    std::vector<IngestColumnValues> columns;
    RowType tags;
};

struct IngestRequestData {
    std::string session_id;
    std::string request_id;
    AckMode ack_mode = AckMode::EnqueueOnly;
    std::vector<IngestTableBatch> tables;
};

struct IngestValidationResult {
    size_t row_count = 0;
    size_t table_count = 0;
};
