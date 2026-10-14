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

#include "taos.h"
#include <string>
#include <cstdint>
#include <typeindex>

struct SchemalessInsertData {
    std::string lines;
    int32_t total_rows = 0;
    int protocol = TSDB_SML_LINE_PROTOCOL;
    int precision = TSDB_SML_TIMESTAMP_MILLI_SECONDS;

    SchemalessInsertData() = default;

    SchemalessInsertData(std::string&& lines, int32_t total_rows, int protocol, int precision)
        : lines(std::move(lines)), total_rows(total_rows), protocol(protocol), precision(precision) {}

    SchemalessInsertData(SchemalessInsertData&&) = default;
    SchemalessInsertData& operator=(SchemalessInsertData&&) = default;

    SchemalessInsertData(const SchemalessInsertData&) = delete;
    SchemalessInsertData& operator=(const SchemalessInsertData&) = delete;
};

inline std::type_index SCHEMALESS_TYPE_ID = std::type_index(typeid(SchemalessInsertData));
