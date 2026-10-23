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

#include "IFormatter.hpp"
#include "FormatterFactory.hpp"
#include "SchemalessFormatOptions.hpp"
#include "SchemalessInsertData.hpp"

class SchemalessInsertDataFormatter final : public IInsertDataFormatter {
public:
    explicit SchemalessInsertDataFormatter(const DataFormat& format);

    FormatResult format(MemoryPool::MemoryBlock* batch,
                        bool is_checkpoint_recover = false) const override;

private:
    const DataFormat& format_;
    const SchemalessFormatOptions* format_options_;

    int map_precision(const std::string& precision) const;

    inline static bool registered_ = []() {
        FormatterFactory::register_formatter<InsertDataConfig>(
            "schemaless",
            [](const DataFormat& format) {
                return std::make_unique<SchemalessInsertDataFormatter>(format);
            });
        return true;
    }();
};
