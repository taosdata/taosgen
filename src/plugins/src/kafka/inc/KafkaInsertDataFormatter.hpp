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

#include "IFormatter.hpp"
#include "FormatterFactory.hpp"
#include "KafkaInsertData.hpp"
#include "KafkaFormatOptions.hpp"
#include "KeyGenerator.hpp"
#include "RowSerializer.hpp"
#include <nlohmann/json.hpp>

class KafkaInsertDataFormatter final : public IInsertDataFormatter {
public:
    explicit KafkaInsertDataFormatter(const DataFormat& format);

    FormatResult format(MemoryPool::MemoryBlock* batch,
                        bool is_checkpoint_recover = false) const override;

private:
    const DataFormat& format_;
    const KafkaFormatOptions* format_options_;

    FormatResult format_json(MemoryPool::MemoryBlock* batch) const;

    FormatResult format_influx(MemoryPool::MemoryBlock* batch) const;

    inline static bool registered_ = []() {
        FormatterFactory::register_formatter<InsertDataConfig>(
            "kafka",
            [](const DataFormat& format) {
                return std::make_unique<KafkaInsertDataFormatter>(format);
            });
        return true;
    }();
};