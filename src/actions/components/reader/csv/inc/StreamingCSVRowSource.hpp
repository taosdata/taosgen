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

#include "ICSVRowSource.hpp"
#include "CSVReader.hpp"
#include "TypeConverter.hpp"
#include "TimestampUtils.hpp"
#include "ColumnConfigInstance.hpp"
#include "TimestampGenerator.hpp"
#include "TimestampStrategy.hpp"
#include <memory>
#include <string>
#include <vector>

class StreamingCSVRowSource : public ICSVRowSource {
public:
    StreamingCSVRowSource(const std::vector<std::string>& file_paths,
                          bool has_header,
                          char delimiter,
                          const ColumnConfigInstanceVector& instances,
                          const TimestampStrategy& timestamp_strategy,
                          const std::string& csv_precision,
                          const std::string& target_precision,
                          bool repeat_read);

    std::optional<RowData> next() override;
    bool has_more() const override;
    void reset() override;
    size_t total_rows() const override { return 0; }

private:
    RowData convert_row(const CSVRow& raw_row);

    std::unique_ptr<CSVReader> reader_;
    std::vector<std::string> file_paths_;
    bool has_header_;
    char delimiter_;
    const ColumnConfigInstanceVector& instances_;
    TimestampStrategy timestamp_strategy_;
    std::string csv_precision_;
    std::string target_precision_;
    bool repeat_read_;
    bool exhausted_ = false;

    // Timestamp generator (when strategy is "generator")
    std::unique_ptr<TimestampGenerator> timestamp_generator_;

    // For absolute offset tracking
    int64_t first_raw_ts_ = 0;
    bool first_raw_ts_set_ = false;
};
