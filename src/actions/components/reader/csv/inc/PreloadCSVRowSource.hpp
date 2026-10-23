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
#include "CSVDataManager.hpp"
#include "TimestampGenerator.hpp"
#include "TimestampGeneratorConfig.hpp"
#include <vector>
#include <memory>
#include <string>

class PreloadCSVRowSource : public ICSVRowSource {
public:
    // CSV timestamp mode: use timestamps already present in the rows
    PreloadCSVRowSource(CSVDataManager::SharedRows shared_rows, bool repeat_read);
    PreloadCSVRowSource(std::vector<RowData> rows, bool repeat_read);

    // Generator timestamp mode: override row timestamps with TimestampGenerator
    PreloadCSVRowSource(const TimestampGeneratorConfig& ts_config,
                        const std::string& target_precision,
                        CSVDataManager::SharedRows shared_rows,
                        bool repeat_read);
    PreloadCSVRowSource(const TimestampGeneratorConfig& ts_config,
                        const std::string& target_precision,
                        std::vector<RowData> rows,
                        bool repeat_read);

    // Degenerate case: use_cache + generator ts, only need timestamp generation, no row data
    PreloadCSVRowSource(const TimestampGeneratorConfig& ts_config,
                        const std::string& target_precision);

    std::optional<RowData> next() override;
    bool has_more() const override;
    void reset() override;
    size_t total_rows() const override;

private:
    const std::vector<RowData>& get_rows() const;

    CSVDataManager::SharedRows shared_rows_;
    std::vector<RowData> owned_rows_;
    size_t row_index_ = 0;
    bool repeat_read_ = false;
    bool exhausted_ = false;
    bool degenerate_ = false;  // true when no row data, only timestamp generation
    std::unique_ptr<TimestampGenerator> timestamp_generator_;
    std::string target_precision_;
};
