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

#include "PreloadCSVRowSource.hpp"
#include "TimestampUtils.hpp"

// CSV timestamp mode constructors
PreloadCSVRowSource::PreloadCSVRowSource(CSVDataManager::SharedRows shared_rows, bool repeat_read)
    : shared_rows_(std::move(shared_rows)), repeat_read_(repeat_read) {}

PreloadCSVRowSource::PreloadCSVRowSource(std::vector<RowData> rows, bool repeat_read)
    : owned_rows_(std::move(rows)), repeat_read_(repeat_read) {}

// Generator timestamp mode constructors
PreloadCSVRowSource::PreloadCSVRowSource(const TimestampGeneratorConfig& ts_config,
                                         const std::string& target_precision,
                                         CSVDataManager::SharedRows shared_rows,
                                         bool repeat_read)
    : shared_rows_(std::move(shared_rows)),
      repeat_read_(repeat_read),
      timestamp_generator_(std::make_unique<TimestampGenerator>(ts_config)),
      target_precision_(target_precision) {}

PreloadCSVRowSource::PreloadCSVRowSource(const TimestampGeneratorConfig& ts_config,
                                         const std::string& target_precision,
                                         std::vector<RowData> rows,
                                         bool repeat_read)
    : owned_rows_(std::move(rows)),
      repeat_read_(repeat_read),
      timestamp_generator_(std::make_unique<TimestampGenerator>(ts_config)),
      target_precision_(target_precision) {}

// Degenerate case: use_cache + generator ts, only timestamp generation
PreloadCSVRowSource::PreloadCSVRowSource(const TimestampGeneratorConfig& ts_config,
                                         const std::string& target_precision)
    : repeat_read_(false),
      degenerate_(true),
      timestamp_generator_(std::make_unique<TimestampGenerator>(ts_config)),
      target_precision_(target_precision) {}

std::optional<RowData> PreloadCSVRowSource::next() {
    // Degenerate case: no row data, only generate timestamps
    if (degenerate_) {
        RowData row;
        row.timestamp = TimestampUtils::convert_timestamp_precision(
            timestamp_generator_->generate(),
            timestamp_generator_->timestamp_precision(),
            target_precision_);
        return row;
    }

    if (exhausted_) return std::nullopt;

    const auto& rows = get_rows();
    if (rows.empty()) {
        exhausted_ = true;
        return std::nullopt;
    }

    if (row_index_ >= rows.size()) {
        if (repeat_read_) {
            row_index_ = 0;
            // Note: do NOT reset timestamp_generator_ here.
            // repeat_read cycles through rows while timestamps advance monotonically.
        } else {
            exhausted_ = true;
            return std::nullopt;
        }
    }

    RowData row = rows[row_index_];
    row_index_++;

    // Override timestamp if we have a generator
    if (timestamp_generator_) {
        row.timestamp = TimestampUtils::convert_timestamp_precision(
            timestamp_generator_->generate(),
            timestamp_generator_->timestamp_precision(),
            target_precision_);
    }

    return row;
}

bool PreloadCSVRowSource::has_more() const {
    if (degenerate_) return true;  // always has more when only generating timestamps
    if (exhausted_) return false;
    if (repeat_read_) return true;
    const auto& rows = get_rows();
    return !rows.empty() && row_index_ < rows.size();
}

void PreloadCSVRowSource::reset() {
    row_index_ = 0;
    exhausted_ = false;
    if (timestamp_generator_) {
        timestamp_generator_->reset();
    }
}

size_t PreloadCSVRowSource::total_rows() const {
    if (degenerate_) return 0;
    return get_rows().size();
}

const std::vector<RowData>& PreloadCSVRowSource::get_rows() const {
    if (shared_rows_) return *shared_rows_;
    return owned_rows_;
}
