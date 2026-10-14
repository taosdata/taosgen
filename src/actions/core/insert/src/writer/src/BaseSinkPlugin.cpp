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

#include "BaseSinkPlugin.hpp"
#include <algorithm>
#include <stdexcept>
#include <chrono>
#include <iostream>

BaseSinkPlugin::BaseSinkPlugin(const InsertDataConfig& config,
                               const ColumnConfigInstanceVector& col_instances,
                               const ColumnConfigInstanceVector& tag_instances,
                               std::shared_ptr<ActionRegisterInfo> action_info)
    : config_(config),
      col_instances_(col_instances),
      tag_instances_(tag_instances),
      timestamp_precision_(config.timestamp_precision),
      time_strategy_(config.time_interval, config.timestamp_precision),
      start_write_time_(std::chrono::steady_clock::now()),
      end_write_time_(std::chrono::steady_clock::now()),
      action_info_(action_info) {

    // Validate timestamp precision
    if (timestamp_precision_.empty()) {
        timestamp_precision_ = "ms";
    } else if (timestamp_precision_ != "ms" &&
               timestamp_precision_ != "us" &&
               timestamp_precision_ != "ns") {
        throw std::invalid_argument("Invalid timestamp precision: " + timestamp_precision_);
    }
}

void BaseSinkPlugin::apply_time_interval_strategy(int64_t current_start, int64_t current_end) {
    time_strategy_.apply_wait_strategy(
        current_start, current_end,
        last_start_time_, last_end_time_,
        first_write_
    );

    if (first_write_) {
        start_write_time_ = time_strategy_.last_write_time();
    }
}

std::string BaseSinkPlugin::get_format_description() const {
    return config_.data_format.format_type;
}

void BaseSinkPlugin::update_write_state(const BaseInsertData& data, bool /* success */ ) {
    end_write_time_ = std::chrono::steady_clock::now();
    last_start_time_ = data.start_time;
    last_end_time_ = data.end_time;
    first_write_ = false;
}

void BaseSinkPlugin::notify(const BaseInsertData& data, bool success) {
    if (action_info_ && action_info_->action && success) {
        std::shared_ptr<ActionBase>& ptr = action_info_->action.value();
        if (ptr) {
            const auto& checkpoint_data = data.get_block()->checkpoint_data_list_;
            std::any payload = std::cref(checkpoint_data);
            ptr->notify(payload);
        }
    }
}

std::string BaseSinkPlugin::get_retry_batch_context(const BaseInsertData& data) const {
    const auto* block = data.get_block();
    if (!block) {
        return fmt::format(" rows={}", data.total_rows);
    }

    const size_t table_count = std::min(block->used_tables, block->tables.size());
    if (table_count == 0) {
        return fmt::format(" table_count=0 rows={}", data.total_rows);
    }

    const auto table_name = [](const MemoryPool::TableBlock& table) {
        return table.table_name ? std::string_view(table.table_name) : std::string_view();
    };

    if (table_count == 1) {
        return fmt::format(
            " table_count=1 table={:?} rows={}",
            table_name(block->tables.front()),
            data.total_rows
        );
    }

    return fmt::format(
        " table_count={} table_first={:?} table_last={:?} rows={}",
        table_count,
        table_name(block->tables.front()),
        table_name(block->tables[table_count - 1]),
        data.total_rows
    );
}

std::string BaseSinkPlugin::get_retry_error_context(std::string_view fallback_reason) const {
    return fmt::format("reason={:?}", fallback_reason);
}

void BaseSinkPlugin::log_retrying(std::string_view operation,
                                  std::string_view batch_context,
                                  size_t attempt,
                                  size_t retries,
                                  size_t max_retries,
                                  std::string_view error_context) const {
    LogUtils::warn(
        "write_status event=retrying operation={} attempt={} retries={} max_retries={}{} "
        "{} retry_interval_ms={}",
        operation,
        attempt,
        retries,
        max_retries,
        batch_context,
        error_context,
        config_.failure_handling.retry_interval_ms
    );
}

void BaseSinkPlugin::log_recovered(std::string_view operation,
                                   std::string_view batch_context,
                                   size_t attempt,
                                   size_t retries,
                                   size_t max_retries,
                                   std::chrono::milliseconds recovery_time) const {
    LogUtils::info(
        "write_status event=recovered operation={} attempt={} retries={} max_retries={}{} recovery_after_ms={}",
        operation,
        attempt,
        retries,
        max_retries,
        batch_context,
        recovery_time.count()
    );
}

void BaseSinkPlugin::log_retry_failed(std::string_view operation,
                                      std::string_view batch_context,
                                      size_t attempt,
                                      size_t retries,
                                      size_t max_retries,
                                      std::string_view error_context) const {
    LogUtils::error(
        "write_status event=failed operation={} attempt={} retries={} max_retries={}{} "
        "{} on_failure={}",
        operation,
        attempt,
        retries,
        max_retries,
        batch_context,
        error_context,
        config_.failure_handling.on_failure
    );
}

void BaseSinkPlugin::update_play_metrics(const BaseInsertData& data) {
    if (time_strategy_.is_literal_strategy()) {
        double now = TimestampUtils::convert_to_timestamp("us");
        double start_time = TimestampUtils::convert_timestamp_precision_double(data.start_time, timestamp_precision_, "us");
        double elapsed_ms = (now - start_time) / 1000.0;
        elapsed_ms = elapsed_ms < 0 ? 0.0 : elapsed_ms;
        play_metrics_.add_sample(elapsed_ms);
    }
}
