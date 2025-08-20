#include "BaseWriter.hpp"
#include <stdexcept>
#include <chrono>
#include <iostream>

BaseWriter::BaseWriter(const InsertDataConfig& config, const ColumnConfigInstanceVector& col_instances)
    : config_(config), col_instances_(col_instances),
      timestamp_precision_(config.target.timestamp_precision),
      time_strategy_(config.control.time_interval, config.target.timestamp_precision),
      start_write_time_(std::chrono::steady_clock::now()),
      end_write_time_(std::chrono::steady_clock::now()) {

    // Validate timestamp precision
    if (timestamp_precision_.empty()) {
        timestamp_precision_ = "ms";
    } else if (timestamp_precision_ != "ms" &&
               timestamp_precision_ != "us" &&
               timestamp_precision_ != "ns") {
        throw std::invalid_argument("Invalid timestamp precision: " + timestamp_precision_);
    }
}

void BaseWriter::apply_time_interval_strategy(int64_t current_start, int64_t current_end) {
    time_strategy_.apply_wait_strategy(
        current_start, current_end,
        last_start_time_, last_end_time_,
        first_write_
    );

    if (first_write_) {
        start_write_time_ = time_strategy_.last_write_time();
    }
}

std::string BaseWriter::get_format_description() const {
    return config_.control.data_format.format_type;
}

void BaseWriter::update_write_state(const BaseInsertData& data, bool success) {
    if (success) {
        end_write_time_ = std::chrono::steady_clock::now();
        last_start_time_ = data.start_time;
        last_end_time_ = data.end_time;
        current_retry_count_ = 0;
        first_write_ = false;
    }
}

void BaseWriter::update_play_metrics(const BaseInsertData& data) {
    if (time_strategy_.is_literal_strategy()) {
        auto now = std::chrono::system_clock::now();
        int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            now.time_since_epoch()
        ).count();

        int64_t elapsed_ms = now_ms - data.start_time;
        play_metrics_.add_sample(elapsed_ms);
    }
}