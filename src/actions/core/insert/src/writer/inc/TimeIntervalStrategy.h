#pragma once
#include <chrono>
#include <atomic>
#include "InsertDataConfig.h"

class TimeIntervalStrategy {
public:
    TimeIntervalStrategy(
        const InsertDataConfig::Control::TimeInterval& config,
        const std::string& timestamp_precision);
    
    // Apply wait strategy
    void apply_wait_strategy(int64_t current_start_time,
                             int64_t current_end_time,
                             int64_t last_start_time,
                             int64_t last_end_time,
                             bool is_first_write);

    // Timestamp conversion
    int64_t to_milliseconds(int64_t ts) const;
    
    // Fixed interval strategy
    int64_t fixed_interval_strategy() const;

    // Dynamic interval clamping
    int64_t clamp_interval(int64_t interval) const;
    
private:
    // First row to first row strategy
    int64_t first_to_first_strategy(int64_t current_start, int64_t last_start) const;
    
    // Last row to first row strategy
    int64_t last_to_first_strategy(int64_t current_start, int64_t last_end) const;
    
    // Real-time strategy
    int64_t literal_strategy(int64_t current_start) const;
    
    // Configuration
    const InsertDataConfig::Control::TimeInterval& config_;
    
    // Timestamp precision
    const std::string& timestamp_precision_;
    
    // Last write completion timestamp
    std::chrono::steady_clock::time_point last_write_time_;
};