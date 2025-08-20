#include "TimeIntervalStrategy.hpp"
#include <chrono>
#include <thread>


IntervalStrategyType parse_strategy(const std::string& s) {
    if (s == "fixed") return IntervalStrategyType::Fixed;
    if (s == "first_to_first") return IntervalStrategyType::FirstToFirst;
    if (s == "last_to_first") return IntervalStrategyType::LastToFirst;
    if (s == "literal") return IntervalStrategyType::Literal;
    return IntervalStrategyType::Unknown;
}

TimeIntervalStrategy::TimeIntervalStrategy(
    const InsertDataConfig::Control::TimeInterval& config,
    const std::string& timestamp_precision)
    : config_(config)
    , timestamp_precision_(timestamp_precision)
    , last_write_time_(std::chrono::steady_clock::now())
    , strategy_type_(parse_strategy(config.interval_strategy))
    {}

int64_t TimeIntervalStrategy::to_milliseconds(int64_t ts) const {
    if (timestamp_precision_ == "ms") return ts;
    if (timestamp_precision_ == "us") return ts / 1000;
    if (timestamp_precision_ == "ns") return ts / 1000000;
    throw std::runtime_error("Unknown timestamp precision");
}

int64_t TimeIntervalStrategy::clamp_interval(int64_t interval) const {
    const auto& dyn_cfg = config_.dynamic_interval;
    if (dyn_cfg.min_interval >= 0 && interval < dyn_cfg.min_interval) {
        return dyn_cfg.min_interval;
    }
    if (dyn_cfg.max_interval >= 0 && interval > dyn_cfg.max_interval) {
        return dyn_cfg.max_interval;
    }
    return interval;
}

int64_t TimeIntervalStrategy::fixed_interval_strategy() const {
    return config_.fixed_interval.base_interval;
}

int64_t TimeIntervalStrategy::first_to_first_strategy(
    int64_t current_start, int64_t last_start) const
{
    return to_milliseconds(current_start) - to_milliseconds(last_start);
}

int64_t TimeIntervalStrategy::last_to_first_strategy(
    int64_t current_start, int64_t last_end) const
{
    return to_milliseconds(current_start) - to_milliseconds(last_end);
}

int64_t TimeIntervalStrategy::literal_strategy(int64_t current_start) const {
    auto now = std::chrono::system_clock::now();
    auto now_ms = std::chrono::time_point_cast<std::chrono::milliseconds>(now);
    auto epoch = now_ms.time_since_epoch();
    int64_t now_timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(epoch).count();

    return to_milliseconds(current_start) - now_timestamp;
}

void TimeIntervalStrategy::apply_wait_strategy(
    int64_t current_start_time,
    int64_t current_end_time,
    int64_t last_start_time,
    int64_t last_end_time,
    bool is_first_write)
{
    (void)current_end_time;

    if (!config_.enabled || (is_first_write && strategy_type_ != IntervalStrategyType::Literal)) {
        last_write_time_ = std::chrono::steady_clock::now();
        return;
    }

    int64_t wait_time_ms = 0;

    // Calculate wait time
    switch (strategy_type_) {
        case IntervalStrategyType::Fixed:
            wait_time_ms = fixed_interval_strategy();
            break;
        case IntervalStrategyType::FirstToFirst:
            wait_time_ms = first_to_first_strategy(current_start_time, last_start_time);
            wait_time_ms = clamp_interval(wait_time_ms);
            break;
        case IntervalStrategyType::LastToFirst:
            wait_time_ms = last_to_first_strategy(current_start_time, last_end_time);
            wait_time_ms = clamp_interval(wait_time_ms);
            break;
        case IntervalStrategyType::Literal:
            wait_time_ms = literal_strategy(current_start_time);
            break;
        default:
            wait_time_ms = 0;
            break;
    }

    // Execute wait strategy
    if (wait_time_ms > 0) {
        if (config_.wait_strategy == "sleep") {
            std::this_thread::sleep_for(std::chrono::milliseconds(wait_time_ms));
        } else { // busy_wait
            auto start = std::chrono::steady_clock::now();
            while (std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - start).count() < wait_time_ms) {
            }
        }
    }

    last_write_time_ = std::chrono::steady_clock::now();
}