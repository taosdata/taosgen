#include "ActionMetrics.h"
#include <numeric>
#include <cmath>
#include <iterator>

ActionMetrics::ActionMetrics(size_t sample_count) {
    thread_samples_.reserve(sample_count);
}

void ActionMetrics::add_sample(double duration_ms) {
    thread_samples_.push_back(duration_ms);
}

std::vector<double> ActionMetrics::get_samples_snapshot() const {
    return thread_samples_;
}

std::vector<double> ActionMetrics::get_all_samples() const {
    return all_samples_;
}

void ActionMetrics::merge_from(const std::vector<ActionMetrics>& others) {
    all_samples_.clear();
    
    // First add the current thread's data
    if (!thread_samples_.empty()) {
        all_samples_.insert(all_samples_.end(),
                           thread_samples_.begin(), thread_samples_.end());
    }
    
    // Then add data from other threads
    for (const auto& other : others) {
        const auto& samples = other.get_samples_snapshot();
        if (!samples.empty()) {
            all_samples_.insert(all_samples_.end(), samples.begin(), samples.end());
        }
    }
}

void ActionMetrics::calculate() {
    if (all_samples_.empty()) {
        return;
    }

    std::sort(all_samples_.begin(), all_samples_.end());

    metrics_.min = all_samples_.front();
    metrics_.max = all_samples_.back();
    metrics_.avg = std::accumulate(all_samples_.begin(), all_samples_.end(), 0.0) / all_samples_.size();

    calculate_percentile(90.0, metrics_.p90);
    calculate_percentile(95.0, metrics_.p95);
    calculate_percentile(99.0, metrics_.p99);
}

void ActionMetrics::calculate_percentile(double percentile, double& result) {
    if (all_samples_.empty()) {
        result = 0.0;
        return;
    }
    
    double index = (percentile / 100.0) * (all_samples_.size() - 1);
    size_t lower = static_cast<size_t>(std::floor(index));
    size_t upper = static_cast<size_t>(std::ceil(index));
    
    if (lower == upper) {
        result = all_samples_[lower];
    } else {
        double weight = index - lower;
        result = all_samples_[lower] * (1 - weight) + all_samples_[upper] * weight;
    }
}

std::string ActionMetrics::get_summary() const {
    char buffer[256];
    snprintf(buffer, sizeof(buffer),
        "min: %.4fms, avg: %.4fms, p90: %.4fms, p95: %.4fms, p99: %.4fms, max: %.4fms",
        metrics_.min, metrics_.avg, metrics_.p90, metrics_.p95, metrics_.p99, metrics_.max);
    return buffer;
}

void ActionMetrics::reset() {
    thread_samples_.clear();
    all_samples_.clear();
    metrics_ = {};
}