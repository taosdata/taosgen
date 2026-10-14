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

#include "ActionMetrics.hpp"
#include <numeric>
#include <cmath>
#include <iterator>

ActionMetrics::ActionMetrics(size_t sample_count) {
    samples_.reserve(sample_count);
}

void ActionMetrics::add_sample(double duration_ms) {
    samples_.push_back(duration_ms);
}

const std::vector<double>& ActionMetrics::get_samples() const {
    return samples_;
}

void ActionMetrics::merge_from(const ActionMetrics& other) {
    const auto& samples = other.get_samples();
    if (!samples.empty()) {
        samples_.insert(samples_.end(), samples.begin(), samples.end());
    }
}

void ActionMetrics::merge_from(const std::vector<ActionMetrics>& others) {
    // samples_.clear();

    // Add data from other threads
    for (const auto& other : others) {
        const auto& samples = other.get_samples();
        if (!samples.empty()) {
            samples_.insert(samples_.end(), samples.begin(), samples.end());
        }
    }
}

void ActionMetrics::calculate() {
    if (samples_.empty()) {
        return;
    }

    std::sort(samples_.begin(), samples_.end());

    metrics_.min = samples_.front();
    metrics_.max = samples_.back();
    metrics_.sum = std::accumulate(samples_.begin(), samples_.end(), 0.0);
    metrics_.avg = metrics_.sum / samples_.size();

    calculate_percentile(90.0, metrics_.p90);
    calculate_percentile(95.0, metrics_.p95);
    calculate_percentile(99.0, metrics_.p99);
}

void ActionMetrics::calculate_percentile(double percentile, double& result) {
    if (samples_.empty()) {
        result = 0.0;
        return;
    }
    
    double index = (percentile / 100.0) * (samples_.size() - 1);
    size_t lower = static_cast<size_t>(std::floor(index));
    size_t upper = static_cast<size_t>(std::ceil(index));
    
    if (lower == upper) {
        result = samples_[lower];
    } else {
        double weight = index - lower;
        result = samples_[lower] * (1 - weight) + samples_[upper] * weight;
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
    samples_.clear();
    metrics_ = {};
}