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

#include <vector>
#include <mutex>
#include <algorithm>
#include <string>

class ActionMetrics {
public:
    ActionMetrics(size_t sample_count = 10000);

    // Add a sample to the thread-local bucket
    void add_sample(double duration_ms);

    // Merge data from other thread instances
    void merge_from(const ActionMetrics& other);
    void merge_from(const std::vector<ActionMetrics>& others);
    
    // Calculate final metrics
    void calculate();
    
    // Get a summary of the statistics
    std::string get_summary() const;
    
    // Reset the statistical data
    void reset();
    
    // Get samples
    const std::vector<double>& get_samples() const;

    double get_min() const { return metrics_.min; }
    double get_max() const { return metrics_.max; }
    double get_avg() const { return metrics_.avg; }
    double get_sum() const { return metrics_.sum; }
    double get_p90() const { return metrics_.p90; }
    double get_p95() const { return metrics_.p95; }
    double get_p99() const { return metrics_.p99; }

private:
    // Samples data
    std::vector<double> samples_;
    
    // Final calculated results
    struct {
        double min = 0.0;
        double max = 0.0;
        double avg = 0.0;
        double sum = 0.0;
        double p90 = 0.0;
        double p95 = 0.0;
        double p99 = 0.0;
    } metrics_;
    
    // Calculate percentile using linear interpolation
    void calculate_percentile(double percentile, double& result);
};