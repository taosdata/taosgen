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

#include "TimestampGenerator.hpp"
#include <chrono>
#include <stdexcept>
#include <variant>
#include <unordered_map>
#include "TimestampUtils.hpp"


TimestampGenerator::TimestampGenerator(const TimestampGeneratorConfig& config)
    : config_(config)
{
    // Initialize current timestamp
    reset();
}

void TimestampGenerator::reset() {
    // Reset current timestamp to initial value based on config
    current_ = TimestampUtils::parse_timestamp(
        config_.start_timestamp,
        config_.timestamp_precision
    );
    timestamp_step_ = TimestampUtils::parse_step(
        config_.timestamp_step,
        config_.timestamp_precision
    );
}

Timestamp TimestampGenerator::generate() const {
    Timestamp ts{current_};
    current_ += timestamp_step_;
    return ts;
}

std::vector<Timestamp> TimestampGenerator::generate(size_t count) const {
    std::vector<Timestamp> timestamps;
    timestamps.reserve(count);

    for (size_t i = 0; i < count; ++i) {
        timestamps.push_back(generate());
    }

    return timestamps;
}

const std::string& TimestampGenerator::timestamp_precision() const {
    return config_.timestamp_precision;
}