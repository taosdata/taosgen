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

#include "RateLimiter.hpp"
#include <algorithm>

RateLimiter::RateLimiter(int64_t rate_limit)
    : rate_limit_(rate_limit),
      tokens_(0),
      last_time_(std::chrono::steady_clock::now()) {}

void RateLimiter::acquire(int64_t tokens) {
    using namespace std::chrono;
    auto now = steady_clock::now();
    auto elapsed = duration_cast<milliseconds>(now - last_time_).count();
    
    // Calculate new tokens
    tokens_ += elapsed * rate_limit_ / 1000.0;
    tokens_ = std::min(tokens_, static_cast<double>(rate_limit_));
    
    // Check for sufficient tokens
    while (tokens_ < tokens) {
        // Calculate wait time
        double deficit = tokens - tokens_;
        int64_t wait_ms = std::max(static_cast<int64_t>(1), static_cast<int64_t>(deficit * 1000 / rate_limit_));
        std::this_thread::sleep_for(milliseconds(wait_ms));
        
        // Update tokens
        now = steady_clock::now();
        elapsed = duration_cast<milliseconds>(now - last_time_).count();
        tokens_ += elapsed * rate_limit_ / 1000;
        tokens_ = std::min(tokens_, static_cast<double>(rate_limit_));
    }
    
    // Consume tokens
    tokens_ -= tokens;
    last_time_ = now;
}