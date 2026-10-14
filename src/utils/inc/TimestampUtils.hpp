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

#include <string>
#include <chrono>
#include <variant>
#include <unordered_map>

class TimestampUtils {
public:
    static int64_t get_precision_multiplier(const std::string& precision);

    static std::tuple<int64_t, int64_t> get_precision_factor(
        const std::string& from_precision,
        const std::string& to_precision);

    static int64_t convert_timestamp_precision(
        int64_t ts,
        const std::string& from_precision,
        const std::string& to_precision);

    static double convert_timestamp_precision_double(
        int64_t ts,
        const std::string& from_precision,
        const std::string& to_precision);

    static int64_t convert_to_timestamp(const std::string& precision);
    static int64_t parse_timestamp(const std::variant<int64_t, std::string>& timestamp, const std::string& precision);
    static int64_t parse_step(const std::variant<int64_t, std::string>& step, const std::string& precision);

private:
    TimestampUtils() = delete;
    static const std::unordered_map<std::string, int64_t> precision_map;

    // Performance optimization helpers
    static bool is_numeric_string(const std::string& str);
    static int64_t parse_numeric_timestamp(const std::string& str);
    static int64_t parse_iso_timestamp(const std::string& timestamp,
                                       const std::string& precision,
                                       bool is_utc);
    static int64_t parse_iso_local_time(const std::string& iso_str,
                                        const std::string& precision);
    static int64_t parse_iso_utc_time(const std::string& iso_str,
                                      const std::string& precision);
};