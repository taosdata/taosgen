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

#include "StringUtils.hpp"
#include "TimestampUtils.hpp"
#include <string>
#include <chrono>
#include <optional>
#include <variant>
#include <tuple>
#include <sstream>
#include <cctype>
#include <stdexcept>
#include <iomanip>
#include <ctime>
#include <algorithm>


struct TimestampCSVConfig {
    bool enabled = false;
    size_t timestamp_index = 0;                                 // Index of the original timestamp column (starting from 0)
    std::optional<std::string> timestamp_precision;             // Time precision, options: "s", "ms", "us", "ns"
    bool timestamp_precision_configured = false;

    struct OffsetConfig {
        std::string offset_type;                    // Timestamp offset type: "relative" or "absolute"
        std::variant<int64_t, std::string> value;   // Offset value or starting timestamp

        // Parse result cache
        std::tuple<int, int, int, int, int, int> relative_offset = {0, 0, 0, 0, 0, 0}; // (years, months, days, hours, minutes, seconds)
        int64_t absolute_value = 0;
        bool parsed = false;

        OffsetConfig() = default;
        OffsetConfig(const std::string& type, const std::variant<int64_t, std::string>& val, const std::string& precision)
            : offset_type(type), value(val)
        {
            parse_offset(precision);
        }

        void parse_offset(const std::string& precision) {
            if (offset_type == "relative") {
                if (!std::holds_alternative<std::string>(value)) {
                    throw std::runtime_error("Relative offset must be string");
                }
                relative_offset = parse_time_offset(std::get<std::string>(value));
                parsed = true;
            } else if (offset_type == "absolute") {
                absolute_value = TimestampUtils::parse_timestamp(value, precision);
                parsed = true;
            }
        }

        // Parse relative offset string
        static std::tuple<int, int, int, int, int, int> parse_time_offset(const std::string& offset_str) {
            if (offset_str.empty()) {
                throw std::runtime_error("Empty offset string");
            }
            char sign = offset_str[0];
            if (sign != '+' && sign != '-') {
                throw std::runtime_error("Invalid sign character: " + std::string(1, sign));
            }
            int multiplier = (sign == '+') ? 1 : -1;
            int years = 0, months = 0, days = 0, hours = 0, minutes = 0, seconds = 0;
            size_t pos = 1;
            while (pos < offset_str.size()) {
                size_t num_end = pos;
                while (num_end < offset_str.size() && std::isdigit(offset_str[num_end])) {
                    ++num_end;
                }
                if (num_end == pos) {
                    throw std::runtime_error("Missing number at position: " + std::to_string(pos));
                }
                int value;
                try {
                    value = std::stoi(offset_str.substr(pos, num_end - pos));
                } catch (...) {
                    throw std::runtime_error("Invalid number format at position: " + std::to_string(pos));
                }
                pos = num_end;
                if (pos >= offset_str.size()) {
                    throw std::runtime_error("Missing unit after number at position: " + std::to_string(pos));
                }
                char unit = offset_str[pos++];
                const int scaled_value = value * multiplier;
                switch (unit) {
                    case 'y': years   += scaled_value; break;
                    case 'M': months  += scaled_value; break;
                    case 'd': days    += scaled_value; break;
                    case 'h': hours   += scaled_value; break;
                    case 'm': minutes += scaled_value; break;
                    case 's': seconds += scaled_value; break;
                    default:
                        throw std::runtime_error("Invalid time unit: " + std::string(1, unit));
                }
            }
            return {years, months, days, hours, minutes, seconds};
        }
    };

    std::optional<OffsetConfig> offset_config;
};
