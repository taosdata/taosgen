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

#include <sstream>
#include "taos.h"
#include "IFormatter.hpp"
#include "FormatterFactory.hpp"


class SqlChildTableFormatter final : public IChildTableFormatter {
public:
    explicit SqlChildTableFormatter(const DataFormat& format) : format_(format) {}

    std::string format(const CreateChildTableConfig& config, const std::string& table_name, RowType tags, size_t index = 0) const {
        std::string result;
        result.reserve(1024 * 1024 * 2);

        if (index == 0) result += "CREATE TABLE";
        result += " IF NOT EXISTS `";
        result += config.tdengine.database;
        result += "`.`";
        result += table_name;
        result += "` USING `";
        result += config.tdengine.database;
        result += "`.`";
        result += config.schema.name;
        result += "` TAGS (";

        for (size_t i = 0; i < tags.size(); ++i) {
            if (i > 0) {
                result += ", ";
            }
            std::visit([&result](const auto& value) {
                using T = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<T, NullValue>) {
                    result += "NULL";
                } else if constexpr (std::is_same_v<T, NoneValue>) {
                    result += "NULL";
                } else if constexpr (std::is_same_v<T, std::string>) {
                    result += "'";
                    result += escape_single_quotes(value);
                    result += "'";
                } else if constexpr (std::is_same_v<T, std::u16string>) {
                    result += "'";
                    result += escape_single_quotes(StringUtils::u16string_to_utf8(value));
                    result += "'";
                } else if constexpr (std::is_same_v<T, bool>) {
                    result += value ? "true" : "false";
                } else if constexpr (std::is_arithmetic_v<T>) {
                    result += std::to_string(value);
                } else {
                    throw std::runtime_error("Unsupported tag value type for SQL formatting");
                }
            }, tags[i]);
        }

        result += ")";
        return result;
    }

    FormatResult format(const CreateChildTableConfig& config,
                        const std::vector<std::string>& table_names,
                        const std::vector<RowType>& tags) const override {

        std::ostringstream result;
        for (size_t i = 0; i < table_names.size(); ++i) {
            result << format(config, table_names[i], tags[i], i);
        }
        result << ";";
        return result.str();
    }


private:
    const DataFormat& format_;

    static std::string escape_single_quotes(const std::string& input) {
        std::string escaped;
        escaped.reserve(input.size() + 4);
        for (size_t i = 0; i < input.size(); ++i) {
            const char c = input[i];
            if (c == '\'') {
                escaped.push_back('\'');
            }
            escaped.push_back(c);
        }
        return escaped;
    }

    inline static bool registered_ = []() {
        FormatterFactory::register_formatter<CreateChildTableConfig>(
            "sql",
            [](const DataFormat& format) {
                return std::make_unique<SqlChildTableFormatter>(format);
            });
        return true;
    }();
};