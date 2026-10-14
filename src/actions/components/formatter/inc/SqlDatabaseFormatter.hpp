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

#include "IFormatter.hpp"
#include "FormatterFactory.hpp"
#include <regex>


class SqlDatabaseFormatter final : public IDatabaseFormatter {
public:
    explicit SqlDatabaseFormatter(const DataFormat& format) : format_(format) {}

    FormatResult format(const CreateDatabaseConfig& config) const override {
        std::vector<std::string> stmts;
        if (config.tdengine.drop_if_exists) {
            stmts.push_back("DROP DATABASE IF EXISTS `" + config.tdengine.database + "`");
        }
        std::string create_stmt = "CREATE DATABASE IF NOT EXISTS `" + config.tdengine.database + "`";
        if (config.tdengine.properties.has_value()) {
            create_stmt += " " + config.tdengine.properties.value();
            static const std::regex precision_regex(R"(\bprecision\b)", std::regex_constants::icase);
            if (!std::regex_search(config.tdengine.properties.value(), precision_regex)) {
                create_stmt += " precision '" + config.tdengine.precision + "'";
            }
        } else {
            create_stmt += " precision '" + config.tdengine.precision + "'";
        }
        stmts.push_back(std::move(create_stmt));
        return stmts;
    }


private:
    const DataFormat& format_;

    // Register SqlDatabaseFormatter to FormatterFactory
    inline static bool registered_ = []() {
        FormatterFactory::register_formatter<CreateDatabaseConfig>(
            "sql",
            [](const DataFormat& format) {
                return std::make_unique<SqlDatabaseFormatter>(format);
            });
        return true;
    }();
};
