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
#include "taos.h"
#include <sstream>

class SqlSuperTableFormatter final : public ISuperTableFormatter {
public:
    explicit SqlSuperTableFormatter(const DataFormat& format) : format_(format) {}

    FormatResult format(const CreateSuperTableConfig& config) const override {
        std::ostringstream result;
        result << "CREATE TABLE IF NOT EXISTS `"
               << config.tdengine.database << "`.`"
               << config.schema.name << "` (`" << config.schema.columns[0].name << "` TIMESTAMP";

        // columns
        auto col_instances = ColumnConfigInstanceFactory::create(config.schema.columns_cfg.get_schema());
        if (!col_instances.empty()) {
            result << ", ";
            append_fields(result, col_instances, ", ");
        }
        result << ")";

        // tags
        auto tag_instances = ColumnConfigInstanceFactory::create(config.schema.tags_cfg.get_schema());
        if (!tag_instances.empty()) {
            result << " TAGS (";
            append_fields(result, tag_instances, ", ");
            result << ")";
        }

        result << ";";
        return result.str();
    }


private:
    const DataFormat& format_;

    template <typename T>
    void append_fields(std::ostringstream& result, const std::vector<T>& fields, const std::string& separator) const {
        for (size_t i = 0; i < fields.size(); ++i) {
            if (i > 0) {
                result << separator;
            }
            result << generate_column_or_tag(fields[i]);
        }
    }

    std::string generate_column_or_tag(const ColumnConfigInstance& field) const {
        std::ostringstream oss;
        oss << "`" << field.name() << "` " << field.type();

        const auto& config = field.config();

        // if (field.is_var_length()) {
        //     oss << "(" << field.len.value() << ")";
        // } else if (field.type_tag == ColumnTypeTag::DECIMAL) {
        //     oss << "(" << field.precision.value_or(10) << "," << field.scale.value_or(0) << ")";
        // }

        if (config.primary_key) {
            oss << " PRIMARY KEY";
        }
        if (config.properties.has_value()) {
            oss << " " << config.properties.value();
        }

        return oss.str();
    }


    inline static bool registered_ = []() {
        FormatterFactory::register_formatter<CreateSuperTableConfig>(
            "sql",
            [](const DataFormat& format) {
                return std::make_unique<SqlSuperTableFormatter>(format);
            });
        return true;
    }();
};