/*
 * Copyright (c) 2026 TAOS Data, Inc.
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

#include "IngestionValidator.hpp"
#include "IngestionCapacity.hpp"
#include "IngestionErrors.hpp"
#include "TagUtils.hpp"
#include "TypeConverter.hpp"
#include <sstream>
#include <stdexcept>
#include <unordered_map>

namespace {

template<typename T>
bool holds(const IngestColumnValues::Data& data) {
    return std::holds_alternative<T>(data);
}

std::string column_context(const std::string& table_name, const std::string& column_name) {
    return "table '" + table_name + "', column '" + column_name + "'";
}

} // namespace

IngestionValidator::IngestionValidator(SchemaConfig schema, IngressConfig config)
    : schema_(std::move(schema)), config_(std::move(config)) {
    schema_.apply();
    validate_schema();
}

IngestValidationResult IngestionValidator::validate(const IngestRequestData& request) const {
    if (request.session_id.empty()) {
        throw std::runtime_error("ingress request session_id must not be empty.");
    }
    if (request.tables.empty()) {
        throw std::runtime_error("ingress request must contain at least one table batch.");
    }
    if (request.tables.size() > config_.max_tables_per_request) {
        throw IngestionResourceExhaustedError("ingress request table count exceeds max_tables_per_request.");
    }

    size_t total_rows = 0;
    std::unordered_map<std::string, RowType> table_tags;
    for (const auto& table : request.tables) {
        const RowType normalized_tags = validate_table(table, total_rows);

        if (config_.auto_create_table) {
            auto insert_result = table_tags.emplace(table.table_name, normalized_tags);
            if (!insert_result.second && !TagUtils::row_type_equal(insert_result.first->second, normalized_tags)) {
                throw std::runtime_error("ingress request contains inconsistent tags for table '" + table.table_name + "'.");
            }
        }
    }

    if (total_rows > config_.rows_per_batch) {
        throw IngestionResourceExhaustedError("ingress request row count exceeds rows_per_batch.");
    }

    return IngestValidationResult{total_rows, request.tables.size()};
}

RowType IngestionValidator::normalize_tags(const RowType& tags, const std::string& context) const {
    return TagUtils::normalize_tag_row(schema_.tags, tags, context);
}

void IngestionValidator::validate_schema() const {
    if (schema_.name.empty()) {
        throw std::runtime_error("ingress schema name must not be empty.");
    }
    if (schema_.columns.empty()) {
        throw std::runtime_error("ingress schema must contain at least the first timestamp column.");
    }
    if (schema_.columns[0].type_tag != ColumnTypeTag::TIMESTAMP) {
        throw std::runtime_error("ingress schema first column must be TIMESTAMP.");
    }
}

RowType IngestionValidator::validate_table(const IngestTableBatch& table, size_t& total_rows) const {
    if (table.table_name.empty()) {
        throw std::runtime_error("ingress table_name must not be empty.");
    }

    validate_timestamp_values(table.timestamps, "table '" + table.table_name + "' timestamps");
    const size_t row_count = table.timestamps.row_count();
    if (row_count == 0) {
        throw std::runtime_error("ingress table '" + table.table_name + "' must contain at least one row.");
    }
    const size_t max_rows_per_table = effective_ingress_max_rows_per_table(config_);
    if (row_count > max_rows_per_table) {
        throw IngestionResourceExhaustedError(
            "ingress table '" + table.table_name + "' row count exceeds per-table capacity.");
    }

    const size_t expected_column_count = schema_.columns.size() - 1;
    if (table.columns.size() != expected_column_count) {
        std::ostringstream ss;
        ss << "ingress table '" << table.table_name << "' column count mismatch: expected "
           << expected_column_count << ", got " << table.columns.size() << ".";
        throw std::runtime_error(ss.str());
    }

    for (size_t i = 0; i < table.columns.size(); ++i) {
        const auto& column = schema_.columns[i + 1];
        validate_column(table.columns[i], column, row_count, column_context(table.table_name, column.name));
    }

    RowType normalized_tags = validate_tags(table);
    total_rows += row_count;
    return normalized_tags;
}

void IngestionValidator::validate_column(const IngestColumnValues& values,
                                         const ColumnConfig& column,
                                         size_t expected_rows,
                                         const std::string& context) const {
    if (values.row_count() != expected_rows) {
        throw std::runtime_error("ingress " + context + " row count mismatch.");
    }
    if (!values.nulls.empty() && values.nulls.size() != expected_rows) {
        throw std::runtime_error("ingress " + context + " null bitmap length mismatch.");
    }

    bool matched = false;
    switch (column.type_tag) {
        case ColumnTypeTag::BOOL:
            matched = holds<std::vector<bool>>(values.data);
            break;
        case ColumnTypeTag::TINYINT:
        case ColumnTypeTag::TINYINT_UNSIGNED:
        case ColumnTypeTag::SMALLINT:
        case ColumnTypeTag::SMALLINT_UNSIGNED:
        case ColumnTypeTag::INT:
        case ColumnTypeTag::INT_UNSIGNED:
            matched = holds<std::vector<int32_t>>(values.data);
            break;
        case ColumnTypeTag::TIMESTAMP:
            matched = holds<IngestTimestampValues>(values.data);
            if (matched) {
                validate_timestamp_values(std::get<IngestTimestampValues>(values.data), context);
            }
            break;
        case ColumnTypeTag::BIGINT:
        case ColumnTypeTag::BIGINT_UNSIGNED:
            matched = holds<std::vector<int64_t>>(values.data);
            break;
        case ColumnTypeTag::FLOAT:
            matched = holds<std::vector<float>>(values.data);
            break;
        case ColumnTypeTag::DOUBLE:
            matched = holds<std::vector<double>>(values.data);
            break;
        case ColumnTypeTag::NCHAR:
        case ColumnTypeTag::VARCHAR:
        case ColumnTypeTag::BINARY:
        case ColumnTypeTag::JSON:
            matched = holds<std::vector<std::string>>(values.data);
            break;
        case ColumnTypeTag::VARBINARY:
            matched = holds<IngestColumnValues::BinaryArray>(values.data);
            break;
        default:
            matched = false;
            break;
    }

    if (!matched) {
        throw std::runtime_error("ingress " + context + " type does not match fixed schema.");
    }
}

RowType IngestionValidator::validate_tags(const IngestTableBatch& table) const {
    if (!config_.auto_create_table) {
        if (!table.tags.empty()) {
            throw std::runtime_error(
                "ingress table '" + table.table_name + "' tags require ingress.auto_create_table=true.");
        }
        return {};
    }

    if (table.tags.empty()) {
        throw std::runtime_error(
            "ingress table '" + table.table_name + "' tags are required when ingress.auto_create_table=true.");
    }

    if (table.tags.size() != schema_.tags.size()) {
        throw std::runtime_error("ingress table '" + table.table_name + "' tag count mismatch.");
    }

    return normalize_tags(table.tags, "table '" + table.table_name + "'");
}

void IngestionValidator::validate_timestamp_values(const IngestTimestampValues& values,
                                                   const std::string& context) {
    if (!values.values.empty() && !values.string_values.empty()) {
        throw std::runtime_error("ingress " + context + " cannot contain both integer and string timestamp arrays.");
    }
}
