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

#include "IngestionMemoryBlockBuilder.hpp"
#include "IngestionCapacity.hpp"
#include "IngestionErrors.hpp"
#include "TagUtils.hpp"
#include "TypeConverter.hpp"
#include "taosgen/ingress/ingestion.pb.h"
#include <algorithm>
#include <cstring>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <type_traits>
#include <unordered_map>
#include <utility>

namespace proto = taosgen::ingress;

namespace {

SchemaConfig prepare_schema(SchemaConfig schema) {
    schema.apply();
    return schema;
}

ColumnConfigVector data_columns(const SchemaConfig& schema) {
    if (schema.columns.size() <= 1) {
        return {};
    }
    return ColumnConfigVector(schema.columns.begin() + 1, schema.columns.end());
}

struct TableNameGeneratorBounds {
    uint64_t first;
    uint64_t last;
};

TableNameGeneratorBounds validated_generator_bounds(const TableNameConfig::Generator& generator) {
    if (generator.from < 0 || generator.count <= 0) {
        throw std::runtime_error("ingress schema.tbname generator range is invalid.");
    }

    const uint64_t first = static_cast<uint64_t>(generator.from);
    const uint64_t count = static_cast<uint64_t>(generator.count);
    if (first > std::numeric_limits<uint64_t>::max() - count) {
        throw std::runtime_error("ingress schema.tbname generator range exceeds supported size.");
    }

    return TableNameGeneratorBounds{first, first + count};
}

int64_t timestamp_at(const IngestTimestampValues& values, size_t row, const ColumnConfig& config) {
    if (!values.values.empty()) {
        return values.values[row];
    }

    ColumnType parsed = TypeConverter::convert_to_type(values.string_values[row], config);
    return std::get<int64_t>(parsed);
}

void set_fixed_null(MemoryPool::TableBase::Column& column, size_t row) {
    column.is_nulls[row] = 1;
}

void set_var_null(MemoryPool::TableBase::Column& column, size_t row) {
    column.is_nulls[row] = 1;
    column.lengths[row] = 0;
    column.var_offsets[row] = column.current_offset;
}

bool is_null_at(const IngestColumnValues& values, size_t row) {
    return !values.nulls.empty() && values.nulls[row];
}

bool is_null_at(const proto::ColumnValues& values, size_t row) {
    return values.nulls_size() > 0 && values.nulls(static_cast<int>(row));
}

size_t proto_timestamp_row_count(const proto::TimestampValues& values) {
    return values.values_size() > 0 ? static_cast<size_t>(values.values_size())
                                    : static_cast<size_t>(values.string_values_size());
}

size_t proto_column_row_count(const proto::ColumnValues& values) {
    switch (values.values_case()) {
        case proto::ColumnValues::kBoolValues:
            return static_cast<size_t>(values.bool_values().values_size());
        case proto::ColumnValues::kIntValues:
            return static_cast<size_t>(values.int_values().values_size());
        case proto::ColumnValues::kBigintValues:
            return static_cast<size_t>(values.bigint_values().values_size());
        case proto::ColumnValues::kFloatValues:
            return static_cast<size_t>(values.float_values().values_size());
        case proto::ColumnValues::kDoubleValues:
            return static_cast<size_t>(values.double_values().values_size());
        case proto::ColumnValues::kStringValues:
            return static_cast<size_t>(values.string_values().values_size());
        case proto::ColumnValues::kBinaryValues:
            return static_cast<size_t>(values.binary_values().values_size());
        case proto::ColumnValues::kTimestampValues:
            return proto_timestamp_row_count(values.timestamp_values());
        case proto::ColumnValues::VALUES_NOT_SET:
            throw std::runtime_error("ingress column values must be set.");
    }

    throw std::runtime_error("unsupported ingress column values.");
}

size_t proto_interlace_table_count(const proto::TableNames& table_names) {
    switch (table_names.source_case()) {
        case proto::TableNames::kExplicitNames:
            return static_cast<size_t>(table_names.explicit_names().values_size());
        case proto::TableNames::kRange:
            return static_cast<size_t>(table_names.range().count());
        case proto::TableNames::SOURCE_NOT_SET:
            throw std::runtime_error("ingress interlace_batch table_names must be set.");
    }

    throw std::runtime_error("unsupported ingress interlace_batch table_names.");
}

size_t proto_interlace_total_rows(const proto::InterlaceBatch& batch) {
    const size_t table_count = proto_interlace_table_count(batch.table_names());
    const size_t rows_per_table = static_cast<size_t>(batch.rows_per_table());
    if (table_count == 0) {
        throw std::runtime_error("ingress interlace_batch must contain at least one table.");
    }
    if (rows_per_table == 0) {
        throw std::runtime_error("ingress interlace_batch rows_per_table must be greater than zero.");
    }
    if (table_count > std::numeric_limits<size_t>::max() / rows_per_table) {
        throw IngestionResourceExhaustedError("ingress interlace_batch row count exceeds supported size.");
    }
    return table_count * rows_per_table;
}

uint64_t proto_table_range_step(const proto::TableNameRange& range) {
    return range.step() == 0 ? 1 : range.step();
}

void validate_proto_interlace_table_names(const proto::TableNames& table_names) {
    switch (table_names.source_case()) {
        case proto::TableNames::kExplicitNames: {
            const auto& names = table_names.explicit_names().values();
            for (int i = 0; i < names.size(); ++i) {
                if (names.Get(i).empty()) {
                    throw std::runtime_error("ingress table_name must not be empty.");
                }
            }
            return;
        }
        case proto::TableNames::kRange: {
            const auto& range = table_names.range();
            if (range.prefix().empty()) {
                throw std::runtime_error("ingress table_name range prefix must not be empty.");
            }
            const uint64_t count = static_cast<uint64_t>(range.count());
            if (count == 0) {
                throw std::runtime_error("ingress interlace_batch must contain at least one table.");
            }
            const uint64_t step = proto_table_range_step(range);
            if (count > 1 && (count - 1) > (std::numeric_limits<uint64_t>::max() - range.start()) / step) {
                throw IngestionResourceExhaustedError("ingress table_name range exceeds supported size.");
            }
            return;
        }
        case proto::TableNames::SOURCE_NOT_SET:
            throw std::runtime_error("ingress interlace_batch table_names must be set.");
    }

    throw std::runtime_error("unsupported ingress interlace_batch table_names.");
}

void validate_proto_timestamps(const proto::TimestampValues& values, const std::string& context) {
    if (values.values_size() > 0 && values.string_values_size() > 0) {
        throw std::runtime_error("ingress " + context + " cannot contain both integer and string timestamp arrays.");
    }
}

std::string column_context(const ColumnConfigInstance& column_instance) {
    return "column '" + column_instance.name() + "'";
}

RowType normalize_tags_or_throw(const ColumnConfigVector& schema_tags,
                                const RowType& tags,
                                const std::string& table_name) {
    return TagUtils::normalize_tag_row(schema_tags, tags, "table '" + table_name + "'");
}

template <typename T>
void copy_fixed_value(MemoryPool::TableBase::Column& column, size_t row, T value) {
    void* dest = static_cast<char*>(column.fixed_data) + row * column.element_size;
    std::memcpy(dest, &value, sizeof(T));
}

void copy_var_value(MemoryPool::TableBase::Column& column, size_t row, const char* data, size_t size) {
    column.is_nulls[row] = 0;
    column.var_offsets[row] = column.current_offset;

    if (size > column.max_length) {
        throw std::runtime_error("variable-length value exceeds schema capacity");
    }

    const size_t copy_size = size;
    if (copy_size > 0) {
        std::memcpy(column.var_data + column.current_offset, data, copy_size);
    }

    column.lengths[row] = static_cast<int32_t>(copy_size);
    column.current_offset += copy_size;
}

template <typename T>
void copy_proto_fixed_values(MemoryPool::TableBase::Column& column,
                             const google::protobuf::RepeatedField<T>& values,
                             size_t row_count) {
    if (row_count > 0) {
        std::memcpy(column.fixed_data, values.data(), row_count * sizeof(T));
    }
    std::memset(column.is_nulls, 0, row_count);
}

template <typename T>
void copy_proto_fixed_values_segment(MemoryPool::TableBase::Column& column,
                                     const google::protobuf::RepeatedField<T>& values,
                                     size_t offset,
                                     size_t row_count) {
    if (row_count > 0) {
        std::memcpy(column.fixed_data, values.data() + offset, row_count * sizeof(T));
    }
    std::memset(column.is_nulls, 0, row_count);
}

template <typename T>
const std::vector<T>& column_vector(const IngestColumnValues& values,
                                    const ColumnConfigInstance& column_instance) {
    const auto* data = std::get_if<std::vector<T>>(&values.data);
    if (data == nullptr) {
        throw std::runtime_error("ingress " + column_context(column_instance) + " type does not match fixed schema.");
    }
    return *data;
}

const IngestTimestampValues& timestamp_column(const IngestColumnValues& values,
                                             const ColumnConfigInstance& column_instance) {
    const auto* data = std::get_if<IngestTimestampValues>(&values.data);
    if (data == nullptr) {
        throw std::runtime_error("ingress " + column_context(column_instance) + " type does not match fixed schema.");
    }
    return *data;
}

const IngestColumnValues::BinaryArray& binary_column(const IngestColumnValues& values,
                                                    const ColumnConfigInstance& column_instance) {
    const auto* data = std::get_if<IngestColumnValues::BinaryArray>(&values.data);
    if (data == nullptr) {
        throw std::runtime_error("ingress " + column_context(column_instance) + " type does not match fixed schema.");
    }
    return *data;
}

bool proto_column_matches(const proto::ColumnValues& values, ColumnTypeTag tag) {
    switch (tag) {
        case ColumnTypeTag::BOOL:
            return values.values_case() == proto::ColumnValues::kBoolValues;
        case ColumnTypeTag::TINYINT:
        case ColumnTypeTag::TINYINT_UNSIGNED:
        case ColumnTypeTag::SMALLINT:
        case ColumnTypeTag::SMALLINT_UNSIGNED:
        case ColumnTypeTag::INT:
        case ColumnTypeTag::INT_UNSIGNED:
            return values.values_case() == proto::ColumnValues::kIntValues;
        case ColumnTypeTag::TIMESTAMP:
            return values.values_case() == proto::ColumnValues::kTimestampValues;
        case ColumnTypeTag::BIGINT:
        case ColumnTypeTag::BIGINT_UNSIGNED:
            return values.values_case() == proto::ColumnValues::kBigintValues;
        case ColumnTypeTag::FLOAT:
            return values.values_case() == proto::ColumnValues::kFloatValues;
        case ColumnTypeTag::DOUBLE:
            return values.values_case() == proto::ColumnValues::kDoubleValues;
        case ColumnTypeTag::NCHAR:
        case ColumnTypeTag::VARCHAR:
        case ColumnTypeTag::BINARY:
        case ColumnTypeTag::JSON:
            return values.values_case() == proto::ColumnValues::kStringValues;
        case ColumnTypeTag::VARBINARY:
            return values.values_case() == proto::ColumnValues::kBinaryValues;
        default:
            return false;
    }
}

ColumnType proto_tag_value(const proto::Value& input) {
    switch (input.kind_case()) {
        case proto::Value::kBoolValue:
            return input.bool_value();
        case proto::Value::kIntValue:
            return input.int_value();
        case proto::Value::kBigintValue:
            return input.bigint_value();
        case proto::Value::kFloatValue:
            return input.float_value();
        case proto::Value::kDoubleValue:
            return input.double_value();
        case proto::Value::kStringValue:
            return input.string_value();
        case proto::Value::kBinaryValue:
            return std::vector<uint8_t>(input.binary_value().begin(), input.binary_value().end());
        case proto::Value::kTimestampValue:
            return input.timestamp_value();
        case proto::Value::kNullValue:
            return NullValue{};
        case proto::Value::KIND_NOT_SET:
            throw std::runtime_error("ingress tag value must be set.");
    }

    throw std::runtime_error("unsupported ingress tag value.");
}

} // namespace

IngestionMemoryBlockBuilder::IngestionMemoryBlockBuilder(SchemaConfig schema, IngressConfig config)
    : schema_(prepare_schema(std::move(schema))),
      config_(std::move(config)),
      validator_(schema_, config_) {
    validate_schema_for_builder();
    col_instances_ = ColumnConfigInstanceFactory::create(data_columns(schema_));
    if (config_.auto_create_table) {
        tag_instances_ = ColumnConfigInstanceFactory::create(schema_.tags);
    }
}

IngestValidationResult IngestionMemoryBlockBuilder::validate(const IngestRequestData& request) const {
    return validator_.validate(request);
}

MemoryPool::MemoryBlock* IngestionMemoryBlockBuilder::build(const IngestRequestData& request,
                                                            MemoryPool& pool) const {
    IngestValidationResult result = validator_.validate(request);
    MemoryPool::MemoryBlock* block = pool.acquire_block();

    try {
        if (request.tables.size() > block->tables.size()) {
            throw std::runtime_error("ingress request table count exceeds memory block capacity.");
        }

        block->table_name_storage_.reserve(request.tables.size());
        block->used_tables = request.tables.size();
        block->total_rows = result.row_count;

        std::vector<PendingTagRegistration> pending_tags;
        pending_tags.reserve(config_.auto_create_table ? request.tables.size() : 0);
        for (size_t table_index = 0; table_index < request.tables.size(); ++table_index) {
            fill_table(*block, table_index, request.tables[table_index], pool, pending_tags);
        }
        commit_pending_tags(*block, pool, pending_tags);

        block->build_bindv(false);
        return block;
    } catch (...) {
        pool.release_block(block);
        throw;
    }
}

MemoryPool::MemoryBlock* IngestionMemoryBlockBuilder::build(IngestRequestData&& request,
                                                            MemoryPool& pool) const {
    IngestValidationResult result = validator_.validate(request);
    MemoryPool::MemoryBlock* block = pool.acquire_block();

    try {
        if (request.tables.size() > block->tables.size()) {
            throw std::runtime_error("ingress request table count exceeds memory block capacity.");
        }

        block->table_name_storage_.reserve(request.tables.size());
        block->used_tables = request.tables.size();
        block->total_rows = result.row_count;

        std::vector<PendingTagRegistration> pending_tags;
        pending_tags.reserve(config_.auto_create_table ? request.tables.size() : 0);
        for (size_t table_index = 0; table_index < request.tables.size(); ++table_index) {
            fill_table(*block, table_index, request.tables[table_index], pool, true, pending_tags);
        }
        commit_pending_tags(*block, pool, pending_tags);

        block->build_bindv(false);
        return block;
    } catch (...) {
        pool.release_block(block);
        throw;
    }
}

IngestValidationResult IngestionMemoryBlockBuilder::validate_proto(const proto::IngestRequest& request) const {
    if (request.session_id().empty()) {
        throw std::runtime_error("ingress request session_id must not be empty.");
    }
    if (request.tables_size() > 0 && request.has_interlace_batch()) {
        throw std::runtime_error("ingress request cannot contain both tables and interlace_batch.");
    }
    if (request.tables_size() == 0 && !request.has_interlace_batch()) {
        throw std::runtime_error("ingress request must contain tables or interlace_batch.");
    }

    if (request.has_interlace_batch()) {
        if (config_.auto_create_table) {
            throw std::runtime_error("ingress interlace_batch does not support auto_create_table.");
        }
        const auto& batch = request.interlace_batch();
        validate_proto_interlace_table_names(batch.table_names());
        const size_t table_count = proto_interlace_table_count(batch.table_names());
        const size_t max_rows_per_table = effective_ingress_max_rows_per_table(config_);
        if (table_count > config_.max_tables_per_request) {
            throw IngestionResourceExhaustedError("ingress request table count exceeds max_tables_per_request.");
        }
        const size_t total_rows = proto_interlace_total_rows(batch);
        if (total_rows > config_.rows_per_batch) {
            throw IngestionResourceExhaustedError("ingress request row count exceeds rows_per_batch.");
        }
        if (static_cast<size_t>(batch.rows_per_table()) > max_rows_per_table) {
            throw IngestionResourceExhaustedError("ingress interlace_batch rows_per_table exceeds per-table capacity.");
        }
        const size_t expected_column_count = schema_.columns.size() - 1;
        if (static_cast<size_t>(batch.columns_size()) != expected_column_count) {
            std::ostringstream ss;
            ss << "ingress interlace_batch column count mismatch: expected "
               << expected_column_count << ", got " << batch.columns_size() << ".";
            throw std::runtime_error(ss.str());
        }

        validate_proto_timestamps(batch.timestamps(), "interlace_batch timestamps");
        if (proto_timestamp_row_count(batch.timestamps()) != total_rows) {
            throw std::runtime_error("ingress interlace_batch timestamp row count mismatch.");
        }

        for (int i = 0; i < batch.columns_size(); ++i) {
            const auto& proto_column = batch.columns(i);
            const auto& column = schema_.columns[static_cast<size_t>(i) + 1];
            if (proto_column.nulls_size() > 0 && static_cast<size_t>(proto_column.nulls_size()) != total_rows) {
                throw std::runtime_error("ingress interlace_batch column '" +
                                         column.name + "' null bitmap length mismatch.");
            }
            if (proto_column_row_count(proto_column) != total_rows) {
                throw std::runtime_error("ingress interlace_batch column '" + column.name + "' row count mismatch.");
            }
            if (!proto_column_matches(proto_column, column.type_tag)) {
                throw std::runtime_error("ingress interlace_batch column '" +
                                         column.name + "' type does not match fixed schema.");
            }
            if (column.type_tag == ColumnTypeTag::TIMESTAMP) {
                validate_proto_timestamps(proto_column.timestamp_values(),
                                          "interlace_batch column '" + column.name + "'");
            }
        }

        return IngestValidationResult{total_rows, table_count};
    }
    if (static_cast<size_t>(request.tables_size()) > config_.max_tables_per_request) {
        throw IngestionResourceExhaustedError("ingress request table count exceeds max_tables_per_request.");
    }

    const size_t expected_column_count = schema_.columns.size() - 1;
    const size_t max_rows_per_table = effective_ingress_max_rows_per_table(config_);
    size_t total_rows = 0;
    std::unordered_map<std::string, RowType> table_tags;

    for (const auto& table : request.tables()) {
        if (table.table_name().empty()) {
            throw std::runtime_error("ingress table_name must not be empty.");
        }

        const std::string table_context = "table '" + table.table_name() + "'";
        validate_proto_timestamps(table.timestamps(), table_context + " timestamps");
        const size_t row_count = proto_timestamp_row_count(table.timestamps());
        if (row_count == 0) {
            throw std::runtime_error("ingress " + table_context + " must contain at least one row.");
        }
        if (row_count > max_rows_per_table) {
            throw IngestionResourceExhaustedError("ingress " + table_context + " row count exceeds per-table capacity.");
        }

        if (static_cast<size_t>(table.columns_size()) != expected_column_count) {
            std::ostringstream ss;
            ss << "ingress table '" << table.table_name() << "' column count mismatch: expected "
               << expected_column_count << ", got " << table.columns_size() << ".";
            throw std::runtime_error(ss.str());
        }

        for (int i = 0; i < table.columns_size(); ++i) {
            const auto& proto_column = table.columns(i);
            const auto& column = schema_.columns[static_cast<size_t>(i) + 1];
            if (proto_column.nulls_size() > 0 && static_cast<size_t>(proto_column.nulls_size()) != row_count) {
                throw std::runtime_error("ingress table '" + table.table_name() + "', column '" +
                                         column.name + "' null bitmap length mismatch.");
            }
            if (proto_column_row_count(proto_column) != row_count) {
                throw std::runtime_error("ingress table '" + table.table_name() + "', column '" +
                                         column.name + "' row count mismatch.");
            }
            if (!proto_column_matches(proto_column, column.type_tag)) {
                throw std::runtime_error("ingress table '" + table.table_name() + "', column '" +
                                         column.name + "' type does not match fixed schema.");
            }
            if (column.type_tag == ColumnTypeTag::TIMESTAMP) {
                validate_proto_timestamps(proto_column.timestamp_values(),
                                          "table '" + table.table_name() + "', column '" + column.name + "'");
            }
        }

        if (!config_.auto_create_table) {
            if (table.tags_size() > 0) {
                throw std::runtime_error(
                    "ingress table '" + table.table_name() + "' tags require ingress.auto_create_table=true.");
            }
        } else {
            if (table.tags_size() == 0) {
                throw std::runtime_error(
                    "ingress table '" + table.table_name() + "' tags are required when ingress.auto_create_table=true.");
            }
            if (static_cast<size_t>(table.tags_size()) != schema_.tags.size()) {
                throw std::runtime_error("ingress table '" + table.table_name() + "' tag count mismatch.");
            }

            RowType tags;
            tags.reserve(static_cast<size_t>(table.tags_size()));
            for (int i = 0; i < table.tags_size(); ++i) {
                tags.push_back(proto_tag_value(table.tags(i)));
            }
            RowType normalized_tags = normalize_tags_or_throw(schema_.tags, tags, table.table_name());
            auto insert_result = table_tags.emplace(table.table_name(), std::move(normalized_tags));
            if (!insert_result.second) {
                if (!TagUtils::row_type_equal(insert_result.first->second,
                                              normalize_tags_or_throw(schema_.tags, tags, table.table_name()))) {
                    throw std::runtime_error(
                        "ingress request contains inconsistent tags for table '" + table.table_name() + "'.");
                }
            }
        }

        total_rows += row_count;
        if (total_rows > config_.rows_per_batch) {
            throw IngestionResourceExhaustedError("ingress request row count exceeds rows_per_batch.");
        }
    }

    return IngestValidationResult{total_rows, static_cast<size_t>(request.tables_size())};
}

MemoryPool::MemoryBlock* IngestionMemoryBlockBuilder::build_proto(const proto::IngestRequest& request,
                                                                  MemoryPool& pool) const {
    // Keep the write path single-pass: accept_only calls validate_proto(), while
    // enqueue/write fills and validates the block directly below.
    if (request.session_id().empty()) {
        throw std::runtime_error("ingress request session_id must not be empty.");
    }
    if (request.tables_size() > 0 && request.has_interlace_batch()) {
        throw std::runtime_error("ingress request cannot contain both tables and interlace_batch.");
    }
    if (request.tables_size() == 0 && !request.has_interlace_batch()) {
        throw std::runtime_error("ingress request must contain tables or interlace_batch.");
    }

    const size_t required_rows_per_table = request.has_interlace_batch()
        ? static_cast<size_t>(request.interlace_batch().rows_per_table())
        : [&request] {
            size_t max_rows = 0;
            for (const auto& table : request.tables()) {
                max_rows = std::max(max_rows, proto_timestamp_row_count(table.timestamps()));
            }
            return max_rows;
        }();
    const size_t max_rows_per_table = effective_ingress_max_rows_per_table(config_);

    if (required_rows_per_table > max_rows_per_table) {
        throw IngestionResourceExhaustedError("ingress request row count exceeds per-table capacity.");
    }

    if (request.has_interlace_batch()) {
        if (config_.auto_create_table) {
            throw std::runtime_error("ingress interlace_batch does not support auto_create_table.");
        }
        MemoryPool::MemoryBlock* block = pool.acquire_block();
        try {
            fill_proto_interlace_batch(*block, request.interlace_batch(), pool);
            block->build_bindv(false);
            return block;
        } catch (...) {
            pool.release_block(block);
            throw;
        }
    }
    if (static_cast<size_t>(request.tables_size()) > config_.max_tables_per_request) {
        throw IngestionResourceExhaustedError("ingress request table count exceeds max_tables_per_request.");
    }

    MemoryPool::MemoryBlock* block = pool.acquire_block();

    try {
        block->used_tables = static_cast<size_t>(request.tables_size());
        block->table_name_storage_.reserve(static_cast<size_t>(request.tables_size()));
        size_t total_rows = 0;
        std::vector<PendingTagRegistration> pending_tags;
        pending_tags.reserve(config_.auto_create_table ? static_cast<size_t>(request.tables_size()) : 0);
        for (int table_index = 0; table_index < request.tables_size(); ++table_index) {
            fill_proto_table(*block,
                             static_cast<size_t>(table_index),
                             request.tables(table_index),
                             total_rows,
                             pending_tags);
            if (total_rows > config_.rows_per_batch) {
                throw IngestionResourceExhaustedError("ingress request row count exceeds rows_per_batch.");
            }
        }
        block->total_rows = total_rows;
        commit_pending_tags(*block, pool, pending_tags);

        block->build_bindv(false);
        return block;
    } catch (...) {
        pool.release_block(block);
        throw;
    }
}

void IngestionMemoryBlockBuilder::validate_schema_for_builder() const {
    for (const auto& column : schema_.columns) {
        if (column.count != 1) {
            throw std::runtime_error("ingress schema does not support column count expansion.");
        }
    }
    for (const auto& tag : schema_.tags) {
        if (tag.count != 1) {
            throw std::runtime_error("ingress schema does not support tag count expansion.");
        }
    }
}

void IngestionMemoryBlockBuilder::fill_table(MemoryPool::MemoryBlock& block,
                                             size_t table_index,
                                             const IngestTableBatch& table,
                                             MemoryPool& pool,
                                             std::vector<PendingTagRegistration>& pending_tags) const {
    auto& table_block = block.tables[table_index];
    const size_t row_count = table.timestamps.row_count();
    if (row_count > table_block.max_rows) {
        throw IngestionResourceExhaustedError(
            "ingress table '" + table.table_name + "' row count exceeds per-table capacity.");
    }

    block.table_name_storage_.push_back(table.table_name);
    table_block.table_name = block.table_name_storage_.back().c_str();

    if (config_.auto_create_table && !table.tags.empty()) {
        pending_tags.push_back(PendingTagRegistration{
            table_index,
            table.table_name,
            TagUtils::normalize_tag_row(schema_.tags,
                                        table.tags,
                                        "table '" + table.table_name + "'")
        });
        table_block.tags_ptr = nullptr;
    } else if (config_.auto_create_table) {
        table_block.tags_ptr = pool.get_table_tags(table.table_name);
    } else {
        table_block.tags_ptr = nullptr;
    }

    fill_primary_timestamps(block, table_block, table.timestamps);

    for (size_t col_idx = 0; col_idx < table.columns.size(); ++col_idx) {
        fill_column(table_block.columns[col_idx],
                    table.columns[col_idx],
                    schema_.columns[col_idx + 1],
                    col_instances_[col_idx],
                    row_count);
    }

    table_block.used_rows = row_count;
}

void IngestionMemoryBlockBuilder::fill_table(MemoryPool::MemoryBlock& block,
                                             size_t table_index,
                                             IngestTableBatch& table,
                                             MemoryPool& pool,
                                             bool move_table_name,
                                             std::vector<PendingTagRegistration>& pending_tags) const {
    auto& table_block = block.tables[table_index];
    const size_t row_count = table.timestamps.row_count();
    if (row_count > table_block.max_rows) {
        throw IngestionResourceExhaustedError(
            "ingress table '" + table.table_name + "' row count exceeds per-table capacity.");
    }

    if (move_table_name) {
        block.table_name_storage_.push_back(std::move(table.table_name));
    } else {
        block.table_name_storage_.push_back(table.table_name);
    }
    const std::string& stored_table_name = block.table_name_storage_.back();
    table_block.table_name = stored_table_name.c_str();

    if (config_.auto_create_table && !table.tags.empty()) {
        pending_tags.push_back(PendingTagRegistration{
            table_index,
            stored_table_name,
            TagUtils::normalize_tag_row(schema_.tags,
                                        table.tags,
                                        "table '" + stored_table_name + "'")
        });
        table_block.tags_ptr = nullptr;
    } else if (config_.auto_create_table) {
        table_block.tags_ptr = pool.get_table_tags(stored_table_name);
    } else {
        table_block.tags_ptr = nullptr;
    }

    fill_primary_timestamps(block, table_block, table.timestamps);

    for (size_t col_idx = 0; col_idx < table.columns.size(); ++col_idx) {
        fill_column(table_block.columns[col_idx],
                    table.columns[col_idx],
                    schema_.columns[col_idx + 1],
                    col_instances_[col_idx],
                    row_count);
    }

    table_block.used_rows = row_count;
}

void IngestionMemoryBlockBuilder::fill_proto_table(MemoryPool::MemoryBlock& block,
                                                   size_t table_index,
                                                   const proto::TableBatch& table,
                                                   size_t& total_rows,
                                                   std::vector<PendingTagRegistration>& pending_tags) const {
    if (table.table_name().empty()) {
        throw std::runtime_error("ingress table_name must not be empty.");
    }

    auto& table_block = block.tables[table_index];
    const size_t row_count = proto_timestamp_row_count(table.timestamps());
    validate_proto_timestamps(table.timestamps(), "table '" + table.table_name() + "' timestamps");
    if (row_count == 0) {
        throw std::runtime_error("ingress table '" + table.table_name() + "' must contain at least one row.");
    }
    if (row_count > table_block.max_rows) {
        throw IngestionResourceExhaustedError(
            "ingress table '" + table.table_name() + "' row count exceeds per-table capacity.");
    }

    const size_t expected_column_count = schema_.columns.size() - 1;
    if (static_cast<size_t>(table.columns_size()) != expected_column_count) {
        std::ostringstream ss;
        ss << "ingress table '" << table.table_name() << "' column count mismatch: expected "
           << expected_column_count << ", got " << table.columns_size() << ".";
        throw std::runtime_error(ss.str());
    }

    block.table_name_storage_.push_back(table.table_name());
    const std::string& stored_table_name = block.table_name_storage_.back();
    table_block.table_name = stored_table_name.c_str();

    if (!config_.auto_create_table) {
        if (table.tags_size() > 0) {
            throw std::runtime_error(
                "ingress table '" + stored_table_name + "' tags require ingress.auto_create_table=true.");
        }
        table_block.tags_ptr = nullptr;
    } else {
        if (table.tags_size() == 0) {
            throw std::runtime_error(
                "ingress table '" + stored_table_name + "' tags are required when ingress.auto_create_table=true.");
        }
        if (static_cast<size_t>(table.tags_size()) != schema_.tags.size()) {
            throw std::runtime_error("ingress table '" + stored_table_name + "' tag count mismatch.");
        }

        RowType tags;
        tags.reserve(static_cast<size_t>(table.tags_size()));
        for (int i = 0; i < table.tags_size(); ++i) {
            tags.push_back(proto_tag_value(table.tags(i)));
        }
        pending_tags.push_back(PendingTagRegistration{
            table_index,
            stored_table_name,
            normalize_tags_or_throw(schema_.tags, tags, stored_table_name)
        });
        table_block.tags_ptr = nullptr;
    }

    fill_proto_primary_timestamps(block, table_block, table.timestamps());

    for (int col_idx = 0; col_idx < table.columns_size(); ++col_idx) {
        fill_proto_column(table_block.columns[static_cast<size_t>(col_idx)],
                          table.columns(col_idx),
                          schema_.columns[static_cast<size_t>(col_idx) + 1],
                          col_instances_[static_cast<size_t>(col_idx)],
                          row_count);
    }

    table_block.used_rows = row_count;
    total_rows += row_count;
}

void IngestionMemoryBlockBuilder::fill_proto_primary_timestamps(MemoryPool::MemoryBlock& block,
                                                                MemoryPool::TableBlock& table_block,
                                                                const proto::TimestampValues& timestamps) const {
    const size_t row_count = proto_timestamp_row_count(timestamps);
    const auto& ts_config = schema_.columns[0];

    if (timestamps.values_size() > 0) {
        if (row_count > 0) {
            std::memcpy(table_block.timestamps, timestamps.values().data(), row_count * sizeof(int64_t));
        }
        for (size_t row = 0; row < row_count; ++row) {
            const int64_t timestamp = table_block.timestamps[row];
            block.start_time = std::min(block.start_time, timestamp);
            block.end_time = std::max(block.end_time, timestamp);
        }
        return;
    }

    for (size_t row = 0; row < row_count; ++row) {
        ColumnType parsed = TypeConverter::convert_to_type(timestamps.string_values(static_cast<int>(row)), ts_config);
        const int64_t timestamp = std::get<int64_t>(parsed);
        table_block.timestamps[row] = timestamp;
        block.start_time = std::min(block.start_time, timestamp);
        block.end_time = std::max(block.end_time, timestamp);
    }
}

void IngestionMemoryBlockBuilder::fill_proto_column(MemoryPool::TableBase::Column& column_block,
                                                    const proto::ColumnValues& values,
                                                    const ColumnConfig& column_config,
                                                    const ColumnConfigInstance& column_instance,
                                                    size_t row_count) const {
    if (values.nulls_size() > 0 && static_cast<size_t>(values.nulls_size()) != row_count) {
        throw std::runtime_error("ingress " + column_context(column_instance) + " null bitmap length mismatch.");
    }
    if (proto_column_row_count(values) != row_count) {
        throw std::runtime_error("ingress " + column_context(column_instance) + " row count mismatch.");
    }

    if (column_config.is_var_length()) {
        switch (column_config.type_tag) {
            case ColumnTypeTag::NCHAR:
            case ColumnTypeTag::VARCHAR:
            case ColumnTypeTag::BINARY:
            case ColumnTypeTag::JSON: {
                if (values.values_case() != proto::ColumnValues::kStringValues) {
                    throw std::runtime_error("ingress " + column_context(column_instance) + " type does not match fixed schema.");
                }
                const auto& typed_values = values.string_values().values();
                for (size_t row = 0; row < row_count; ++row) {
                    if (is_null_at(values, row)) {
                        set_var_null(column_block, row);
                    } else {
                        const std::string& value = typed_values.Get(static_cast<int>(row));
                        copy_var_value(column_block, row, value.data(), value.size());
                    }
                }
                return;
            }
            case ColumnTypeTag::VARBINARY: {
                if (values.values_case() != proto::ColumnValues::kBinaryValues) {
                    throw std::runtime_error("ingress " + column_context(column_instance) + " type does not match fixed schema.");
                }
                const auto& typed_values = values.binary_values().values();
                for (size_t row = 0; row < row_count; ++row) {
                    if (is_null_at(values, row)) {
                        set_var_null(column_block, row);
                    } else {
                        const std::string& bytes = typed_values.Get(static_cast<int>(row));
                        copy_var_value(column_block, row, bytes.data(), bytes.size());
                    }
                }
                return;
            }
            default:
                throw std::runtime_error("ingress " + column_context(column_instance) + " uses unsupported variable-length type.");
        }
    }

    const bool has_nulls = values.nulls_size() > 0;
    switch (column_config.type_tag) {
        case ColumnTypeTag::BOOL: {
            if (values.values_case() != proto::ColumnValues::kBoolValues) {
                throw std::runtime_error("ingress " + column_context(column_instance) + " type does not match fixed schema.");
            }
            const auto& typed_values = values.bool_values().values();
            for (size_t row = 0; row < row_count; ++row) {
                if (is_null_at(values, row)) {
                    set_fixed_null(column_block, row);
                } else {
                    column_block.is_nulls[row] = 0;
                    copy_fixed_value(column_block, row, static_cast<bool>(typed_values.Get(static_cast<int>(row))));
                }
            }
            return;
        }
        case ColumnTypeTag::TINYINT:
        case ColumnTypeTag::TINYINT_UNSIGNED:
        case ColumnTypeTag::SMALLINT:
        case ColumnTypeTag::SMALLINT_UNSIGNED:
        case ColumnTypeTag::INT:
        case ColumnTypeTag::INT_UNSIGNED: {
            if (values.values_case() != proto::ColumnValues::kIntValues) {
                throw std::runtime_error("ingress " + column_context(column_instance) + " type does not match fixed schema.");
            }
            const auto& typed_values = values.int_values().values();
            if (!has_nulls && column_config.type_tag == ColumnTypeTag::INT) {
                copy_proto_fixed_values(column_block, typed_values, row_count);
                return;
            }
            for (size_t row = 0; row < row_count; ++row) {
                if (is_null_at(values, row)) {
                    set_fixed_null(column_block, row);
                    continue;
                }

                column_block.is_nulls[row] = 0;
                int32_t value = typed_values.Get(static_cast<int>(row));
                switch (column_config.type_tag) {
                    case ColumnTypeTag::TINYINT:
                        copy_fixed_value(column_block, row, TagUtils::checked_cast<int8_t>(value, column_context(column_instance)));
                        break;
                    case ColumnTypeTag::TINYINT_UNSIGNED:
                        copy_fixed_value(column_block, row, TagUtils::checked_cast<uint8_t>(value, column_context(column_instance)));
                        break;
                    case ColumnTypeTag::SMALLINT:
                        copy_fixed_value(column_block, row, TagUtils::checked_cast<int16_t>(value, column_context(column_instance)));
                        break;
                    case ColumnTypeTag::SMALLINT_UNSIGNED:
                        copy_fixed_value(column_block, row, TagUtils::checked_cast<uint16_t>(value, column_context(column_instance)));
                        break;
                    case ColumnTypeTag::INT:
                        copy_fixed_value(column_block, row, value);
                        break;
                    case ColumnTypeTag::INT_UNSIGNED:
                        copy_fixed_value(column_block, row, TagUtils::checked_cast<uint32_t>(value, column_context(column_instance)));
                        break;
                    default:
                        break;
                }
            }
            return;
        }
        case ColumnTypeTag::TIMESTAMP: {
            if (values.values_case() != proto::ColumnValues::kTimestampValues) {
                throw std::runtime_error("ingress " + column_context(column_instance) + " type does not match fixed schema.");
            }
            const auto& typed_values = values.timestamp_values();
            validate_proto_timestamps(typed_values, column_context(column_instance));
            if (!has_nulls && typed_values.values_size() > 0) {
                copy_proto_fixed_values(column_block, typed_values.values(), row_count);
                return;
            }
            for (size_t row = 0; row < row_count; ++row) {
                if (is_null_at(values, row)) {
                    set_fixed_null(column_block, row);
                } else {
                    column_block.is_nulls[row] = 0;
                    int64_t timestamp = 0;
                    if (typed_values.values_size() > 0) {
                        timestamp = typed_values.values(static_cast<int>(row));
                    } else {
                        ColumnType parsed = TypeConverter::convert_to_type(
                            typed_values.string_values(static_cast<int>(row)), column_config);
                        timestamp = std::get<int64_t>(parsed);
                    }
                    copy_fixed_value(column_block, row, timestamp);
                }
            }
            return;
        }
        case ColumnTypeTag::BIGINT:
        case ColumnTypeTag::BIGINT_UNSIGNED: {
            if (values.values_case() != proto::ColumnValues::kBigintValues) {
                throw std::runtime_error("ingress " + column_context(column_instance) + " type does not match fixed schema.");
            }
            const auto& typed_values = values.bigint_values().values();
            if (!has_nulls && column_config.type_tag == ColumnTypeTag::BIGINT) {
                copy_proto_fixed_values(column_block, typed_values, row_count);
                return;
            }
            for (size_t row = 0; row < row_count; ++row) {
                if (is_null_at(values, row)) {
                    set_fixed_null(column_block, row);
                } else {
                    column_block.is_nulls[row] = 0;
                    if (column_config.type_tag == ColumnTypeTag::BIGINT_UNSIGNED) {
                        copy_fixed_value(column_block, row,
                                         TagUtils::checked_cast<uint64_t>(typed_values.Get(static_cast<int>(row)),
                                                                column_context(column_instance)));
                    } else {
                        copy_fixed_value(column_block, row, typed_values.Get(static_cast<int>(row)));
                    }
                }
            }
            return;
        }
        case ColumnTypeTag::FLOAT: {
            if (values.values_case() != proto::ColumnValues::kFloatValues) {
                throw std::runtime_error("ingress " + column_context(column_instance) + " type does not match fixed schema.");
            }
            const auto& typed_values = values.float_values().values();
            if (!has_nulls) {
                copy_proto_fixed_values(column_block, typed_values, row_count);
                return;
            }
            for (size_t row = 0; row < row_count; ++row) {
                if (is_null_at(values, row)) {
                    set_fixed_null(column_block, row);
                } else {
                    column_block.is_nulls[row] = 0;
                    copy_fixed_value(column_block, row, typed_values.Get(static_cast<int>(row)));
                }
            }
            return;
        }
        case ColumnTypeTag::DOUBLE: {
            if (values.values_case() != proto::ColumnValues::kDoubleValues) {
                throw std::runtime_error("ingress " + column_context(column_instance) + " type does not match fixed schema.");
            }
            const auto& typed_values = values.double_values().values();
            if (!has_nulls) {
                copy_proto_fixed_values(column_block, typed_values, row_count);
                return;
            }
            for (size_t row = 0; row < row_count; ++row) {
                if (is_null_at(values, row)) {
                    set_fixed_null(column_block, row);
                } else {
                    column_block.is_nulls[row] = 0;
                    copy_fixed_value(column_block, row, typed_values.Get(static_cast<int>(row)));
                }
            }
            return;
        }
        default:
            throw std::runtime_error("ingress " + column_context(column_instance) + " uses unsupported fixed-length type.");
    }
}

void IngestionMemoryBlockBuilder::fill_proto_interlace_batch(MemoryPool::MemoryBlock& block,
                                                             const proto::InterlaceBatch& batch,
                                                             MemoryPool& pool) const {
    (void)pool;
    const size_t table_count = proto_interlace_table_count(batch.table_names());
    const size_t rows_per_table = static_cast<size_t>(batch.rows_per_table());
    const size_t total_rows = proto_interlace_total_rows(batch);

    if (table_count > config_.max_tables_per_request) {
        throw IngestionResourceExhaustedError("ingress request table count exceeds max_tables_per_request.");
    }
    if (total_rows > config_.rows_per_batch) {
        throw IngestionResourceExhaustedError("ingress request row count exceeds rows_per_batch.");
    }
    if (table_count > block.tables.size()) {
        throw std::runtime_error("ingress request table count exceeds memory block capacity.");
    }
    if (rows_per_table > block.tables[0].max_rows) {
        throw IngestionResourceExhaustedError("ingress interlace_batch rows_per_table exceeds per-table capacity.");
    }

    const size_t expected_column_count = schema_.columns.size() - 1;
    if (static_cast<size_t>(batch.columns_size()) != expected_column_count) {
        std::ostringstream ss;
        ss << "ingress interlace_batch column count mismatch: expected "
           << expected_column_count << ", got " << batch.columns_size() << ".";
        throw std::runtime_error(ss.str());
    }

    block.used_tables = table_count;
    block.total_rows = total_rows;

    block.table_name_storage_.reserve(table_count);

    for (size_t table_index = 0; table_index < table_count; ++table_index) {
        auto& table_block = block.tables[table_index];
        set_proto_interlace_table_name(block, table_index, batch.table_names());
        table_block.tags_ptr = nullptr;
        table_block.used_rows = rows_per_table;
    }

    fill_proto_interlace_primary_timestamps(block, batch, table_count, rows_per_table);

    for (int col_idx = 0; col_idx < batch.columns_size(); ++col_idx) {
        fill_proto_interlace_column(batch.columns(col_idx),
                                    schema_.columns[static_cast<size_t>(col_idx) + 1],
                                    col_instances_[static_cast<size_t>(col_idx)],
                                    static_cast<size_t>(col_idx),
                                    block,
                                    table_count,
                                    rows_per_table);
    }
}

void IngestionMemoryBlockBuilder::fill_proto_interlace_primary_timestamps(MemoryPool::MemoryBlock& block,
                                                                          const proto::InterlaceBatch& batch,
                                                                          size_t table_count,
                                                                          size_t rows_per_table) const {
    const auto& timestamps = batch.timestamps();
    const size_t total_rows = table_count * rows_per_table;
    validate_proto_timestamps(timestamps, "interlace_batch timestamps");
    if (proto_timestamp_row_count(timestamps) != total_rows) {
        throw std::runtime_error("ingress interlace_batch timestamp row count mismatch.");
    }

    const auto& ts_config = schema_.columns[0];
    if (timestamps.values_size() > 0) {
        const int64_t* src = timestamps.values().data();
        for (size_t table_index = 0; table_index < table_count; ++table_index) {
            const size_t offset = table_index * rows_per_table;
            auto& table_block = block.tables[table_index];
            std::memcpy(table_block.timestamps, src + offset, rows_per_table * sizeof(int64_t));
        }
        for (size_t row = 0; row < total_rows; ++row) {
            block.start_time = std::min(block.start_time, src[row]);
            block.end_time = std::max(block.end_time, src[row]);
        }
        return;
    }

    for (size_t table_index = 0; table_index < table_count; ++table_index) {
        auto& table_block = block.tables[table_index];
        for (size_t row = 0; row < rows_per_table; ++row) {
            const size_t offset = table_index * rows_per_table + row;
            ColumnType parsed = TypeConverter::convert_to_type(
                timestamps.string_values(static_cast<int>(offset)), ts_config);
            const int64_t timestamp = std::get<int64_t>(parsed);
            table_block.timestamps[row] = timestamp;
            block.start_time = std::min(block.start_time, timestamp);
            block.end_time = std::max(block.end_time, timestamp);
        }
    }
}

void IngestionMemoryBlockBuilder::fill_proto_interlace_column(const proto::ColumnValues& values,
                                                              const ColumnConfig& column_config,
                                                              const ColumnConfigInstance& column_instance,
                                                              size_t column_index,
                                                              MemoryPool::MemoryBlock& block,
                                                              size_t table_count,
                                                              size_t rows_per_table) const {
    const size_t total_rows = table_count * rows_per_table;
    if (values.nulls_size() > 0 && static_cast<size_t>(values.nulls_size()) != total_rows) {
        throw std::runtime_error("ingress " + column_context(column_instance) + " null bitmap length mismatch.");
    }
    if (proto_column_row_count(values) != total_rows) {
        throw std::runtime_error("ingress " + column_context(column_instance) + " row count mismatch.");
    }

    const bool has_nulls = values.nulls_size() > 0;
    if (column_config.is_var_length()) {
        switch (column_config.type_tag) {
            case ColumnTypeTag::NCHAR:
            case ColumnTypeTag::VARCHAR:
            case ColumnTypeTag::BINARY:
            case ColumnTypeTag::JSON: {
                if (values.values_case() != proto::ColumnValues::kStringValues) {
                    throw std::runtime_error("ingress " + column_context(column_instance) + " type does not match fixed schema.");
                }
                const auto& typed_values = values.string_values().values();
                for (size_t table_index = 0; table_index < table_count; ++table_index) {
                    auto& column_block = block.tables[table_index].columns[column_index];
                    for (size_t row = 0; row < rows_per_table; ++row) {
                        const size_t offset = table_index * rows_per_table + row;
                        if (is_null_at(values, offset)) {
                            set_var_null(column_block, row);
                        } else {
                            const std::string& value = typed_values.Get(static_cast<int>(offset));
                            copy_var_value(column_block, row, value.data(), value.size());
                        }
                    }
                }
                return;
            }
            case ColumnTypeTag::VARBINARY: {
                if (values.values_case() != proto::ColumnValues::kBinaryValues) {
                    throw std::runtime_error("ingress " + column_context(column_instance) + " type does not match fixed schema.");
                }
                const auto& typed_values = values.binary_values().values();
                for (size_t table_index = 0; table_index < table_count; ++table_index) {
                    auto& column_block = block.tables[table_index].columns[column_index];
                    for (size_t row = 0; row < rows_per_table; ++row) {
                        const size_t offset = table_index * rows_per_table + row;
                        if (is_null_at(values, offset)) {
                            set_var_null(column_block, row);
                        } else {
                            const std::string& bytes = typed_values.Get(static_cast<int>(offset));
                            copy_var_value(column_block, row, bytes.data(), bytes.size());
                        }
                    }
                }
                return;
            }
            default:
                throw std::runtime_error("ingress " + column_context(column_instance) + " uses unsupported variable-length type.");
        }
    }

    switch (column_config.type_tag) {
        case ColumnTypeTag::BOOL: {
            if (values.values_case() != proto::ColumnValues::kBoolValues) {
                throw std::runtime_error("ingress " + column_context(column_instance) + " type does not match fixed schema.");
            }
            const auto& typed_values = values.bool_values().values();
            for (size_t table_index = 0; table_index < table_count; ++table_index) {
                auto& column_block = block.tables[table_index].columns[column_index];
                for (size_t row = 0; row < rows_per_table; ++row) {
                    const size_t offset = table_index * rows_per_table + row;
                    if (is_null_at(values, offset)) {
                        set_fixed_null(column_block, row);
                    } else {
                        column_block.is_nulls[row] = 0;
                        copy_fixed_value(column_block, row, static_cast<bool>(typed_values.Get(static_cast<int>(offset))));
                    }
                }
            }
            return;
        }
        case ColumnTypeTag::TINYINT:
        case ColumnTypeTag::TINYINT_UNSIGNED:
        case ColumnTypeTag::SMALLINT:
        case ColumnTypeTag::SMALLINT_UNSIGNED:
        case ColumnTypeTag::INT:
        case ColumnTypeTag::INT_UNSIGNED: {
            if (values.values_case() != proto::ColumnValues::kIntValues) {
                throw std::runtime_error("ingress " + column_context(column_instance) + " type does not match fixed schema.");
            }
            const auto& typed_values = values.int_values().values();
            if (!has_nulls && column_config.type_tag == ColumnTypeTag::INT) {
                for (size_t table_index = 0; table_index < table_count; ++table_index) {
                    copy_proto_fixed_values_segment(block.tables[table_index].columns[column_index],
                                                    typed_values,
                                                    table_index * rows_per_table,
                                                    rows_per_table);
                }
                return;
            }
            for (size_t table_index = 0; table_index < table_count; ++table_index) {
                auto& column_block = block.tables[table_index].columns[column_index];
                for (size_t row = 0; row < rows_per_table; ++row) {
                    const size_t offset = table_index * rows_per_table + row;
                    if (is_null_at(values, offset)) {
                        set_fixed_null(column_block, row);
                        continue;
                    }

                    column_block.is_nulls[row] = 0;
                    int32_t value = typed_values.Get(static_cast<int>(offset));
                    switch (column_config.type_tag) {
                        case ColumnTypeTag::TINYINT:
                            copy_fixed_value(column_block, row, TagUtils::checked_cast<int8_t>(value, column_context(column_instance)));
                            break;
                        case ColumnTypeTag::TINYINT_UNSIGNED:
                            copy_fixed_value(column_block, row, TagUtils::checked_cast<uint8_t>(value, column_context(column_instance)));
                            break;
                        case ColumnTypeTag::SMALLINT:
                            copy_fixed_value(column_block, row, TagUtils::checked_cast<int16_t>(value, column_context(column_instance)));
                            break;
                        case ColumnTypeTag::SMALLINT_UNSIGNED:
                            copy_fixed_value(column_block, row, TagUtils::checked_cast<uint16_t>(value, column_context(column_instance)));
                            break;
                        case ColumnTypeTag::INT:
                            copy_fixed_value(column_block, row, value);
                            break;
                        case ColumnTypeTag::INT_UNSIGNED:
                            copy_fixed_value(column_block, row, TagUtils::checked_cast<uint32_t>(value, column_context(column_instance)));
                            break;
                        default:
                            break;
                    }
                }
            }
            return;
        }
        case ColumnTypeTag::TIMESTAMP: {
            if (values.values_case() != proto::ColumnValues::kTimestampValues) {
                throw std::runtime_error("ingress " + column_context(column_instance) + " type does not match fixed schema.");
            }
            const auto& typed_values = values.timestamp_values();
            validate_proto_timestamps(typed_values, column_context(column_instance));
            if (!has_nulls && typed_values.values_size() > 0) {
                for (size_t table_index = 0; table_index < table_count; ++table_index) {
                    copy_proto_fixed_values_segment(block.tables[table_index].columns[column_index],
                                                    typed_values.values(),
                                                    table_index * rows_per_table,
                                                    rows_per_table);
                }
                return;
            }
            for (size_t table_index = 0; table_index < table_count; ++table_index) {
                auto& column_block = block.tables[table_index].columns[column_index];
                for (size_t row = 0; row < rows_per_table; ++row) {
                    const size_t offset = table_index * rows_per_table + row;
                    if (is_null_at(values, offset)) {
                        set_fixed_null(column_block, row);
                    } else {
                        column_block.is_nulls[row] = 0;
                        int64_t timestamp = 0;
                        if (typed_values.values_size() > 0) {
                            timestamp = typed_values.values(static_cast<int>(offset));
                        } else {
                            ColumnType parsed = TypeConverter::convert_to_type(
                                typed_values.string_values(static_cast<int>(offset)), column_config);
                            timestamp = std::get<int64_t>(parsed);
                        }
                        copy_fixed_value(column_block, row, timestamp);
                    }
                }
            }
            return;
        }
        case ColumnTypeTag::BIGINT:
        case ColumnTypeTag::BIGINT_UNSIGNED: {
            if (values.values_case() != proto::ColumnValues::kBigintValues) {
                throw std::runtime_error("ingress " + column_context(column_instance) + " type does not match fixed schema.");
            }
            const auto& typed_values = values.bigint_values().values();
            if (!has_nulls && column_config.type_tag == ColumnTypeTag::BIGINT) {
                for (size_t table_index = 0; table_index < table_count; ++table_index) {
                    copy_proto_fixed_values_segment(block.tables[table_index].columns[column_index],
                                                    typed_values,
                                                    table_index * rows_per_table,
                                                    rows_per_table);
                }
                return;
            }
            for (size_t table_index = 0; table_index < table_count; ++table_index) {
                auto& column_block = block.tables[table_index].columns[column_index];
                for (size_t row = 0; row < rows_per_table; ++row) {
                    const size_t offset = table_index * rows_per_table + row;
                    if (is_null_at(values, offset)) {
                        set_fixed_null(column_block, row);
                    } else {
                        column_block.is_nulls[row] = 0;
                        if (column_config.type_tag == ColumnTypeTag::BIGINT_UNSIGNED) {
                            copy_fixed_value(column_block, row,
                                             TagUtils::checked_cast<uint64_t>(typed_values.Get(static_cast<int>(offset)),
                                                                    column_context(column_instance)));
                        } else {
                            copy_fixed_value(column_block, row, typed_values.Get(static_cast<int>(offset)));
                        }
                    }
                }
            }
            return;
        }
        case ColumnTypeTag::FLOAT: {
            if (values.values_case() != proto::ColumnValues::kFloatValues) {
                throw std::runtime_error("ingress " + column_context(column_instance) + " type does not match fixed schema.");
            }
            const auto& typed_values = values.float_values().values();
            if (!has_nulls) {
                for (size_t table_index = 0; table_index < table_count; ++table_index) {
                    copy_proto_fixed_values_segment(block.tables[table_index].columns[column_index],
                                                    typed_values,
                                                    table_index * rows_per_table,
                                                    rows_per_table);
                }
                return;
            }
            for (size_t table_index = 0; table_index < table_count; ++table_index) {
                auto& column_block = block.tables[table_index].columns[column_index];
                for (size_t row = 0; row < rows_per_table; ++row) {
                    const size_t offset = table_index * rows_per_table + row;
                    if (is_null_at(values, offset)) {
                        set_fixed_null(column_block, row);
                    } else {
                        column_block.is_nulls[row] = 0;
                        copy_fixed_value(column_block, row, typed_values.Get(static_cast<int>(offset)));
                    }
                }
            }
            return;
        }
        case ColumnTypeTag::DOUBLE: {
            if (values.values_case() != proto::ColumnValues::kDoubleValues) {
                throw std::runtime_error("ingress " + column_context(column_instance) + " type does not match fixed schema.");
            }
            const auto& typed_values = values.double_values().values();
            if (!has_nulls) {
                for (size_t table_index = 0; table_index < table_count; ++table_index) {
                    copy_proto_fixed_values_segment(block.tables[table_index].columns[column_index],
                                                    typed_values,
                                                    table_index * rows_per_table,
                                                    rows_per_table);
                }
                return;
            }
            for (size_t table_index = 0; table_index < table_count; ++table_index) {
                auto& column_block = block.tables[table_index].columns[column_index];
                for (size_t row = 0; row < rows_per_table; ++row) {
                    const size_t offset = table_index * rows_per_table + row;
                    if (is_null_at(values, offset)) {
                        set_fixed_null(column_block, row);
                    } else {
                        column_block.is_nulls[row] = 0;
                        copy_fixed_value(column_block, row, typed_values.Get(static_cast<int>(offset)));
                    }
                }
            }
            return;
        }
        default:
            throw std::runtime_error("ingress " + column_context(column_instance) + " uses unsupported fixed-length type.");
    }
}

void IngestionMemoryBlockBuilder::set_proto_interlace_table_name(MemoryPool::MemoryBlock& block,
                                                                 size_t table_index,
                                                                 const proto::TableNames& table_names) const {
    auto& table_block = block.tables[table_index];
    switch (table_names.source_case()) {
        case proto::TableNames::kExplicitNames: {
            const std::string& table_name = table_names.explicit_names().values(static_cast<int>(table_index));
            if (table_name.empty()) {
                throw std::runtime_error("ingress table_name must not be empty.");
            }
            block.table_name_storage_.push_back(table_name);
            table_block.table_name = block.table_name_storage_.back().c_str();
            return;
        }
        case proto::TableNames::kRange: {
            const auto& range = table_names.range();
            if (range.prefix().empty()) {
                throw std::runtime_error("ingress table_name range prefix must not be empty.");
            }
            const uint64_t step = proto_table_range_step(range);
            if (table_index > (std::numeric_limits<uint64_t>::max() - range.start()) / step) {
                throw IngestionResourceExhaustedError("ingress table_name range exceeds supported size.");
            }
            const uint64_t index = range.start() + static_cast<uint64_t>(table_index) * step;

            const auto& generator = schema_.tbname.generator;
            const bool can_use_schema_cache = [&] {
                if (schema_.tbname.source_type != "generator" || step != 1 || range.prefix() != generator.prefix) {
                    return false;
                }
                const auto bounds = validated_generator_bounds(generator);
                return index >= bounds.first && index < bounds.last;
            }();
            if (can_use_schema_cache) {
                block.table_name_storage_.push_back(generated_table_name(index));
                table_block.table_name = block.table_name_storage_.back().c_str();
                return;
            }

            block.table_name_storage_.push_back(range.prefix() + std::to_string(index));
            table_block.table_name = block.table_name_storage_.back().c_str();
            return;
        }
        case proto::TableNames::SOURCE_NOT_SET:
            throw std::runtime_error("ingress interlace_batch table_names must be set.");
    }

    throw std::runtime_error("unsupported ingress interlace_batch table_names.");
}

void IngestionMemoryBlockBuilder::commit_pending_tags(
    MemoryPool::MemoryBlock& block,
    MemoryPool& pool,
    const std::vector<PendingTagRegistration>& pending_tags) const {
    if (pending_tags.empty()) {
        return;
    }

    std::vector<std::pair<std::string, std::vector<ColumnType>>> table_tags;
    table_tags.reserve(pending_tags.size());
    for (const auto& pending : pending_tags) {
        table_tags.emplace_back(pending.table_name, pending.values);
    }

    const auto cached_tags = pool.get_or_register_table_tags_batch(table_tags);
    if (cached_tags.size() != pending_tags.size()) {
        throw std::runtime_error("ingress failed to register table tags.");
    }

    for (size_t i = 0; i < pending_tags.size(); ++i) {
        if (pending_tags[i].table_index >= block.tables.size() || cached_tags[i] == nullptr) {
            throw std::runtime_error("ingress failed to register table tags.");
        }
        block.tables[pending_tags[i].table_index].tags_ptr = cached_tags[i];
    }
}

const char* IngestionMemoryBlockBuilder::generated_table_name(uint64_t table_index) const {
    const auto& generator = schema_.tbname.generator;
    const auto bounds = validated_generator_bounds(generator);
    if (table_index < bounds.first || table_index >= bounds.last) {
        throw std::runtime_error("ingress table_name range is outside schema.tbname generator range.");
    }

    // The schema generator range is expected to be bounded by configuration.
    // Cache the full range once so table-name lookup stays pointer-only in the hot path.
    std::call_once(table_name_cache_once_, [this, &generator] {
        const auto bounds = validated_generator_bounds(generator);
        generated_table_name_cache_.reserve(static_cast<size_t>(bounds.last - bounds.first));
        for (uint64_t i = bounds.first; i < bounds.last; ++i) {
            generated_table_name_cache_.push_back(generator.prefix + std::to_string(i));
        }
    });

    return generated_table_name_cache_[static_cast<size_t>(table_index - bounds.first)].c_str();
}

void IngestionMemoryBlockBuilder::fill_primary_timestamps(MemoryPool::MemoryBlock& block,
                                                          MemoryPool::TableBlock& table_block,
                                                          const IngestTimestampValues& timestamps) const {
    const size_t row_count = timestamps.row_count();
    const auto& ts_config = schema_.columns[0];

    for (size_t row = 0; row < row_count; ++row) {
        int64_t timestamp = timestamp_at(timestamps, row, ts_config);
        table_block.timestamps[row] = timestamp;
        block.start_time = std::min(block.start_time, timestamp);
        block.end_time = std::max(block.end_time, timestamp);
    }
}

void IngestionMemoryBlockBuilder::fill_column(MemoryPool::TableBase::Column& column_block,
                                              const IngestColumnValues& values,
                                              const ColumnConfig& column_config,
                                              const ColumnConfigInstance& column_instance,
                                              size_t row_count) const {
    if (column_config.is_var_length()) {
        switch (column_config.type_tag) {
            case ColumnTypeTag::NCHAR:
            case ColumnTypeTag::VARCHAR:
            case ColumnTypeTag::BINARY:
            case ColumnTypeTag::JSON: {
                const auto& typed_values = column_vector<std::string>(values, column_instance);
                for (size_t row = 0; row < row_count; ++row) {
                    if (is_null_at(values, row)) {
                        set_var_null(column_block, row);
                    } else {
                        copy_var_value(column_block, row, typed_values[row].data(), typed_values[row].size());
                    }
                }
                return;
            }
            case ColumnTypeTag::VARBINARY: {
                const auto& typed_values = binary_column(values, column_instance);
                for (size_t row = 0; row < row_count; ++row) {
                    if (is_null_at(values, row)) {
                        set_var_null(column_block, row);
                    } else {
                        const auto& bytes = typed_values[row];
                        copy_var_value(column_block, row,
                                       reinterpret_cast<const char*>(bytes.data()),
                                       bytes.size());
                    }
                }
                return;
            }
            default:
                throw std::runtime_error("ingress " + column_context(column_instance) + " uses unsupported variable-length type.");
        }
    }

    switch (column_config.type_tag) {
        case ColumnTypeTag::BOOL: {
            const auto& typed_values = column_vector<bool>(values, column_instance);
            for (size_t row = 0; row < row_count; ++row) {
                if (is_null_at(values, row)) {
                    set_fixed_null(column_block, row);
                } else {
                    column_block.is_nulls[row] = 0;
                    copy_fixed_value(column_block, row, static_cast<bool>(typed_values[row]));
                }
            }
            return;
        }
        case ColumnTypeTag::TINYINT:
        case ColumnTypeTag::TINYINT_UNSIGNED:
        case ColumnTypeTag::SMALLINT:
        case ColumnTypeTag::SMALLINT_UNSIGNED:
        case ColumnTypeTag::INT:
        case ColumnTypeTag::INT_UNSIGNED: {
            const auto& typed_values = column_vector<int32_t>(values, column_instance);
            for (size_t row = 0; row < row_count; ++row) {
                if (is_null_at(values, row)) {
                    set_fixed_null(column_block, row);
                    continue;
                }

                column_block.is_nulls[row] = 0;
                int32_t value = typed_values[row];
                switch (column_config.type_tag) {
                    case ColumnTypeTag::TINYINT:
                        copy_fixed_value(column_block, row, TagUtils::checked_cast<int8_t>(value, column_context(column_instance)));
                        break;
                    case ColumnTypeTag::TINYINT_UNSIGNED:
                        copy_fixed_value(column_block, row, TagUtils::checked_cast<uint8_t>(value, column_context(column_instance)));
                        break;
                    case ColumnTypeTag::SMALLINT:
                        copy_fixed_value(column_block, row, TagUtils::checked_cast<int16_t>(value, column_context(column_instance)));
                        break;
                    case ColumnTypeTag::SMALLINT_UNSIGNED:
                        copy_fixed_value(column_block, row, TagUtils::checked_cast<uint16_t>(value, column_context(column_instance)));
                        break;
                    case ColumnTypeTag::INT:
                        copy_fixed_value(column_block, row, value);
                        break;
                    case ColumnTypeTag::INT_UNSIGNED:
                        copy_fixed_value(column_block, row, TagUtils::checked_cast<uint32_t>(value, column_context(column_instance)));
                        break;
                    default:
                        break;
                }
            }
            return;
        }
        case ColumnTypeTag::TIMESTAMP: {
            const auto& typed_values = timestamp_column(values, column_instance);
            for (size_t row = 0; row < row_count; ++row) {
                if (is_null_at(values, row)) {
                    set_fixed_null(column_block, row);
                } else {
                    column_block.is_nulls[row] = 0;
                    copy_fixed_value(column_block, row, timestamp_at(typed_values, row, column_config));
                }
            }
            return;
        }
        case ColumnTypeTag::BIGINT:
        case ColumnTypeTag::BIGINT_UNSIGNED: {
            const auto& typed_values = column_vector<int64_t>(values, column_instance);
            for (size_t row = 0; row < row_count; ++row) {
                if (is_null_at(values, row)) {
                    set_fixed_null(column_block, row);
                } else {
                    column_block.is_nulls[row] = 0;
                    if (column_config.type_tag == ColumnTypeTag::BIGINT_UNSIGNED) {
                        copy_fixed_value(column_block, row, TagUtils::checked_cast<uint64_t>(typed_values[row], column_context(column_instance)));
                    } else {
                        copy_fixed_value(column_block, row, typed_values[row]);
                    }
                }
            }
            return;
        }
        case ColumnTypeTag::FLOAT: {
            const auto& typed_values = column_vector<float>(values, column_instance);
            for (size_t row = 0; row < row_count; ++row) {
                if (is_null_at(values, row)) {
                    set_fixed_null(column_block, row);
                } else {
                    column_block.is_nulls[row] = 0;
                    copy_fixed_value(column_block, row, typed_values[row]);
                }
            }
            return;
        }
        case ColumnTypeTag::DOUBLE: {
            const auto& typed_values = column_vector<double>(values, column_instance);
            for (size_t row = 0; row < row_count; ++row) {
                if (is_null_at(values, row)) {
                    set_fixed_null(column_block, row);
                } else {
                    column_block.is_nulls[row] = 0;
                    copy_fixed_value(column_block, row, typed_values[row]);
                }
            }
            return;
        }
        default:
            throw std::runtime_error("ingress " + column_context(column_instance) + " uses unsupported fixed-length type.");
    }
}
