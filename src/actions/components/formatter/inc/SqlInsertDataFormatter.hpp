#pragma once

#include "IFormatter.hpp"
#include "FormatterFactory.hpp"
#include "taos.h"
#include <limits>
#include <string>
#include <string_view>
#include <iterator>
#include <fmt/format.h>

class SqlInsertDataFormatter final : public IInsertDataFormatter {
public:
    explicit SqlInsertDataFormatter(const DataFormat& format) : format_(format) {}

    std::string prepare(const InsertDataConfig& config,
                        const ColumnConfigInstanceVector& col_instances,
                        const ColumnConfigInstanceVector& tag_instances) override {
        (void)config;
        (void)col_instances;
        (void)tag_instances;
        return "";
    }

    FormatResult format(const InsertDataConfig& config,
                        const ColumnConfigInstanceVector& col_instances,
                        const ColumnConfigInstanceVector& tag_instances,
                        MemoryPool::MemoryBlock* batch,
                        bool is_checkpoint_recover = false) const override {

        (void)is_checkpoint_recover;
        if (!batch || batch->total_rows == 0) {
            return FormatResult("");
        }

        std::string sql_buffer;
        sql_buffer.reserve(4 * 1024 * 1024);

        auto out = std::back_inserter(sql_buffer);
        fmt::format_to(out, "INSERT INTO");

        auto serialize_field = [&out](const ColumnConfigInstance& col_instance,
                                      const MemoryPool::TableBase::Column& col_block,
                                      size_t row_idx) {
            ColumnTypeTag tag = col_instance.config().type_tag;

            // Handle NULL
            if (col_block.is_nulls[row_idx]) {
                fmt::format_to(out, "NULL");
                return;
            }

            // Check if quotes are needed
            bool needs_quotes = ColumnTypeTraits::needs_quotes(tag);

            // Handle unsupported types
            if (tag == ColumnTypeTag::VARBINARY ||
                tag == ColumnTypeTag::GEOMETRY) {
                throw std::runtime_error("Unsupported column type for SQL insert: " + col_instance.type());
            }

            if (col_block.is_fixed) {
                // Fixed-length
                void* data_ptr = static_cast<char*>(col_block.fixed_data)
                               + row_idx * col_block.element_size;

                switch (tag) {
                    case ColumnTypeTag::BOOL:
                        fmt::format_to(out, "{}", *static_cast<bool*>(data_ptr) ? "true" : "false");
                        break;
                    case ColumnTypeTag::TINYINT:
                        fmt::format_to(out, "{}", *static_cast<int8_t*>(data_ptr));
                        break;
                    case ColumnTypeTag::TINYINT_UNSIGNED:
                        fmt::format_to(out, "{}", *static_cast<uint8_t*>(data_ptr));
                        break;
                    case ColumnTypeTag::SMALLINT:
                        fmt::format_to(out, "{}", *static_cast<int16_t*>(data_ptr));
                        break;
                    case ColumnTypeTag::SMALLINT_UNSIGNED:
                        fmt::format_to(out, "{}", *static_cast<uint16_t*>(data_ptr));
                        break;
                    case ColumnTypeTag::INT:
                        fmt::format_to(out, "{}", *static_cast<int32_t*>(data_ptr));
                        break;
                    case ColumnTypeTag::INT_UNSIGNED:
                        fmt::format_to(out, "{}", *static_cast<uint32_t*>(data_ptr));
                        break;
                    case ColumnTypeTag::BIGINT:
                        fmt::format_to(out, "{}", *static_cast<int64_t*>(data_ptr));
                        break;
                    case ColumnTypeTag::BIGINT_UNSIGNED:
                        fmt::format_to(out, "{}", *static_cast<uint64_t*>(data_ptr));
                        break;
                    case ColumnTypeTag::FLOAT:
                        fmt::format_to(out, "{}", *static_cast<float*>(data_ptr));
                        break;
                    case ColumnTypeTag::DOUBLE:
                        fmt::format_to(out, "{}", *static_cast<double*>(data_ptr));
                        break;
                    default:
                        throw std::runtime_error("Unsupported fixed-length type: " + col_instance.type());
                }
            } else {
                // Variable-length
                char* data_start = col_block.var_data + col_block.var_offsets[row_idx];
                int32_t data_len = col_block.lengths[row_idx];

                if (needs_quotes) {
                    fmt::format_to(out, "'");

                    if (tag == ColumnTypeTag::NCHAR) {
                        std::string_view utf8_sv(data_start, data_len);
                        for (char c : utf8_sv) {
                            if (c == '\'') fmt::format_to(out, "''");
                            else fmt::format_to(out, "{}", c);
                        }
                    } else {
                        for (int32_t i = 0; i < data_len; ++i) {
                            char c = data_start[i];
                            if (c == '\'') fmt::format_to(out, "''");
                            else fmt::format_to(out, "{}", c);
                        }
                    }

                    fmt::format_to(out, "'");
                } else {
                    // Numeric string (e.g., INT as VARCHAR): output as-is
                    fmt::format_to(out, "{}", std::string_view(data_start, data_len));
                }
            }
        };

        // Iterate all tables in the memory block
        for (size_t tbl_idx = 0; tbl_idx < batch->used_tables; ++tbl_idx) {
            auto& table_block = batch->tables[tbl_idx];
            if (table_block.used_rows == 0) continue;

            // Table name
            fmt::format_to(out, " `{}`.`{}`",
                           config.tdengine.database,
                           table_block.table_name);

            if (format_.sql.auto_create_table && table_block.tags_ptr) {
                fmt::format_to(out, " USING `{}`.`{}` TAGS (",
                               config.tdengine.database,
                               config.schema.name);

                for (size_t tag_idx = 0; tag_idx < tag_instances.size(); ++tag_idx) {
                    if (tag_idx > 0) fmt::format_to(out, ",");
                    serialize_field(tag_instances[tag_idx],
                                    table_block.tags_ptr->columns[tag_idx],
                                    0);
                }
                fmt::format_to(out, ")");
            }

            fmt::format_to(out, " VALUES ");

            // Rows
            for (size_t row_idx = 0; row_idx < table_block.used_rows; ++row_idx) {
                fmt::format_to(out, "({}", table_block.timestamps[row_idx]);

                // Columns
                for (size_t col_idx = 0; col_idx < col_instances.size(); ++col_idx) {
                    fmt::format_to(out, ",");
                    serialize_field(col_instances[col_idx],
                                    table_block.columns[col_idx],
                                    row_idx);
                }
                fmt::format_to(out, ")");
            }
        }

        fmt::format_to(out, ";");

        return SqlInsertData(batch, col_instances, tag_instances, std::move(sql_buffer));
    }

private:
    const DataFormat& format_;

    inline static bool registered_ = []() {
        FormatterFactory::instance().register_formatter<InsertDataConfig>(
            "sql",
            [](const DataFormat& format) {
                return std::make_unique<SqlInsertDataFormatter>(format);
            });
        return true;
    }();
};