#pragma once

#include "IFormatter.hpp"
#include "FormatterFactory.hpp"
#include "taos.h"
#include <sstream>
#include <limits>

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

        std::ostringstream result;
        result << "INSERT INTO";

        auto serialize_field = [&result](const ColumnConfigInstance& col_instance,
                                         const MemoryPool::TableBase::Column& col_block,
                                         size_t row_idx) {
            ColumnTypeTag tag = col_instance.config().type_tag;

            // Handle NULL value
            if (col_block.is_nulls[row_idx]) {
                result << "NULL";
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
                // Fixed-length column processing
                void* data_ptr = static_cast<char*>(col_block.fixed_data)
                            + row_idx * col_block.element_size;

                switch (tag) {
                    case ColumnTypeTag::BOOL: {
                        bool value = *static_cast<bool*>(data_ptr);
                        result << (value ? "true" : "false");
                        break;
                    }
                    case ColumnTypeTag::TINYINT:
                        result << static_cast<int>(*static_cast<int8_t*>(data_ptr));
                        break;
                    case ColumnTypeTag::TINYINT_UNSIGNED:
                        result << static_cast<unsigned>(*static_cast<uint8_t*>(data_ptr));
                        break;
                    case ColumnTypeTag::SMALLINT:
                        result << *static_cast<int16_t*>(data_ptr);
                        break;
                    case ColumnTypeTag::SMALLINT_UNSIGNED:
                        result << *static_cast<uint16_t*>(data_ptr);
                        break;
                    case ColumnTypeTag::INT:
                        result << *static_cast<int32_t*>(data_ptr);
                        break;
                    case ColumnTypeTag::INT_UNSIGNED:
                        result << *static_cast<uint32_t*>(data_ptr);
                        break;
                    case ColumnTypeTag::BIGINT:
                        result << *static_cast<int64_t*>(data_ptr);
                        break;
                    case ColumnTypeTag::BIGINT_UNSIGNED:
                        result << *static_cast<uint64_t*>(data_ptr);
                        break;
                    case ColumnTypeTag::FLOAT:
                        result << *static_cast<float*>(data_ptr);
                        break;
                    case ColumnTypeTag::DOUBLE:
                        result << *static_cast<double*>(data_ptr);
                        break;
                    default:
                        throw std::runtime_error("Unsupported fixed-length type: " + col_instance.type());
                }
            } else {
                // Variable-length column processing
                char* data_start = col_block.var_data + col_block.var_offsets[row_idx];
                int32_t data_len = col_block.lengths[row_idx];

                if (needs_quotes) {
                    result << "'";

                    if (tag == ColumnTypeTag::NCHAR) {
                        std::string utf8(data_start, data_len);

                        // Escape single quotes
                        for (char c : utf8) {
                            if (c == '\'') result << "''";
                            else result << c;
                        }
                    } else {
                        // Escape single quotes
                        for (int32_t i = 0; i < data_len; ++i) {
                            char c = data_start[i];
                            if (c == '\'') result << "''";
                            else result << c;
                        }
                    }

                    result << "'";
                } else {
                    // Directly output numeric value
                    result << std::string(data_start, data_len);
                }
            }
        };

        // Iterate all tables in the memory block
        for (size_t tbl_idx = 0; tbl_idx < batch->used_tables; ++tbl_idx) {
            auto& table_block = batch->tables[tbl_idx];
            if (table_block.used_rows == 0) continue;

            // Write table name
            result << " `" << config.tdengine.database
                << "`.`" << table_block.table_name << "`";

            if (format_.sql.auto_create_table && table_block.tags_ptr) {
                result << " USING `" << config.tdengine.database << "`.`"
                    << config.schema.name << "` TAGS (";

                for (size_t tag_idx = 0; tag_idx < tag_instances.size(); ++tag_idx) {
                    if (tag_idx > 0) result << ",";
                    serialize_field(tag_instances[tag_idx],
                                    table_block.tags_ptr->columns[tag_idx],
                                    0);
                }
                result << ")";
            }

            result << " VALUES ";

            // Iterate all rows in the table
            for (size_t row_idx = 0; row_idx < table_block.used_rows; ++row_idx) {
                result << "(" << table_block.timestamps[row_idx];

                // Iterate all columns
                for (size_t col_idx = 0; col_idx < col_instances.size(); ++col_idx) {
                    result << ",";
                    serialize_field(col_instances[col_idx],
                                    table_block.columns[col_idx],
                                    row_idx);
                }
                result << ")";
            }
        }

        result << ";";

        return SqlInsertData(batch, col_instances, tag_instances, result.str());
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