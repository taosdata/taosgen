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
                        const ColumnConfigInstanceVector& col_instances) override {
        (void)config;
        (void)col_instances;
        return "";
    }

    FormatResult format(const InsertDataConfig& config, 
                        const ColumnConfigInstanceVector& col_instances, 
                        MemoryPool::MemoryBlock* batch) const override{
        if (!batch || batch->total_rows == 0) {
            return FormatResult("");
        }

        std::ostringstream result;
        const auto& db_info = config.target.tdengine.database_info;
        result << "INSERT INTO";


        // Iterate all tables in the memory block
        for (size_t tbl_idx = 0; tbl_idx < batch->used_tables; ++tbl_idx) {
            auto& table_block = batch->tables[tbl_idx];
            if (table_block.used_rows == 0) continue;

            // Write table name
            result << " `" << db_info.name 
                << "`.`" << table_block.table_name << "` VALUES ";

            // Iterate all rows in the table
            for (size_t row_idx = 0; row_idx < table_block.used_rows; ++row_idx) {
                result << "(" << table_block.timestamps[row_idx];

                // Iterate all columns
                for (size_t col_idx = 0; col_idx < col_instances.size(); ++col_idx) {
                    auto& col_instance = col_instances[col_idx];
                    auto& col_block = table_block.columns[col_idx];
                    ColumnTypeTag tag = col_instance.config().type_tag;
                    
                    result << ",";
                    
                    // Handle NULL value
                    if (col_block.is_nulls[row_idx]) {
                        result << "NULL";
                        continue;
                    }

                    // Check if quotes are needed
                    bool needs_quotes = (tag == ColumnTypeTag::NCHAR ||
                                        tag == ColumnTypeTag::VARCHAR ||
                                        tag == ColumnTypeTag::BINARY ||
                                        tag == ColumnTypeTag::JSON);

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
                            // case ColumnTypeTag::DECIMAL: {
                            //     const Decimal& dec = *static_cast<Decimal*>(data_ptr);
                            //     result << dec.to_string();
                            //     break;
                            // }
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
                                // UTF-16 to UTF-8 conversion
                                const char16_t* u16str = reinterpret_cast<const char16_t*>(data_start);
                                size_t u16len = data_len / sizeof(char16_t);
                                std::u16string u16(u16str, u16len);

                                // Convert to UTF-8
                                std::string utf8;
                                {
                                    std::u16string u16(u16str, u16len);
                                    utf8.reserve(u16.size() * 2);
                                    for (char16_t c : u16) {
                                        if (c <= 0x7F) {
                                            utf8.push_back(static_cast<char>(c));
                                        } else if (c <= 0x7FF) {
                                            utf8.push_back(static_cast<char>(0xC0 | ((c >> 6) & 0x1F)));
                                            utf8.push_back(static_cast<char>(0x80 | (c & 0x3F)));
                                        } else {
                                            utf8.push_back(static_cast<char>(0xE0 | ((c >> 12) & 0x0F)));
                                            utf8.push_back(static_cast<char>(0x80 | ((c >> 6) & 0x3F)));
                                            utf8.push_back(static_cast<char>(0x80 | (c & 0x3F)));
                                        }
                                    }
                                }

                                // Escape single quotes
                                for (char c : utf8) {
                                    if (c == '\'') result << "''";
                                    else result << c;
                                }
                            } else {
                                // Escape single quotes
                                for (int i = 0; i < data_len; ++i) {
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
                }
                result << ")";
            }
        }
        
        result << ";";

        int64_t start_time = batch->start_time;
        int64_t end_time = batch->end_time;
        size_t total_rows = batch->total_rows;
        batch->release();

        return SqlInsertData(
            start_time,
            end_time,
            total_rows,
            result.str()
        );
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