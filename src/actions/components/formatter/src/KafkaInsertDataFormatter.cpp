#include "KafkaInsertDataFormatter.hpp"
#include "KafkaInsertData.hpp"
#include <fmt/core.h>
#include <fmt/format.h>
#include <stdexcept>

std::string KafkaInsertDataFormatter::prepare(const InsertDataConfig& config,
                                              const ColumnConfigInstanceVector& col_instances,
                                              const ColumnConfigInstanceVector& tag_instances) {
    (void)config;
    (void)col_instances;
    (void)tag_instances;
    return "";
}

FormatResult KafkaInsertDataFormatter::format(const InsertDataConfig& config,
                                              const ColumnConfigInstanceVector& col_instances,
                                              const ColumnConfigInstanceVector& tag_instances,
                                              MemoryPool::MemoryBlock* batch,
                                              bool is_checkpoint_recover) const {
    (void)config;
    (void)is_checkpoint_recover;
    if (!batch || batch->total_rows == 0) {
        return FormatResult("");
    }

    const auto& format = format_.kafka;

    // Dispatch based on the value serializer type
    if (format.value_serializer == "json") {
        return format_json(col_instances, tag_instances, batch);
    } else if (format.value_serializer == "influx") {
        return format_influx(col_instances, tag_instances, batch);
    } else {
        throw std::runtime_error("Unsupported Kafka value_serializer: " + format.value_serializer);
    }
}

KafkaInsertData KafkaInsertDataFormatter::format_json(const ColumnConfigInstanceVector& col_instances,
                                                      const ColumnConfigInstanceVector& tag_instances,
                                                      MemoryPool::MemoryBlock* batch) const {
    const auto& format = format_.kafka;
    KafkaMessageBatch msg_batch;
    msg_batch.reserve((batch->total_rows + format.records_per_message - 1) / format.records_per_message);

    KeyGenerator key_generator(format.key_pattern, format.key_serializer, col_instances, tag_instances);

    if (format.records_per_message == 1) {
        nlohmann::ordered_json json_data;

        for (size_t table_idx = 0; table_idx < batch->used_tables; ++table_idx) {
            auto& table_block = batch->tables[table_idx];
            for (size_t row_idx = 0; row_idx < table_block.used_rows; ++row_idx) {
                std::string key = key_generator.generate(table_block, row_idx);
                RowSerializer::to_json_inplace(col_instances, tag_instances, table_block, row_idx, format.tbname_key, json_data);
                msg_batch.emplace_back(std::move(key), json_data.dump());
                json_data.clear();
            }
        }
    } else {
        nlohmann::ordered_json json_array = nlohmann::ordered_json::array();
        std::string first_record_key;
        nlohmann::ordered_json record_json;

        for (size_t table_idx = 0; table_idx < batch->used_tables; ++table_idx) {
            auto& table_block = batch->tables[table_idx];
            for (size_t row_idx = 0; row_idx < table_block.used_rows; ++row_idx) {
                // If this is the first record of a new batch, generate the key
                if (json_array.empty()) {
                    first_record_key = key_generator.generate(table_block, row_idx);
                }

                // Add the current record's JSON to the array
                RowSerializer::to_json_inplace(col_instances, tag_instances, table_block, row_idx, format.tbname_key, record_json);
                json_array.emplace_back(std::move(record_json));
                record_json.clear();

                // If the batch is full, create the message and reset
                if (json_array.size() >= format.records_per_message) {
                    msg_batch.emplace_back(std::move(first_record_key), json_array.dump());
                    json_array.clear();
                }
            }
        }

        // Add any remaining records from the last partial batch
        if (!json_array.empty()) {
            msg_batch.emplace_back(std::move(first_record_key), json_array.dump());
        }
    }

    return KafkaInsertData(batch, col_instances, tag_instances, std::move(msg_batch));
}

// Formats data into InfluxDB Line Protocol.
KafkaInsertData KafkaInsertDataFormatter::format_influx(const ColumnConfigInstanceVector& col_instances,
                                                        const ColumnConfigInstanceVector& tag_instances,
                                                        MemoryPool::MemoryBlock* batch) const {
    const auto& format = format_.kafka;
    KafkaMessageBatch msg_batch;
    msg_batch.reserve((batch->total_rows + format.records_per_message - 1) / format.records_per_message);

    KeyGenerator key_generator(format.key_pattern, format.key_serializer, col_instances, tag_instances);

    std::vector<const char*> field_formats;

    for (const auto& col : col_instances) {
        const char* fmt_str;

        if (ColumnTypeTraits::needs_quotes(col.type_tag())) {
            fmt_str = "{}=\"{}\"";  // name="value"
        } else if (ColumnTypeTraits::is_integer(col.type_tag())) {
            fmt_str = "{}={}i";     // name=valuei
        } else {
            fmt_str = "{}={}";      // name=value
        }
        field_formats.emplace_back(fmt_str);
    }

    fmt::memory_buffer line_buffer;
    line_buffer.reserve(1048576);
    std::string first_record_key;
    size_t records_in_current_message = 0;

    for (size_t table_idx = 0; table_idx < batch->used_tables; ++table_idx) {
        auto& table_block = batch->tables[table_idx];
        const char* measurement = table_block.table_name ? table_block.table_name : "unknown";

        for (size_t row_idx = 0; row_idx < table_block.used_rows; ++row_idx) {
            if (records_in_current_message == 0) {
                first_record_key = key_generator.generate(table_block, row_idx);
            } else {
                fmt::format_to(std::back_inserter(line_buffer), "\n");
            }

            // 1. Measurement
            fmt::format_to(std::back_inserter(line_buffer), "{}", measurement);

            // 2. Tags
            for (size_t tag_idx = 0; tag_idx < tag_instances.size(); ++tag_idx) {
                fmt::format_to(std::back_inserter(line_buffer), ",{}=\"{}\"",
                               tag_instances[tag_idx].name(),
                               table_block.get_tag_cell(row_idx, tag_idx));
            }

            // 3. Space separator
            line_buffer.push_back(' ');

            // 4. Fields
            bool first_field = true;
            for (size_t col_idx = 0; col_idx < col_instances.size(); ++col_idx) {
                if (!first_field) {
                    line_buffer.push_back(',');
                }

                // The format string can handles quotes and integer suffixes
                fmt::format_to(std::back_inserter(line_buffer), fmt::runtime(field_formats[col_idx]),
                               col_instances[col_idx].name(),
                               table_block.get_column_cell(row_idx, col_idx));
                first_field = false;
            }

            // 5. Timestamp
            fmt::format_to(std::back_inserter(line_buffer), " {}", table_block.timestamps[row_idx]);

            records_in_current_message++;

            // If the message is full, push it to the batch and reset
            if (records_in_current_message >= format.records_per_message) {
                msg_batch.emplace_back(std::move(first_record_key), fmt::to_string(line_buffer));
                line_buffer.clear();
                records_in_current_message = 0;
            }
        }
    }

    // Add any remaining records from the last partial message
    if (records_in_current_message > 0) {
        msg_batch.emplace_back(std::move(first_record_key), fmt::to_string(line_buffer));
    }

    return KafkaInsertData(batch, col_instances, tag_instances, std::move(msg_batch));
}