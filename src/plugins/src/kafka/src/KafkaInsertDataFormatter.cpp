#include "KafkaInsertData.hpp"
#include "KafkaInsertDataFormatter.hpp"
#include <fmt/core.h>
#include <fmt/format.h>
#include <stdexcept>

KafkaInsertDataFormatter::KafkaInsertDataFormatter(const DataFormat& format) : format_(format) {
    format_options_ = get_format_opt<KafkaFormatOptions>(format_, "kafka");
    if (!format_options_) {
        throw std::runtime_error("Kafka formatter options not found in DataFormat");
    }
}

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

    // Dispatch based on the value serializer type
    if (format_options_->value_serializer == "json") {
        return format_json(col_instances, tag_instances, batch);
    } else if (format_options_->value_serializer == "influx") {
        return format_influx(col_instances, tag_instances, batch);
    } else {
        throw std::runtime_error("Unsupported Kafka value_serializer: " + format_options_->value_serializer);
    }
}

 FormatResult KafkaInsertDataFormatter::format_json(const ColumnConfigInstanceVector& col_instances,
                                                    const ColumnConfigInstanceVector& tag_instances,
                                                    MemoryPool::MemoryBlock* batch) const {
    KafkaMessageBatch msg_batch;
    msg_batch.reserve((batch->total_rows + format_options_->records_per_message - 1) / format_options_->records_per_message);

    KeyGenerator key_generator(format_options_->key_pattern, format_options_->key_serializer, col_instances, tag_instances);

    if (format_options_->records_per_message == 1) {
        nlohmann::ordered_json json_data;

        for (size_t table_idx = 0; table_idx < batch->used_tables; ++table_idx) {
            auto& table_block = batch->tables[table_idx];
            for (size_t row_idx = 0; row_idx < table_block.used_rows; ++row_idx) {
                std::string key = key_generator.generate(table_block, row_idx);
                RowSerializer::to_json_inplace(col_instances, tag_instances, table_block, row_idx, format_options_->tbname_key, json_data);
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
                RowSerializer::to_json_inplace(col_instances, tag_instances, table_block, row_idx, format_options_->tbname_key, record_json);
                json_array.emplace_back(std::move(record_json));
                record_json.clear();

                // If the batch is full, create the message and reset
                if (json_array.size() >= format_options_->records_per_message) {
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

    auto payload = std::make_unique<KafkaInsertData>(batch, col_instances, tag_instances, std::move(msg_batch));
    return FormatResult(std::move(payload));
}

// Formats data into InfluxDB Line Protocol.
FormatResult KafkaInsertDataFormatter::format_influx(const ColumnConfigInstanceVector& col_instances,
                                                     const ColumnConfigInstanceVector& tag_instances,
                                                     MemoryPool::MemoryBlock* batch) const {
    KafkaMessageBatch msg_batch;
    msg_batch.reserve((batch->total_rows + format_options_->records_per_message - 1) / format_options_->records_per_message);

    KeyGenerator key_generator(format_options_->key_pattern, format_options_->key_serializer, col_instances, tag_instances);

    fmt::memory_buffer line_buffer;
    line_buffer.reserve(1048576);
    std::string first_record_key;
    size_t records_in_current_message = 0;

    for (size_t table_idx = 0; table_idx < batch->used_tables; ++table_idx) {
        auto& table_block = batch->tables[table_idx];

        for (size_t row_idx = 0; row_idx < table_block.used_rows; ++row_idx) {
            if (records_in_current_message == 0) {
                first_record_key = key_generator.generate(table_block, row_idx);
            } else {
                line_buffer.push_back('\n');
            }

            RowSerializer::to_influx_inplace(col_instances, tag_instances, table_block, row_idx, line_buffer);
            records_in_current_message++;

            // If the message is full, push it to the batch and reset
            if (records_in_current_message >= format_options_->records_per_message) {
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

    auto payload = std::make_unique<KafkaInsertData>(batch, col_instances, tag_instances, std::move(msg_batch));
    return FormatResult(std::move(payload));
}