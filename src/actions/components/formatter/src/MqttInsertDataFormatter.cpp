#include "MqttInsertDataFormatter.hpp"

std::string MqttInsertDataFormatter::prepare(const InsertDataConfig& config,
                                             const ColumnConfigInstanceVector& col_instances) {
    (void)config;
    (void)col_instances;
    return "";
}

FormatResult MqttInsertDataFormatter::format(const InsertDataConfig& config,
                                             const ColumnConfigInstanceVector& col_instances,
                                             MemoryPool::MemoryBlock* batch, bool is_checkpoint_recover) const {
    (void)is_checkpoint_recover;
    if (!batch || batch->total_rows == 0) {
        return FormatResult("");
    }

    return format_json(config.data_format.mqtt, col_instances, batch);
}

MqttInsertData MqttInsertDataFormatter::format_json(const DataFormat::MqttConfig& format,
                                                   const ColumnConfigInstanceVector& col_instances,
                                                   MemoryPool::MemoryBlock* batch) {
    CompressionType compression_type = string_to_compression(format.compression);
    EncodingType encoding_type = string_to_encoding(format.encoding);

    MqttMessageBatch msg_batch;
    msg_batch.reserve((batch->total_rows + format.records_per_message - 1) / format.records_per_message);

    TopicGenerator topic_generator(format.topic, col_instances);


    if (format.records_per_message == 1) {
        for (size_t table_idx = 0; table_idx < batch->used_tables; ++table_idx) {
            auto& table_block = batch->tables[table_idx];
            for (size_t row_idx = 0; row_idx < table_block.used_rows; ++row_idx) {
                std::string topic = topic_generator.generate(table_block, row_idx);
                nlohmann::ordered_json json_data = RowSerializer::to_json(col_instances, table_block, row_idx, format.tbname_key);
                std::string payload = json_data.dump();

                // Encoding conversion
                if (encoding_type != EncodingType::UTF8) {
                    payload = EncodingConverter::convert(payload, EncodingType::UTF8, encoding_type);
                }

                // Compression
                if (compression_type != CompressionType::NONE) {
                    payload = Compressor::compress(payload, compression_type);
                }

                msg_batch.emplace_back(std::move(topic), std::move(payload));
            }
        }
    } else {
        nlohmann::ordered_json json_array = nlohmann::ordered_json::array();
        std::string first_record_topic;

        for (size_t table_idx = 0; table_idx < batch->used_tables; ++table_idx) {
            auto& table_block = batch->tables[table_idx];
            for (size_t row_idx = 0; row_idx < table_block.used_rows; ++row_idx) {
                // If this is the first record of a new batch, generate the topic
                if (json_array.empty()) {
                    first_record_topic = topic_generator.generate(table_block, row_idx);
                }

                // Add the current record's JSON to the array
                json_array.push_back(RowSerializer::to_json(col_instances, table_block, row_idx, format.tbname_key));

                // If the batch is full, create the message and reset
                if (json_array.size() >= format.records_per_message) {
                    std::string payload = json_array.dump();

                    // Encoding conversion
                    if (encoding_type != EncodingType::UTF8) {
                        payload = EncodingConverter::convert(payload, EncodingType::UTF8, encoding_type);
                    }

                    // Compression
                    if (compression_type != CompressionType::NONE) {
                        payload = Compressor::compress(payload, compression_type);
                    }

                    msg_batch.emplace_back(std::move(first_record_topic), std::move(payload));
                    json_array.clear();
                }
            }
        }

        // Add any remaining records from the last partial batch
        if (!json_array.empty()) {
            std::string payload = json_array.dump();
            if (encoding_type != EncodingType::UTF8) {
                payload = EncodingConverter::convert(payload, EncodingType::UTF8, encoding_type);
            }
            if (compression_type != CompressionType::NONE) {
                payload = Compressor::compress(payload, compression_type);
            }
            msg_batch.emplace_back(std::move(first_record_topic), std::move(payload));
        }
    }

    return MqttInsertData(batch, col_instances, std::move(msg_batch));
}