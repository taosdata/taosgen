#include "MqttInsertData.hpp"
#include "MqttInsertDataFormatter.hpp"

std::string MqttInsertDataFormatter::prepare(const InsertDataConfig& config,
                                             const ColumnConfigInstanceVector& col_instances,
                                             const ColumnConfigInstanceVector& tag_instances) {
    (void)config;
    (void)col_instances;
    (void)tag_instances;
    return "";
}

FormatResult MqttInsertDataFormatter::format(const InsertDataConfig& config,
                                             const ColumnConfigInstanceVector& col_instances,
                                             const ColumnConfigInstanceVector& tag_instances,
                                             MemoryPool::MemoryBlock* batch, bool is_checkpoint_recover) const {
    (void)config;
    (void)is_checkpoint_recover;
    if (!batch || batch->total_rows == 0) {
        return FormatResult("");
    }

    return format_json(col_instances, tag_instances, batch);
}

FormatResult MqttInsertDataFormatter::format_json(const ColumnConfigInstanceVector& col_instances,
                                                  const ColumnConfigInstanceVector& tag_instances,
                                                  MemoryPool::MemoryBlock* batch) const {
    const DataFormat::MqttConfig& format = format_.mqtt;
    CompressionType compression_type = string_to_compression(format.compression);
    EncodingType encoding_type = string_to_encoding(format.encoding);

    MqttMessageBatch msg_batch;
    msg_batch.reserve((batch->total_rows + format.records_per_message - 1) / format.records_per_message);

    TopicGenerator topic_generator(format.topic, col_instances, tag_instances);


    if (format.records_per_message == 1) {
        nlohmann::ordered_json json_data;

        for (size_t table_idx = 0; table_idx < batch->used_tables; ++table_idx) {
            auto& table_block = batch->tables[table_idx];
            for (size_t row_idx = 0; row_idx < table_block.used_rows; ++row_idx) {
                std::string topic = topic_generator.generate(table_block, row_idx);
                RowSerializer::to_json_inplace(col_instances, tag_instances, table_block, row_idx, format.tbname_key, json_data);
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
                json_data.clear();
            }
        }
    } else {
        nlohmann::ordered_json json_array = nlohmann::ordered_json::array();
        std::string first_record_topic;
        nlohmann::ordered_json record_json;

        for (size_t table_idx = 0; table_idx < batch->used_tables; ++table_idx) {
            auto& table_block = batch->tables[table_idx];
            for (size_t row_idx = 0; row_idx < table_block.used_rows; ++row_idx) {
                // If this is the first record of a new batch, generate the topic
                if (json_array.empty()) {
                    first_record_topic = topic_generator.generate(table_block, row_idx);
                }

                // Add the current record's JSON to the array
                RowSerializer::to_json_inplace(col_instances, tag_instances, table_block, row_idx, format.tbname_key, record_json);
                json_array.emplace_back(std::move(record_json));
                record_json.clear();

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

    auto payload = std::make_unique<MqttInsertData>(batch, col_instances, tag_instances, std::move(msg_batch));
    return FormatResult(std::move(payload));
}