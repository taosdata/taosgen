#pragma once

#include "IFormatter.hpp"
#include "FormatterFactory.hpp"
#include "TopicGenerator.hpp"
#include "Compressor.hpp"
#include "EncodingConverter.hpp"
#include "taos.h"
#include <nlohmann/json.hpp>
#include <sstream>
#include <limits>

class MsgInsertDataFormatter final : public IInsertDataFormatter {
public:
    explicit MsgInsertDataFormatter(const DataFormat& format) : format_(format) {}

    std::string prepare(const InsertDataConfig& config,
                        const ColumnConfigInstanceVector& col_instances) override {
        (void)config;
        (void)col_instances;
        return "";
    }

    FormatResult format(const InsertDataConfig& config,
                        const ColumnConfigInstanceVector& col_instances,
                        MemoryPool::MemoryBlock* batch, bool is_checkpoint_recover = false) const override{
        (void)is_checkpoint_recover;
        if (!batch || batch->total_rows == 0) {
            return FormatResult("");
        }

        const auto& mqtt_info = config.target.mqtt;
        return format_mqtt(mqtt_info, col_instances, batch);
    }

    static MsgInsertData format_mqtt(const MqttInfo& mqtt_info,
                                    const ColumnConfigInstanceVector& col_instances,
                                    MemoryPool::MemoryBlock* batch) {
        if (!batch || batch->total_rows == 0) {
            return MsgInsertData(batch, col_instances, {});
        }

        CompressionType compression_type = string_to_compression(mqtt_info.compression);
        EncodingType encoding_type = string_to_encoding(mqtt_info.encoding);

        MessageBatches msg_batches;
        size_t batch_count = (batch->total_rows + mqtt_info.batch_messages - 1) / mqtt_info.batch_messages;
        msg_batches.reserve(batch_count);

        MessageBatch msg_batch;
        msg_batch.reserve(mqtt_info.batch_messages);

        TopicGenerator topic_generator(mqtt_info.topic, col_instances);

        // Iterate over all subtables
        for (size_t table_idx = 0; table_idx < batch->used_tables; table_idx++) {
            auto& table_block = batch->tables[table_idx];

            // Iterate over all rows
            for (size_t row_idx = 0; row_idx < table_block.used_rows; ++row_idx) {
                std::string topic = topic_generator.generate(table_block, row_idx);
                nlohmann::ordered_json json_data = serialize_row_to_json(col_instances, table_block, row_idx);

                // Encoding conversion
                std::string payload = json_data.dump();
                if (encoding_type != EncodingType::UTF8) {
                    payload = EncodingConverter::convert(payload, EncodingType::UTF8, encoding_type);
                }

                // Compression
                if (compression_type != CompressionType::NONE) {
                    payload = Compressor::compress(payload, compression_type);
                }
                msg_batch.emplace_back(std::move(topic), std::move(payload));

                // Batch publish when reaching batch size
                if (msg_batch.size() >= mqtt_info.batch_messages) {
                    msg_batches.emplace_back(std::move(msg_batch));
                    msg_batch.clear();
                    msg_batch.reserve(mqtt_info.batch_messages);
                }
            }
        }

        // Publish any remaining messages
        if (!msg_batch.empty()) {
            msg_batches.emplace_back(std::move(msg_batch));
        }

        return MsgInsertData(batch, col_instances, std::move(msg_batches));
    }


private:
    const DataFormat& format_;


    static nlohmann::ordered_json serialize_row_to_json(
        const ColumnConfigInstanceVector& col_instances,
        const MemoryPool::TableBlock& table,
        size_t row_index
    ) {
        nlohmann::ordered_json json_data;

        // Add table name
        if (table.table_name) {
            json_data["table"] = table.table_name;
        }

        // Add timestamp
        if (table.timestamps && row_index < table.used_rows) {
            json_data["ts"] = table.timestamps[row_index];
        }

        // Add data columns
        for (size_t col_idx = 0; col_idx < col_instances.size(); col_idx++) {
            const auto& col_inst = col_instances[col_idx];
            try {
                const auto& cell = table.get_cell(row_index, col_idx);

                std::visit([&](const auto& value) {
                    using T = std::decay_t<decltype(value)>;
                    if constexpr (std::is_same_v<T, Decimal>) {
                        json_data[col_inst.name()] = value.value;
                    } else if constexpr (std::is_same_v<T, JsonValue>) {
                        json_data[col_inst.name()] = value.raw_json;
                    } else if constexpr (std::is_same_v<T, Geometry>) {
                        json_data[col_inst.name()] = value.wkt;
                    } else if constexpr (std::is_same_v<T, std::u16string>) {
                        // Convert to utf8 string
                        json_data[col_inst.name()] = std::string(value.begin(), value.end());
                    } else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
                        // Can be base64 or hex encoded, here simply convert to string
                        json_data[col_inst.name()] = std::string(value.begin(), value.end());
                    } else {
                        json_data[col_inst.name()] = value;
                    }
                }, cell);

            } catch (const std::exception& e) {
                // json_data[col_inst.name()] = "ERROR: " + std::string(e.what());
                throw std::runtime_error("serialize_row_to_json failed for column '" + col_inst.name() + "': " + e.what());
            }
        }

        return json_data;
    }

    inline static bool registered_ = []() {
        FormatterFactory::instance().register_formatter<InsertDataConfig>(
            "msg",
            [](const DataFormat& format) {
                return std::make_unique<MsgInsertDataFormatter>(format);
            });
        return true;
    }();
};