#pragma once
#include "BaseInsertData.hpp"
#include <string>
#include <vector>

using TopicPayloadPair = std::pair<std::string, std::string>;
using MqttMessageBatch = std::vector<TopicPayloadPair>;

struct MqttInsertData : public BaseInsertData {
    MqttMessageBatch data;

    MqttInsertData(MemoryPool::MemoryBlock* block,
                   const ColumnConfigInstanceVector& col_instances,
                   const ColumnConfigInstanceVector& tag_instances,
                   MqttMessageBatch&& msgs) noexcept
        : BaseInsertData(DataType::MQTT, block, col_instances, tag_instances), data(std::move(msgs)) {}

    MqttInsertData(MqttInsertData&& other) noexcept
        : BaseInsertData(std::move(other))
        , data(std::move(other.data))
    {
        this->type = DataType::MQTT;
    }

    // Disable copy
    MqttInsertData(const MqttInsertData&) = delete;
    MqttInsertData& operator=(const MqttInsertData&) = delete;
    MqttInsertData& operator=(MqttInsertData&&) = delete;

    ~MqttInsertData() = default;
};