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
        : BaseInsertData(typeid(MqttInsertData), block, col_instances, tag_instances), data(std::move(msgs)) {}

    MqttInsertData(MqttInsertData&& other) noexcept
        : BaseInsertData(std::move(other))
        , data(std::move(other.data))
    {
        this->type = typeid(MqttInsertData);
    }

    // Disable copy
    MqttInsertData(const MqttInsertData&) = delete;
    MqttInsertData& operator=(const MqttInsertData&) = delete;
    MqttInsertData& operator=(MqttInsertData&&) = delete;

    ~MqttInsertData() = default;
};

inline std::type_index MQTT_TYPE_ID = std::type_index(typeid(MqttInsertData));
inline uint64_t MQTT_TYPE_HASH = MQTT_TYPE_ID.hash_code();