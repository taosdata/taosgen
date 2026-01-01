#pragma once
#include "BaseInsertData.hpp"
#include <string>
#include <vector>

using KeyValuePayload = std::pair<std::string, std::string>;
using KafkaMessageBatch = std::vector<KeyValuePayload>;

struct KafkaInsertData : public BaseInsertData {
    KafkaMessageBatch data;

    KafkaInsertData(MemoryPool::MemoryBlock* block,
                    const ColumnConfigInstanceVector& col_instances,
                    const ColumnConfigInstanceVector& tag_instances,
                    KafkaMessageBatch&& msgs) noexcept
        : BaseInsertData(typeid(KafkaInsertData), block, col_instances, tag_instances), data(std::move(msgs)) {}

    KafkaInsertData(KafkaInsertData&& other) noexcept
        : BaseInsertData(std::move(other))
        , data(std::move(other.data))
    {
        this->type = typeid(KafkaInsertData);
    }

    // Disable copy
    KafkaInsertData(const KafkaInsertData&) = delete;
    KafkaInsertData& operator=(const KafkaInsertData&) = delete;
    KafkaInsertData& operator=(KafkaInsertData&&) = delete;

    ~KafkaInsertData() = default;
};

inline std::type_index KAFKA_TYPE_ID = std::type_index(typeid(KafkaInsertData));
inline uint64_t KAFKA_TYPE_HASH = KAFKA_TYPE_ID.hash_code();