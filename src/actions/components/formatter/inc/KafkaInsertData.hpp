#pragma once
#include "BaseInsertData.hpp"
#include <string>
#include <vector>

using KeyValuePayload = std::pair<std::string, std::string>;
using KafkaMessageBatch = std::vector<KeyValuePayload>;

struct KafkaInsertData : public BaseInsertData {
    KafkaMessageBatch data;

    KafkaInsertData(MemoryPool::MemoryBlock* block, const ColumnConfigInstanceVector& col_instances, KafkaMessageBatch&& msgs) noexcept
        : BaseInsertData(DataType::KAFKA, block, col_instances), data(std::move(msgs)) {}

    KafkaInsertData(KafkaInsertData&& other) noexcept
        : BaseInsertData(std::move(other))
        , data(std::move(other.data))
    {
        this->type = DataType::KAFKA;
    }

    // Disable copy
    KafkaInsertData(const KafkaInsertData&) = delete;
    KafkaInsertData& operator=(const KafkaInsertData&) = delete;
    KafkaInsertData& operator=(KafkaInsertData&&) = delete;

    ~KafkaInsertData() = default;
};