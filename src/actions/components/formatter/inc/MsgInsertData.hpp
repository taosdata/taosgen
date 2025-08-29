#pragma once
#include "BaseInsertData.hpp"
#include <string>
#include <vector>

using TopicPayloadPair = std::pair<std::string, std::string>;
using MessageBatch = std::vector<TopicPayloadPair>;
using MessageBatches = std::vector<MessageBatch>;

struct MsgInsertData : public BaseInsertData {
    MessageBatches data;

    MsgInsertData(MemoryPool::MemoryBlock* block, const ColumnConfigInstanceVector& col_instances, MessageBatches&& msgs) noexcept
        : BaseInsertData(DataType::MSG, block, col_instances), data(std::move(msgs)) {}

    MsgInsertData(MsgInsertData&& other) noexcept
        : BaseInsertData(std::move(other))
        , data(std::move(other.data))
    {
        this->type = DataType::MSG;
    }

    // Disable copy
    MsgInsertData(const MsgInsertData&) = delete;
    MsgInsertData& operator=(const MsgInsertData&) = delete;
    MsgInsertData& operator=(MsgInsertData&&) = delete;

    ~MsgInsertData() = default;
};