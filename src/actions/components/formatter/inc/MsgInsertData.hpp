#pragma once
#include "BaseInsertData.hpp"
#include <string>
#include <vector>

using TopicPayloadPair = std::pair<std::string, std::string>;
using MessageBatch = std::vector<TopicPayloadPair>;
using MessageBatches = std::vector<MessageBatch>;

struct MsgInsertData : public BaseInsertData {
    MessageBatches data;

    MsgInsertData(int64_t start, int64_t end, size_t rows, MessageBatches&& msgs) noexcept
        : BaseInsertData(DataType::MSG, start, end, rows), data(std::move(msgs)) {}

    MsgInsertData(MsgInsertData&& other) noexcept
        : BaseInsertData(std::move(other))
        , data(std::move(other.data))
    {
        this->type = DataType::MSG;
    }

    MsgInsertData& operator=(MsgInsertData&& other) noexcept {
        if (this != &other) {
            BaseInsertData::operator=(std::move(other));
            data = std::move(other.data);
            this->type = DataType::MSG;
        }
        return *this;
    }

    MsgInsertData(const MsgInsertData&) = delete;
    MsgInsertData& operator=(const MsgInsertData&) = delete;
};