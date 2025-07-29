#pragma once
#include <string>
#include "BaseInsertData.hpp"
#include "SqlData.hpp"


struct SqlInsertData : public BaseInsertData {
    SqlData data;

    SqlInsertData(int64_t start, int64_t end, size_t rows, std::string&& sql)
        : BaseInsertData(DataType::SQL, start, end, rows), data(std::move(sql)) {}

    SqlInsertData(SqlInsertData&& other) noexcept
        : BaseInsertData(std::move(other))
        , data(std::move(other.data))
    {
        this->type = DataType::SQL;
    }

    SqlInsertData& operator=(SqlInsertData&& other) noexcept {
        if (this != &other) {
            BaseInsertData::operator=(std::move(other));
            data = std::move(other.data);
            this->type = DataType::SQL;
        }
        return *this;
    }

    SqlInsertData(const SqlInsertData&) = delete;
    SqlInsertData& operator=(const SqlInsertData&) = delete;
};