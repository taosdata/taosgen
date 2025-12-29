#pragma once
#include "BaseInsertData.hpp"
#include "SqlData.hpp"
#include <string>

struct SqlInsertData : public BaseInsertData {
    SqlData data;

    SqlInsertData(MemoryPool::MemoryBlock* block,
                  const ColumnConfigInstanceVector& col_instances,
                  const ColumnConfigInstanceVector& tag_instances,
                  std::string&& sql)
        : BaseInsertData(typeid(SqlInsertData), block, col_instances, tag_instances), data(std::move(sql)) {}

    SqlInsertData(SqlInsertData&& other) noexcept
        : BaseInsertData(std::move(other))
        , data(std::move(other.data))
    {
        this->type = typeid(SqlInsertData);
    }

    // Disable copy
    SqlInsertData(const SqlInsertData&) = delete;
    SqlInsertData& operator=(const SqlInsertData&) = delete;
    SqlInsertData& operator=(SqlInsertData&&) = delete;

    ~SqlInsertData() = default;
};

inline std::type_index SQL_TYPE_ID = std::type_index(typeid(SqlInsertData));
inline uint64_t SQL_TYPE_HASH = SQL_TYPE_ID.hash_code();