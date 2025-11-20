#pragma once

#include "ColumnConfigInstance.hpp"
#include "MemoryPool.hpp"
#include <nlohmann/json.hpp>

class RowSerializer {
public:
    static nlohmann::ordered_json to_json(
        const ColumnConfigInstanceVector& col_instances,
        const MemoryPool::TableBlock& table,
        size_t row_index,
        const std::string& tbname_key
    );
};
