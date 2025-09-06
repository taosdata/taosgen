#pragma once
#include "BaseInsertData.hpp"
#include "ColumnConfigInstance.hpp"
#include "TableData.hpp"

struct StmtV2InsertData : public BaseInsertData {

    StmtV2InsertData(MemoryPool::MemoryBlock* block, const ColumnConfigInstanceVector& col_instances, bool is_checkpoint_recover = false)
        : BaseInsertData(DataType::STMT, block, col_instances) {
            block->build_bindv(is_checkpoint_recover);
          }

    StmtV2InsertData(StmtV2InsertData&& other) noexcept
          : BaseInsertData(std::move(other))
    {
        this->type = DataType::STMT;
    }

    // Disable copy
    StmtV2InsertData(const StmtV2InsertData&) = delete;
    StmtV2InsertData& operator=(const StmtV2InsertData&) = delete;
    StmtV2InsertData& operator=(StmtV2InsertData&&) = delete;

    ~StmtV2InsertData() = default;
};