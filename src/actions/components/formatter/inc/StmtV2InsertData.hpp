#pragma once
#include "BaseInsertData.hpp"
#include "StmtV2Data.hpp"
#include "ColumnConfigInstance.hpp"
#include "TableData.hpp"
#include "BlockStmtV2Data.hpp"

struct StmtV2InsertData : public BaseInsertData {
    BlockStmtV2Data data;

    StmtV2InsertData(MemoryPool::MemoryBlock* block, const ColumnConfigInstanceVector& col_instances)
        : BaseInsertData(DataType::STMT,
                         block->start_time,
                         block->end_time,
                         block->total_rows),
          data(block, col_instances) {
            block->build_bindv();
          }

    // Move constructor
    StmtV2InsertData(StmtV2InsertData&& other) noexcept
        : BaseInsertData(std::move(other)),
          data(std::move(other.data)) {
        other.data.reset_block();
    }

    // Disable copy
    StmtV2InsertData(const StmtV2InsertData&) = delete;
    StmtV2InsertData& operator=(const StmtV2InsertData&) = delete;

    ~StmtV2InsertData() {
        auto* block = data.get_block();
        if (block) {
            block->release();
        }
    }
};