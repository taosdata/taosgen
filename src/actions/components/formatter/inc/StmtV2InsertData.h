#pragma once
#include "BaseInsertData.h"
#include "StmtV2Data.h"
#include "ColumnConfigInstance.h"
#include "TableData.h"
#include "BlockStmtV2Data.h"

struct StmtV2InsertData : public BaseInsertData {
    BlockStmtV2Data data;
    MemoryPool::MemoryBlock* block;
    
    StmtV2InsertData(MemoryPool::MemoryBlock* block, const ColumnConfigInstanceVector& col_instances)
        : BaseInsertData(DataType::STMT_V2, 
                         block->start_time,
                         block->end_time,
                         block->total_rows),
          data(block, col_instances),
          block(block) {
            block->build_bindv();
          }

    // Move constructor
    StmtV2InsertData(StmtV2InsertData&& other) noexcept 
        : BaseInsertData(std::move(other)),
          data(std::move(other.data)),
          block(other.block) 
    {
        other.block = nullptr;
    }
    
    // Disable copy
    StmtV2InsertData(const StmtV2InsertData&) = delete;
    StmtV2InsertData& operator=(const StmtV2InsertData&) = delete;
    
    ~StmtV2InsertData() {
        if (block) {
            block->release();
        }
    }
};