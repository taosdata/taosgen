#pragma once
#include "BaseInsertData.h"
#include "StmtV2Data.h"
#include "ColumnConfigInstance.h"
#include "TableData.h"


struct StmtV2InsertData : public BaseInsertData {
    StmtV2Data data;
    StmtV2InsertData(int64_t start, int64_t end, size_t rows, const ColumnConfigInstanceVector& col_instances, MultiBatch&& batch) 
        : BaseInsertData(DataType::STMT_V2, start, end, rows), data(col_instances, std::move(batch)) {}

    StmtV2InsertData(StmtV2InsertData&& other) noexcept 
        : BaseInsertData(std::move(other))
        , data(std::move(other.data))
    {
        this->type = DataType::STMT_V2;
    }
};
