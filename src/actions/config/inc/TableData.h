#pragma once

#include "ColumnType.h"


struct RowData {
    // std::string table_name;
    // std::optional<int64_t> timestamp;
    int64_t timestamp;
    RowType columns;
};

struct TableData {
    std::string table_name;
    std::vector<int64_t> timestamps;
    std::vector<RowType> rows;
};

struct MultiBatch {
    std::vector<std::pair<std::string, std::vector<RowData>>> table_batches;
    int64_t start_time{0};
    int64_t end_time{0};
    size_t total_rows{0};

    MultiBatch() = default;
    MultiBatch(MultiBatch&& other) noexcept 
        : table_batches(std::move(other.table_batches))
        , start_time(other.start_time)
        , end_time(other.end_time)
        , total_rows(other.total_rows)
    {
        other.start_time = 0;
        other.end_time = 0;
        other.total_rows = 0;
    }
    
    MultiBatch& operator=(MultiBatch&& other) noexcept {
        if (this != &other) {
            table_batches = std::move(other.table_batches);
            start_time = other.start_time;
            end_time = other.end_time;
            total_rows = other.total_rows;
            
            other.start_time = 0;
            other.end_time = 0;
            other.total_rows = 0;
        }
        return *this;
    }

    void reserve(size_t table_count) {
        table_batches.reserve(table_count);
    }
};