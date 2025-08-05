#pragma once
#include <cstdint>
#include <cstddef>


struct BaseInsertData {
    enum class DataType { SQL, STMT_V2 };
    DataType type;

    int64_t start_time;
    int64_t end_time;
    size_t total_rows;

    BaseInsertData(int64_t start, int64_t end, size_t rows) 
        : type(DataType::SQL), start_time(start), end_time(end), total_rows(rows) {}

    BaseInsertData(DataType t, int64_t start, int64_t end, size_t rows) 
        : type(t), start_time(start), end_time(end), total_rows(rows) {}
    virtual ~BaseInsertData() = default; 
};