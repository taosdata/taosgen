#pragma once

#include <stdexcept>
#include <cstring>
#include <variant>
#include "ColumnConfigInstance.h"


namespace ColumnConverter {

    using FixedHandler = void(*)(const ColumnType&, void*, size_t);
    using VarHandler = size_t(*)(const ColumnType&, char*, size_t);

    // Column handler function structure
    struct ColumnHandler {
        FixedHandler fixed_handler = nullptr;
        VarHandler var_handler = nullptr;
    };

    // Fixed-length type handler
    template<typename T>
    void fixed_type_handler(const ColumnType& value, void* dest, size_t size);

    // Variable-length type handler
    size_t string_type_handler(const ColumnType& value, char* dest, size_t max_len);
    size_t u16string_type_handler(const ColumnType& value, char* dest, size_t max_len);
    size_t binary_type_handler(const ColumnType& value, char* dest, size_t max_len);

    ColumnHandler create_handler_for_column(const ColumnConfigInstance& col_config);
    std::vector<ColumnHandler> create_handlers_for_columns(const ColumnConfigInstanceVector& col_instances);
}