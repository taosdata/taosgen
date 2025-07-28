#include "ColumnConverter.h"

namespace ColumnConverter {
    template<typename T>
    void fixed_type_handler(const ColumnType& value, void* dest, size_t size) {
        const T& val = std::get<T>(value);
        (void)size;
        // if (sizeof(T) != size) {
        //     throw std::runtime_error("Fixed-length type size mismatch");
        // }
        memcpy(dest, &val, sizeof(T));
    }

    // Explicit instantiation for all supported types
    template void fixed_type_handler<bool>(const ColumnType&, void*, size_t);
    template void fixed_type_handler<int8_t>(const ColumnType&, void*, size_t);
    template void fixed_type_handler<uint8_t>(const ColumnType&, void*, size_t);
    template void fixed_type_handler<int16_t>(const ColumnType&, void*, size_t);
    template void fixed_type_handler<uint16_t>(const ColumnType&, void*, size_t);
    template void fixed_type_handler<int32_t>(const ColumnType&, void*, size_t);
    template void fixed_type_handler<uint32_t>(const ColumnType&, void*, size_t);
    template void fixed_type_handler<int64_t>(const ColumnType&, void*, size_t);
    template void fixed_type_handler<uint64_t>(const ColumnType&, void*, size_t);
    template void fixed_type_handler<float>(const ColumnType&, void*, size_t);
    template void fixed_type_handler<double>(const ColumnType&, void*, size_t);
    template void fixed_type_handler<Decimal>(const ColumnType&, void*, size_t);

    size_t string_type_handler(const ColumnType& value, char* dest, size_t max_len) {
        const auto& str = std::get<std::string>(value);
        size_t len = std::min(str.size(), max_len);
        memcpy(dest, str.data(), len);
        return len;
    }

    size_t u16string_type_handler(const ColumnType& value, char* dest, size_t max_len) {
        const auto& str = std::get<std::u16string>(value);
        size_t byte_len = str.size() * sizeof(char16_t);
        size_t len = std::min(byte_len, max_len);
        memcpy(dest, str.data(), len);
        return len;
    }

    size_t binary_type_handler(const ColumnType& value, char* dest, size_t max_len) {
        const auto& bin = std::get<std::vector<uint8_t>>(value);
        size_t len = std::min(bin.size(), max_len);
        memcpy(dest, bin.data(), len);
        return len;
    }

    ColumnHandler create_handler_for_column(const ColumnConfigInstance& col_instance) {
        ColumnHandler handler;
        const auto& config = col_instance.config();
        ColumnTypeTag tag = config.type_tag;
        
        if (!config.is_var_length()) {
            // Fixed-length column processing
            switch (tag) {
                case ColumnTypeTag::BOOL:
                    handler.fixed_handler = fixed_type_handler<bool>;
                    break;
                case ColumnTypeTag::TINYINT:
                    handler.fixed_handler = fixed_type_handler<int8_t>;
                    break;
                case ColumnTypeTag::TINYINT_UNSIGNED:
                    handler.fixed_handler = fixed_type_handler<uint8_t>;
                    break;
                case ColumnTypeTag::SMALLINT:
                    handler.fixed_handler = fixed_type_handler<int16_t>;
                    break;
                case ColumnTypeTag::SMALLINT_UNSIGNED:
                    handler.fixed_handler = fixed_type_handler<uint16_t>;
                    break;
                case ColumnTypeTag::INT:
                    handler.fixed_handler = fixed_type_handler<int32_t>;
                    break;
                case ColumnTypeTag::INT_UNSIGNED:
                    handler.fixed_handler = fixed_type_handler<uint32_t>;
                    break;
                case ColumnTypeTag::BIGINT:
                    handler.fixed_handler = fixed_type_handler<int64_t>;
                    break;
                case ColumnTypeTag::BIGINT_UNSIGNED:
                    handler.fixed_handler = fixed_type_handler<uint64_t>;
                    break;
                case ColumnTypeTag::FLOAT:
                    handler.fixed_handler = fixed_type_handler<float>;
                    break;
                case ColumnTypeTag::DOUBLE:
                    handler.fixed_handler = fixed_type_handler<double>;
                    break;
                case ColumnTypeTag::DECIMAL:
                    handler.fixed_handler = fixed_type_handler<Decimal>;
                    break;
                default:
                    throw std::runtime_error("Unsupported fixed-length type");
            }
        } else {
            // Variable-length column processing
            switch (tag) {
                case ColumnTypeTag::VARCHAR:
                case ColumnTypeTag::BINARY:
                case ColumnTypeTag::JSON:
                    handler.var_handler = string_type_handler;
                    break;
                case ColumnTypeTag::NCHAR:
                    handler.var_handler = u16string_type_handler;
                    break;
                case ColumnTypeTag::VARBINARY:
                    handler.var_handler = binary_type_handler;
                    break;
                default:
                    throw std::runtime_error("Unsupported var-length type");
            }
        }
        
        return handler;
    }

    std::vector<ColumnHandler> create_handlers_for_columns(const ColumnConfigInstanceVector& col_instances) {
        std::vector<ColumnHandler> col_handlers;
        col_handlers.reserve(col_instances.size());
        for (const auto& col_inst : col_instances) {
            col_handlers.push_back(create_handler_for_column(col_inst));
        }
        return col_handlers;
    }
}