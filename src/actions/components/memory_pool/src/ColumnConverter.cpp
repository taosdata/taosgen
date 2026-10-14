/*
 * Copyright (c) 2025 TAOS Data, Inc.
 *
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#include "ColumnConverter.hpp"
#include <sstream>
#include <fmt/format.h>

namespace ColumnConverter {

    size_t string_type_handler(const ColumnType& value, char* dest, size_t max_len) {
        const auto& str = std::get<std::string>(value);
        if (str.size() > max_len) {
            throw std::runtime_error("variable-length value exceeds schema capacity");
        }
        size_t len = str.size();
        memcpy(dest, str.data(), len);
        return len;
    }

    size_t u16string_type_handler(const ColumnType& value, char* dest, size_t max_len) {
        const auto& str = std::get<std::u16string>(value);
        std::string utf8 = StringUtils::u16string_to_utf8(str);
        if (utf8.size() > max_len) {
            throw std::runtime_error("variable-length value exceeds schema capacity");
        }
        size_t len = utf8.size();
        memcpy(dest, utf8.data(), len);
        return len;
    }

    size_t json_type_handler(const ColumnType& value, char* dest, size_t max_len) {
        const auto& json = std::get<JsonValue>(value);
        if (json.raw_json.size() > max_len) {
            throw std::runtime_error("variable-length value exceeds schema capacity");
        }
        size_t len = json.raw_json.size();
        memcpy(dest, json.raw_json.data(), len);
        return len;
    }

    size_t binary_type_handler(const ColumnType& value, char* dest, size_t max_len) {
        const auto& bin = std::get<std::vector<uint8_t>>(value);
        if (bin.size() > max_len) {
            throw std::runtime_error("variable-length value exceeds schema capacity");
        }
        size_t len = bin.size();
        memcpy(dest, bin.data(), len);
        return len;
    }

    ColumnType string_to_column(const char* src, size_t len) {
        return std::string(src, len);
    }

    ColumnType u16string_to_column(const char* src, size_t len) {
        std::string utf8_str(src, len);
        return StringUtils::utf8_to_u16string(utf8_str);
    }

    ColumnType json_to_column(const char* src, size_t len) {
        return JsonValue{std::string(src, len)};
    }

    ColumnType binary_to_column(const char* src, size_t len) {
        const uint8_t* byte_ptr = reinterpret_cast<const uint8_t*>(src);
        return std::vector<uint8_t>(byte_ptr, byte_ptr + len);
    }

    std::string column_to_string(const ColumnType& value) {
        return fmt::format("{}", value);
    }

    ColumnHandler create_handler_for_column(const ColumnConfigInstance& col_instance) {
        ColumnHandler handler;
        const auto& config = col_instance.config();
        ColumnTypeTag tag = config.type_tag;

        handler.to_string = column_to_string;

        if (!config.is_var_length()) {
            // Fixed-length column handling
            switch (tag) {
                case ColumnTypeTag::BOOL:
                    handler.to_fixed = fixed_type_handler<bool>;
                    handler.to_column_fixed = fixed_to_column<bool>;
                    break;
                case ColumnTypeTag::TINYINT:
                    handler.to_fixed = fixed_type_handler<int8_t>;
                    handler.to_column_fixed = fixed_to_column<int8_t>;
                    break;
                case ColumnTypeTag::TINYINT_UNSIGNED:
                    handler.to_fixed = fixed_type_handler<uint8_t>;
                    handler.to_column_fixed = fixed_to_column<uint8_t>;
                    break;
                case ColumnTypeTag::SMALLINT:
                    handler.to_fixed = fixed_type_handler<int16_t>;
                    handler.to_column_fixed = fixed_to_column<int16_t>;
                    break;
                case ColumnTypeTag::SMALLINT_UNSIGNED:
                    handler.to_fixed = fixed_type_handler<uint16_t>;
                    handler.to_column_fixed = fixed_to_column<uint16_t>;
                    break;
                case ColumnTypeTag::INT:
                    handler.to_fixed = fixed_type_handler<int32_t>;
                    handler.to_column_fixed = fixed_to_column<int32_t>;
                    break;
                case ColumnTypeTag::INT_UNSIGNED:
                    handler.to_fixed = fixed_type_handler<uint32_t>;
                    handler.to_column_fixed = fixed_to_column<uint32_t>;
                    break;
                case ColumnTypeTag::TIMESTAMP:
                case ColumnTypeTag::BIGINT:
                    handler.to_fixed = fixed_type_handler<int64_t>;
                    handler.to_column_fixed = fixed_to_column<int64_t>;
                    break;
                case ColumnTypeTag::BIGINT_UNSIGNED:
                    handler.to_fixed = fixed_type_handler<uint64_t>;
                    handler.to_column_fixed = fixed_to_column<uint64_t>;
                    break;
                case ColumnTypeTag::FLOAT:
                    handler.to_fixed = fixed_type_handler<float>;
                    handler.to_column_fixed = fixed_to_column<float>;
                    break;
                case ColumnTypeTag::DOUBLE:
                    handler.to_fixed = fixed_type_handler<double>;
                    handler.to_column_fixed = fixed_to_column<double>;
                    break;
                case ColumnTypeTag::DECIMAL:
                    handler.to_fixed = fixed_type_handler<Decimal>;
                    handler.to_column_fixed = fixed_to_column<Decimal>;
                    break;
                default:
                    throw std::runtime_error("Unsupported fixed-length type");
            }
        } else {
            // Variable-length column handling
            switch (tag) {
                case ColumnTypeTag::VARCHAR:
                case ColumnTypeTag::BINARY:
                    handler.to_var = string_type_handler;
                    handler.to_column_var = string_to_column;
                    break;

                case ColumnTypeTag::NCHAR:
                    handler.to_var = u16string_type_handler;
                    handler.to_column_var = u16string_to_column;
                    break;

                case ColumnTypeTag::JSON:
                    handler.to_var = json_type_handler;
                    handler.to_column_var = json_to_column;
                    break;

                case ColumnTypeTag::VARBINARY:
                    handler.to_var = binary_type_handler;
                    handler.to_column_var = binary_to_column;
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
