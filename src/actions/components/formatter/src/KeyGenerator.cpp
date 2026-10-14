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

#include "KeyGenerator.hpp"
#include <algorithm>
#include <cstring>
#include <stdexcept>

const bool KeyGenerator::is_little_endian_ = [] {
    uint16_t num = 0x0001;
    return *(reinterpret_cast<uint8_t*>(&num)) == 0x01;
}();

KeyGenerator::KeyGenerator(const std::string& pattern,
                           const std::string& serializer,
                           const ColumnConfigInstanceVector& col_instances,
                           const ColumnConfigInstanceVector& tag_instances) {
    serializer_type_ = string_to_serializer(serializer);

    // Parse the pattern
    tokens_ = parse_pattern(pattern);

    if (serializer_type_ != SerializerType::STRING_UTF8) {
        if (!(tokens_.size() == 1 && tokens_[0].type == Token::PLACEHOLDER)) {
            throw std::invalid_argument("For integer key_serializer, key_pattern must be a single placeholder like '{column_name}', but got '" + pattern + "'.");
        }
        single_placeholder_key_ = tokens_[0].value;
    }

    // Build mapping
    build_mapping(col_instances, tag_instances);
}

std::string KeyGenerator::generate(const MemoryPool::TableBlock& data, size_t row_index) const {
    if (serializer_type_ == SerializerType::STRING_UTF8) {
        std::ostringstream oss;
        for (const auto& token : tokens_) {
            if (token.type == Token::TEXT) {
                oss << token.value;
            } else {
                oss << get_value_as_string(token.value, data, row_index);
            }
        }
        return oss.str();
    } else {
        // Integer serialization path
        std::string value_str = get_value_as_string(single_placeholder_key_, data, row_index);
        try {
            switch (serializer_type_) {
                case SerializerType::INT8:   return serialize_to_big_endian(static_cast<int8_t>(std::stoll(value_str)));
                case SerializerType::UINT8:  return serialize_to_big_endian(static_cast<uint8_t>(std::stoull(value_str)));
                case SerializerType::INT16:  return serialize_to_big_endian(static_cast<int16_t>(std::stoll(value_str)));
                case SerializerType::UINT16: return serialize_to_big_endian(static_cast<uint16_t>(std::stoull(value_str)));
                case SerializerType::INT32:  return serialize_to_big_endian(static_cast<int32_t>(std::stoll(value_str)));
                case SerializerType::UINT32: return serialize_to_big_endian(static_cast<uint32_t>(std::stoull(value_str)));
                case SerializerType::INT64:  return serialize_to_big_endian(static_cast<int64_t>(std::stoll(value_str)));
                case SerializerType::UINT64: return serialize_to_big_endian(static_cast<uint64_t>(std::stoull(value_str)));
                default: return ""; // Should not happen
            }
        } catch (const std::exception& e) {
            throw std::runtime_error("Failed to parse key value '" + value_str + "' for integer serialization: " + e.what());
        }
    }
}

template<typename T>
std::string KeyGenerator::serialize_to_big_endian(T value) const {
    std::string bytes(sizeof(T), '\0');
    std::memcpy(&bytes[0], &value, sizeof(T));

    if (is_little_endian_) {
        std::reverse(bytes.begin(), bytes.end());
    }
    return bytes;
}

KeyGenerator::SerializerType KeyGenerator::string_to_serializer(const std::string& s) {
    if (s == "string-utf8") return SerializerType::STRING_UTF8;
    if (s == "int8") return SerializerType::INT8;
    if (s == "uint8") return SerializerType::UINT8;
    if (s == "int16") return SerializerType::INT16;
    if (s == "uint16") return SerializerType::UINT16;
    if (s == "int32") return SerializerType::INT32;
    if (s == "uint32") return SerializerType::UINT32;
    if (s == "int64") return SerializerType::INT64;
    if (s == "uint64") return SerializerType::UINT64;
    throw std::invalid_argument("Unsupported key_serializer: " + s);
}