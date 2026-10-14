#pragma once

/*
 * Copyright (c) 2026 TAOS Data, Inc.
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

#include "ColumnConfig.hpp"
#include "ColumnType.hpp"
#include "StringUtils.hpp"
#include <limits>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace TagUtils {

template <typename To, typename From>
inline To checked_cast(From value, const std::string& context) {
    static_assert(std::is_integral<To>::value, "target must be integral");
    static_assert(std::is_integral<From>::value, "source must be integral");

    if constexpr (std::is_signed<From>::value == std::is_signed<To>::value) {
        using Common = std::common_type_t<To, From>;
        const Common common_value = static_cast<Common>(value);
        if (common_value < static_cast<Common>(std::numeric_limits<To>::min()) ||
            common_value > static_cast<Common>(std::numeric_limits<To>::max())) {
            throw std::runtime_error("ingress " + context + " value out of range.");
        }
    } else if constexpr (std::is_signed<From>::value) {
        if (value < 0 ||
            static_cast<typename std::make_unsigned<From>::type>(value) >
                static_cast<typename std::make_unsigned<To>::type>(std::numeric_limits<To>::max())) {
            throw std::runtime_error("ingress " + context + " value out of range.");
        }
    } else {
        if (value > static_cast<typename std::make_unsigned<To>::type>(std::numeric_limits<To>::max())) {
            throw std::runtime_error("ingress " + context + " value out of range.");
        }
    }

    return static_cast<To>(value);
}

template <typename T>
inline bool column_value_equal_impl(const T& lhs, const T& rhs) {
    if constexpr (std::is_same_v<T, Decimal>) {
        return lhs.value == rhs.value;
    } else if constexpr (std::is_same_v<T, JsonValue>) {
        return lhs.raw_json == rhs.raw_json;
    } else if constexpr (std::is_same_v<T, Geometry>) {
        return lhs.wkt == rhs.wkt;
    } else {
        return lhs == rhs;
    }
}

inline bool column_value_equal(const ColumnType& lhs, const ColumnType& rhs) {
    return std::visit([](const auto& left, const auto& right) {
        using L = std::decay_t<decltype(left)>;
        using R = std::decay_t<decltype(right)>;
        if constexpr (std::is_same_v<L, R>) {
            return column_value_equal_impl(left, right);
        } else {
            return false;
        }
    }, lhs, rhs);
}

inline bool row_type_equal(const RowType& lhs, const RowType& rhs) {
    if (lhs.size() != rhs.size()) {
        return false;
    }

    for (size_t i = 0; i < lhs.size(); ++i) {
        if (!column_value_equal(lhs[i], rhs[i])) {
            return false;
        }
    }
    return true;
}

template <typename To>
inline ColumnType normalize_integral_value(const ColumnType& value, const std::string& context) {
    return std::visit([&](const auto& raw) -> ColumnType {
        using T = std::decay_t<decltype(raw)>;
        if constexpr (std::is_same_v<T, NullValue>) {
            return value;
        } else if constexpr (std::is_same_v<T, NoneValue>) {
            throw std::runtime_error("ingress " + context + " type does not match fixed schema.");
        } else if constexpr (std::is_integral_v<T> && !std::is_same_v<T, bool>) {
            return checked_cast<To>(raw, context);
        } else if constexpr (std::is_same_v<T, bool>) {
            throw std::runtime_error("ingress " + context + " type does not match fixed schema.");
        } else {
            throw std::runtime_error("ingress " + context + " type does not match fixed schema.");
        }
    }, value);
}

inline ColumnType normalize_tag_value(ColumnTypeTag tag, const ColumnType& value, const std::string& context) {
    if (std::holds_alternative<NullValue>(value)) {
        return value;
    }

    switch (tag) {
        case ColumnTypeTag::BOOL:
            if (std::holds_alternative<bool>(value)) {
                return value;
            }
            break;
        case ColumnTypeTag::TINYINT:
            return normalize_integral_value<int8_t>(value, context);
        case ColumnTypeTag::TINYINT_UNSIGNED:
            return normalize_integral_value<uint8_t>(value, context);
        case ColumnTypeTag::SMALLINT:
            return normalize_integral_value<int16_t>(value, context);
        case ColumnTypeTag::SMALLINT_UNSIGNED:
            return normalize_integral_value<uint16_t>(value, context);
        case ColumnTypeTag::INT:
            return normalize_integral_value<int32_t>(value, context);
        case ColumnTypeTag::INT_UNSIGNED:
            return normalize_integral_value<uint32_t>(value, context);
        case ColumnTypeTag::TIMESTAMP:
        case ColumnTypeTag::BIGINT:
            return normalize_integral_value<int64_t>(value, context);
        case ColumnTypeTag::BIGINT_UNSIGNED:
            return normalize_integral_value<uint64_t>(value, context);
        case ColumnTypeTag::FLOAT:
            if (std::holds_alternative<float>(value)) {
                return value;
            }
            break;
        case ColumnTypeTag::DOUBLE:
            if (std::holds_alternative<double>(value)) {
                return value;
            }
            break;
        case ColumnTypeTag::NCHAR:
            if (std::holds_alternative<std::u16string>(value)) {
                return value;
            }
            if (std::holds_alternative<std::string>(value)) {
                return StringUtils::utf8_to_u16string(std::get<std::string>(value));
            }
            break;
        case ColumnTypeTag::VARCHAR:
        case ColumnTypeTag::BINARY:
        case ColumnTypeTag::JSON:
            if (std::holds_alternative<std::string>(value)) {
                return value;
            }
            break;
        case ColumnTypeTag::VARBINARY:
            if (std::holds_alternative<std::vector<uint8_t>>(value)) {
                return value;
            }
            break;
        default:
            break;
    }

    throw std::runtime_error("ingress " + context + " type does not match fixed schema.");
}

inline RowType normalize_tag_row(const ColumnConfigVector& schema_tags,
                                 const RowType& values,
                                 const std::string& context) {
    if (schema_tags.size() != values.size()) {
        throw std::runtime_error("ingress " + context + " tag count mismatch.");
    }

    RowType normalized;
    normalized.reserve(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        normalized.push_back(normalize_tag_value(
            schema_tags[i].type_tag,
            values[i],
            context + " tag '" + schema_tags[i].name + "'"));
    }
    return normalized;
}

} // namespace TagUtils
