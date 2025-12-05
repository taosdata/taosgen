#include "RowSerializer.hpp"
#include <stdexcept>

nlohmann::ordered_json RowSerializer::to_json(
    const ColumnConfigInstanceVector& col_instances,
    const MemoryPool::TableBlock& table,
    size_t row_index,
    const std::string& tbname_key
) {
    nlohmann::ordered_json json_data;

    if (row_index >= table.used_rows) {
        throw std::out_of_range("Row index " + std::to_string(row_index) + " is out of range for table with " + std::to_string(table.used_rows) + " used rows");
    }

    // Add table name
    if (!tbname_key.empty() && table.table_name) {
        json_data[tbname_key] = table.table_name;
    }

    // Add timestamp
    if (table.timestamps) {
        json_data["ts"] = table.timestamps[row_index];
    }

    // Add data columns
    for (size_t col_idx = 0; col_idx < col_instances.size(); col_idx++) {
        const auto& col_inst = col_instances[col_idx];
        try {
            const auto& cell = table.get_cell(row_index, col_idx);

            std::visit([&](const auto& value) {
                using T = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<T, Decimal>) {
                    json_data[col_inst.name()] = value.value;
                } else if constexpr (std::is_same_v<T, JsonValue>) {
                    json_data[col_inst.name()] = value.raw_json;
                } else if constexpr (std::is_same_v<T, Geometry>) {
                    json_data[col_inst.name()] = value.wkt;
                } else if constexpr (std::is_same_v<T, std::u16string>) {
                    // Convert to utf8 string
                    json_data[col_inst.name()] = StringUtils::u16string_to_utf8(value);
                } else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
                    // Can be base64 or hex encoded, here simply convert to string
                    json_data[col_inst.name()] = std::string(value.begin(), value.end());
                } else {
                    json_data[col_inst.name()] = value;
                }
            }, cell);

        } catch (const std::exception& e) {
            throw std::runtime_error("RowSerializer::to_json failed for column '" + col_inst.name() + "': " + e.what());
        }
    }

    return json_data;
}


void RowSerializer::to_json_inplace(
    const ColumnConfigInstanceVector& col_instances,
    const MemoryPool::TableBlock& table,
    size_t row_index,
    const std::string& tbname_key,
    nlohmann::ordered_json& out) {

    if (row_index >= table.used_rows) {
        throw std::out_of_range("Row index " + std::to_string(row_index) + " is out of range for table with " + std::to_string(table.used_rows) + " used rows");
    }

    // Add table name
    if (!tbname_key.empty() && table.table_name) {
        out[tbname_key] = table.table_name;
    }

    // Add timestamp
    if (table.timestamps) {
        out["ts"] = table.timestamps[row_index];
    }

    // Add data columns
    for (size_t col_idx = 0; col_idx < col_instances.size(); col_idx++) {
        const auto& col_inst = col_instances[col_idx];
        try {
            const auto& cell = table.get_cell(row_index, col_idx);

            std::visit([&](const auto& value) {
                using T = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<T, Decimal>) {
                    out[col_inst.name()] = value.value;
                } else if constexpr (std::is_same_v<T, JsonValue>) {
                    out[col_inst.name()] = value.raw_json;
                } else if constexpr (std::is_same_v<T, Geometry>) {
                    out[col_inst.name()] = value.wkt;
                } else if constexpr (std::is_same_v<T, std::u16string>) {
                    // Convert to utf8 string
                    out[col_inst.name()] = StringUtils::u16string_to_utf8(value);
                } else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
                    // Can be base64 or hex encoded, here simply convert to string
                    out[col_inst.name()] = std::string(value.begin(), value.end());
                } else {
                    out[col_inst.name()] = value;
                }
            }, cell);

        } catch (const std::exception& e) {
            throw std::runtime_error("RowSerializer::to_json_inplace failed for column '" + col_inst.name() + "': " + e.what());
        }
    }
}