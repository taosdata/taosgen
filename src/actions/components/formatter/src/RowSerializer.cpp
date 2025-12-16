#include "RowSerializer.hpp"
#include <stdexcept>

nlohmann::ordered_json RowSerializer::to_json(
    const ColumnConfigInstanceVector& col_instances,
    const ColumnConfigInstanceVector& tag_instances,
    const MemoryPool::TableBlock& table,
    size_t row_index,
    const std::string& tbname_key) {

    if (row_index >= table.used_rows) {
        throw std::out_of_range("Row index " + std::to_string(row_index) + " is out of range for table with " + std::to_string(table.used_rows) + " used rows");
    }

    nlohmann::ordered_json json_data;

    // Add table name
    if (!tbname_key.empty() && table.table_name) {
        json_data[tbname_key] = table.table_name;
    }

    // Add timestamp
    if (table.timestamps) {
        json_data["ts"] = table.timestamps[row_index];
    }

    auto serialize_cell = [&](const auto& instances, size_t idx,
                              const char* context,
                              auto get_cell_fn) -> void {
        const auto& inst = instances[idx];
        try {
            const auto cell = get_cell_fn(row_index, idx);

            std::visit([&](const auto& value) {
                using T = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<T, Decimal>) {
                    json_data[inst.name()] = value.value;
                } else if constexpr (std::is_same_v<T, JsonValue>) {
                    json_data[inst.name()] = value.raw_json;
                } else if constexpr (std::is_same_v<T, Geometry>) {
                    json_data[inst.name()] = value.wkt;
                } else if constexpr (std::is_same_v<T, std::u16string>) {
                    json_data[inst.name()] = StringUtils::u16string_to_utf8(value);
                } else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
                    json_data[inst.name()] = std::string(value.begin(), value.end());
                } else {
                    json_data[inst.name()] = value;
                }
            }, cell);

        } catch (const std::exception& e) {
            throw std::runtime_error(
                std::string("RowSerializer::to_json failed for ") + context +
                    " '" + std::string(inst.name()) + "': " + e.what()
            );
        }
    };

    // Add data columns
    for (size_t i = 0; i < col_instances.size(); ++i) {
        serialize_cell(col_instances, i, "column",
                       [&](size_t r, size_t c) -> ColumnType {
                           return table.get_column_cell(r, c);
                       });
    }

    // Add tag columns
    for (size_t i = 0; i < tag_instances.size(); ++i) {
        serialize_cell(tag_instances, i, "tag",
                       [&](size_t r, size_t c) -> ColumnType {
                           return table.get_tag_cell(r, c);
                       });
    }

    return json_data;
}

void RowSerializer::to_json_inplace(
    const ColumnConfigInstanceVector& col_instances,
    const ColumnConfigInstanceVector& tag_instances,
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

    auto serialize_cell = [&](const auto& instances, size_t idx,
                              const char* context,
                              auto get_cell_fn) -> void {
        const auto& inst = instances[idx];
        try {
            const auto cell = get_cell_fn(row_index, idx);

            std::visit([&](const auto& value) {
                using T = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<T, Decimal>) {
                    out[inst.name()] = value.value;
                } else if constexpr (std::is_same_v<T, JsonValue>) {
                    out[inst.name()] = value.raw_json;
                } else if constexpr (std::is_same_v<T, Geometry>) {
                    out[inst.name()] = value.wkt;
                } else if constexpr (std::is_same_v<T, std::u16string>) {
                    out[inst.name()] = StringUtils::u16string_to_utf8(value);
                } else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
                    out[inst.name()] = std::string(value.begin(), value.end());
                } else {
                    out[inst.name()] = value;
                }
            }, cell);

        } catch (const std::exception& e) {
            throw std::runtime_error(
                std::string("RowSerializer::to_json_inplace failed for ")
                    + context + " '" + std::string(inst.name()) + "': " + e.what());
        }
    };

    // Add data columns
    for (size_t i = 0; i < col_instances.size(); ++i) {
        serialize_cell(col_instances, i, "column",
                    [&](size_t r, size_t c) -> ColumnType {
                        return table.get_column_cell(r, c);
                    });
    }

    // Add tag columns
    for (size_t i = 0; i < tag_instances.size(); ++i) {
        serialize_cell(tag_instances, i, "tag",
                    [&](size_t r, size_t c) -> ColumnType {
                        return table.get_tag_cell(r, c);
                    });
    }
}
