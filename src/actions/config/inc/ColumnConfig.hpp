#pragma once

#include <string>
#include <vector>
#include <optional>
#include <cstdint>
#include "ColumnType.hpp"
#include "StringUtils.hpp"


struct ColumnConfig {
    std::string name;
    std::string type;
    ColumnTypeTag type_tag;
    size_t type_index = std::variant_npos;
    bool primary_key = false;
    std::optional<int> len;
    size_t count = 1;
    std::optional<int> precision;
    std::optional<int> scale;
    std::optional<std::string> properties;
    std::optional<std::string> gen_type;
    std::optional<float> null_ratio;
    std::optional<float> none_ratio;

    // Attributes for gen_type=random
    std::optional<std::string> distribution;
    std::optional<double> min;
    std::optional<double> max;
    std::optional<std::string> dec_min;
    std::optional<std::string> dec_max;
    std::optional<std::string> corpus;
    std::optional<bool> chinese;
    std::optional<std::vector<std::string>> values;

    // Attributes for gen_type=order
    std::optional<int64_t> order_min;
    std::optional<int64_t> order_max;

    // Attributes for gen_type=expression
    std::optional<std::string> formula; 

    ColumnConfig() = default;
    ColumnConfig(const std::string& name, const std::string& type);
    ColumnConfig(const std::string& name, const std::string& type, std::optional<std::string> gen_type);
    ColumnConfig(const std::string& name, const std::string& type, std::optional<std::string> gen_type, std::optional<double> min, std::optional<double> max);

    static ColumnTypeTag get_type_tag(const std::string& type_str);

    std::size_t get_type_index(const std::string& type_str);

    void parse_type();

    double get_min_value() const noexcept;
    
    double get_max_value() const noexcept;

    // Check if the column type is variable length
    bool is_var_length() const noexcept;

    size_t get_fixed_type_size() const noexcept;

    // Convert to TDengine internal type
    int get_taos_type() const noexcept;
};

using ColumnConfigVector = std::vector<ColumnConfig>;