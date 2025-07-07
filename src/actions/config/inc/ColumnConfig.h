#pragma once

#include <string>
#include <vector>
#include <optional>
#include <cstdint>
#include "ColumnType.h"
#include "StringUtils.h"


struct ColumnConfig {
    std::string name;
    std::string type;
    ColumnTypeTag type_tag;
    bool primary_key = false;
    std::optional<int> len;
    size_t count = 1;
    std::optional<int> precision;
    std::optional<int> scale;
    std::optional<std::string> properties;
    std::optional<std::string> gen_type;
    std::optional<float> null_ratio;

    // Attributes for gen_type=random
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

    // Attributes for gen_type=function
    struct FunctionConfig {
        std::string expression; // Full function expression
        std::string function;   // Function name, e.g. sinusoid, counter, etc.
        double multiple = 1.0;  // Multiplier
        double addend = 0.0;    // Addend
        int random = 0;         // Random range
        double base = 0.0;      // Base value
        std::optional<double> min; // Function parameter: min value
        std::optional<double> max; // Function parameter: max value
        std::optional<int> period; // Function parameter: period
        std::optional<int> offset; // Function parameter: offset
    };
    std::optional<FunctionConfig> function_config;

    static ColumnTypeTag get_type_tag(const std::string& type_str);

    ColumnConfig() = default;
    ColumnConfig(const std::string& name, const std::string& type);
    ColumnConfig(const std::string& name, const std::string& type, std::optional<std::string> gen_type);
    ColumnConfig(const std::string& name, const std::string& type, std::optional<std::string> gen_type, std::optional<double> min, std::optional<double> max);

    void parse_type();

    double get_min_value() const noexcept;
    
    double get_max_value() const noexcept;

    // Check if the column type is variable length
    bool is_var_length() const noexcept;

    size_t get_fixed_type_size() const;

    // Convert to TDengine internal type
    int get_taos_type() const noexcept;
};

using ColumnConfigVector = std::vector<ColumnConfig>;