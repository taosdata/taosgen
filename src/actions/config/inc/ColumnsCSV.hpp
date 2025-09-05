#pragma once

#include "ColumnConfig.hpp"
#include "TimestampOriginalConfig.hpp"
#include "TimestampGeneratorConfig.hpp"
#include <string>

struct ColumnsCSV {
    bool enabled = false;
    ColumnConfigVector schema;

    std::string file_path;
    bool has_header = true;
    bool repeat_read = false;
    std::string delimiter = ",";
    int tbname_index = -1;

    struct TimestampStrategy {
        std::string strategy_type = "original"; // Timestamp strategy type: original or generator
        std::variant<TimestampOriginalConfig, TimestampGeneratorConfig> timestamp_config;

        std::string get_precision() const {
            if (strategy_type == "original") {
                return std::get<TimestampOriginalConfig>(timestamp_config).timestamp_precision;
            } else if (strategy_type == "generator") {
                return std::get<TimestampGeneratorConfig>(timestamp_config).timestamp_precision;
            }
            throw std::runtime_error("Invalid timestamp strategy type: " + strategy_type);
        }
    } timestamp_strategy;
};