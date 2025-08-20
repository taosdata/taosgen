#pragma once

#include <string>
#include <vector>
#include "ColumnConfig.hpp"
#include "TimestampOriginalConfig.hpp"
#include "TimestampGeneratorConfig.hpp"

struct ColumnsConfig {
    std::string source_type; // Data source type: generator or csv

    struct Generator {
        ColumnConfigVector schema; // Schema definition for normal columns

        struct TimestampStrategy {
            TimestampGeneratorConfig timestamp_config;
        } timestamp_strategy;
    } generator;

    struct CSV {
        ColumnConfigVector schema; // Schema definition for normal columns

        std::string file_path;
        bool has_header = true;
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
    } csv;

    const ColumnConfigVector& get_schema() const {
        if (source_type == "generator") {
            return generator.schema;
        } else if (source_type == "csv") {
            return csv.schema;
        } else {
            throw std::runtime_error("Unknown source_type: " + source_type);
        }
    }
};