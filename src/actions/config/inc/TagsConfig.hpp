#pragma once

#include <vector>
#include <string>
#include <sstream>
#include <algorithm>
#include "ColumnConfig.hpp"
#include "TagsCSV.hpp"

struct TagsConfig {
    std::string source_type = "generator";          // Data source type: generator or csv

    struct Generator {
        ColumnConfigVector schema;
    } generator;

    TagsCSV csv;

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