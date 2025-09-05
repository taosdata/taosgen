#pragma once

#include "TagsCSV.hpp"
#include "ColumnsCSV.hpp"

struct FromCSVConfig {
    bool enabled = false;
    TagsCSV tags;
    ColumnsCSV columns;
};
