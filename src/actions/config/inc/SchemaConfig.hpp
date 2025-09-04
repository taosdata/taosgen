#pragma once

#include "TableNameConfig.hpp"
#include "ColumnConfig.hpp"
#include "GenerationConfig.hpp"

struct SchemaConfig {
    std::string name;
    TableNameConfig tbname;
    ColumnConfigVector columns;
    ColumnConfigVector tags;
    GenerationConfig generation;
};
