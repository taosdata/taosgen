#pragma once

#include "TableNameConfig.hpp"
#include "ColumnConfig.hpp"
#include "GenerationConfig.hpp"
#include "FromCSVConfig.hpp"
#include "ColumnsConfig.hpp"
#include "TagsConfig.hpp"

struct SchemaConfig {
    std::string name;
    FromCSVConfig from_csv;
    TableNameConfig tbname;
    ColumnConfigVector columns;
    ColumnConfigVector tags;
    GenerationConfig generation;

    ColumnsConfig columns_cfg;
    TagsConfig tags_cfg;


    void apply() {
        if (from_csv.tags.enabled) {
            tags_cfg.source_type = "csv";
            tags_cfg.csv = from_csv.tags;
            tags_cfg.csv.schema = tags;
        } else {
            tags_cfg.source_type = "generator";
            tags_cfg.generator.schema = tags;
        }

        if (from_csv.columns.enabled) {
            columns_cfg.source_type = "csv";
            columns_cfg.csv = from_csv.columns;
            columns_cfg.csv.schema = columns;
        } else {
            columns_cfg.source_type = "generator";
            columns_cfg.generator.schema = columns;
        }
    }
};
