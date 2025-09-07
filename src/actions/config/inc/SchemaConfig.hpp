#pragma once

#include "TableNameConfig.hpp"
#include "ColumnConfig.hpp"
#include "GenerationConfig.hpp"
#include "FromCSVConfig.hpp"
#include "ColumnsConfig.hpp"
#include "TagsConfig.hpp"

struct SchemaConfig {
    bool enabled = false;
    std::string name;
    FromCSVConfig from_csv;
    TableNameConfig tbname;
    ColumnConfigVector columns;
    ColumnConfigVector tags;
    GenerationConfig generation;

    ColumnsConfig columns_cfg;
    TagsConfig tags_cfg;

    void apply() {
        if (columns.empty() || columns[0].type_tag != ColumnTypeTag::BIGINT) {
            columns.insert(columns.begin(), ColumnConfig("ts", "TIMESTAMP"));
        }

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
            columns_cfg.csv.schema = ColumnConfigVector(columns.begin() + 1, columns.end());

            if (columns_cfg.csv.timestamp_index >= 0) {
                columns_cfg.csv.timestamp_strategy.strategy_type = "csv";
                columns_cfg.csv.timestamp_strategy.csv.timestamp_index = columns_cfg.csv.timestamp_index;
            } else {
                columns_cfg.csv.timestamp_strategy.strategy_type = "generator";
                columns_cfg.csv.timestamp_strategy.generator = columns[0].ts.generator;
            }
        } else {
            columns_cfg.source_type = "generator";
            columns_cfg.generator.schema = ColumnConfigVector(columns.begin() + 1, columns.end());
            columns_cfg.csv.timestamp_strategy.strategy_type = "generator";
            columns_cfg.csv.timestamp_strategy.generator = columns[0].ts.generator;
        }
    }
};
