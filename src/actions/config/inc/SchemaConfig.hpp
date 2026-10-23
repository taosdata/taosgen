#pragma once

/*
 * Copyright (c) 2025 TAOS Data, Inc.
 *
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#include "LogUtils.hpp"
#include "TableNameConfig.hpp"
#include "ColumnConfig.hpp"
#include "GenerationConfig.hpp"
#include "FromCSVConfig.hpp"
#include "ColumnsConfig.hpp"
#include "TagsConfig.hpp"
#include <optional>

struct SchemaConfig {
    bool enabled = false;
    std::string name = "meters";
    FromCSVConfig from_csv;
    TableNameConfig tbname;
    ColumnConfigVector columns;
    ColumnConfigVector tags;
    GenerationConfig generation;

    ColumnsConfig columns_cfg;
    TagsConfig tags_cfg;

    void apply() {
        apply(std::nullopt);
    }

    void apply(const std::string& default_timestamp_precision) {
        apply(std::optional<std::string>(default_timestamp_precision));
    }

    void apply(const std::optional<std::string>& default_timestamp_precision) {
        if (columns.empty() || !ColumnTypeTraits::is_timestamp(columns[0].type_tag)) {
            columns.insert(columns.begin(), ColumnConfig("ts", "TIMESTAMP"));
        }

        if (default_timestamp_precision.has_value()) {
            for (auto& column : columns) {
                column.apply_timestamp_precision_default(default_timestamp_precision.value());
            }
        }

        if (from_csv.enabled && from_csv.tags.enabled && from_csv.tags.tbname_index >= 0) {
            tbname.enabled = true;
            tbname.source_type = "csv";
            tbname.csv.file_path = from_csv.tags.file_path;
            tbname.csv.has_header = from_csv.tags.has_header;
            tbname.csv.delimiter = from_csv.tags.delimiter;
            tbname.csv.tbname_index = from_csv.tags.tbname_index;
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

            if (from_csv.columns.timestamp_strategy.csv.enabled) {
                if(!from_csv.columns.timestamp_strategy.csv.timestamp_precision_configured) {
                    from_csv.columns.timestamp_strategy.csv.timestamp_precision = columns[0].ts.generator.timestamp_precision;
                    if (from_csv.columns.timestamp_strategy.csv.offset_config.has_value()) {
                        from_csv.columns.timestamp_strategy.csv.offset_config->parse_offset(from_csv.columns.timestamp_strategy.csv.timestamp_precision.value());
                    }
                }
                columns_cfg.csv.timestamp_strategy = from_csv.columns.timestamp_strategy;
            } else {
                columns_cfg.csv.timestamp_strategy = columns[0].ts;
            }
        } else {
            columns_cfg.source_type = "generator";
            columns_cfg.generator.schema = ColumnConfigVector(columns.begin() + 1, columns.end());
            columns_cfg.generator.timestamp_strategy.timestamp_config = columns[0].ts.generator;
        }

        if (generation.data_cache.enabled) {
            if (from_csv.columns.enabled) {
                if (from_csv.columns.tbname_index >= 0) {
                    LogUtils::warn("Configuration 'data_cache.enabled' is set to 'false' because 'from_csv.columns.tbname_index' in effect");
                    generation.data_cache.enabled = false;
                }

                if (generation.data_cache.enabled && !generation.tables_reuse_data) {
                    LogUtils::warn("Configuration 'data_cache.enabled' is set to 'false' because 'tables_reuse_data' is false");
                    generation.data_cache.enabled = false;
                }
            }
        }

        if (generation.tables_reuse_data) {
            if (from_csv.columns.enabled) {
                if (from_csv.columns.tbname_index >= 0) {
                    LogUtils::warn("Configuration 'tables_reuse_data' is set to 'false' because 'from_csv.columns.tbname_index' in effect");
                    generation.tables_reuse_data = false;
                }
            }
        }

        // Streaming mode validations
        if (from_csv.columns.enabled && from_csv.columns.loading_mode == "streaming") {
            if (from_csv.columns.tbname_index >= 0) {
                throw std::runtime_error("loading_mode 'streaming' is incompatible with 'tbname_index' >= 0. "
                    "Streaming mode requires tbname_index = -1.");
            }
            if (generation.data_disorder.enabled) {
                throw std::runtime_error("loading_mode 'streaming' is incompatible with 'data_disorder'. "
                    "Disorder requires buffering rows, which conflicts with streaming.");
            }
        }
    }
};
