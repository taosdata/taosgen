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

#include <string>
#include <vector>
#include "TDengineConfig.hpp"
#include "DataFormat.hpp"
#include "DataChannel.hpp"

struct QueryDataConfig {
    struct Source {
        TDengineConfig connection_info;
    } source;

    struct Control {
        DataFormat data_format;
        DataChannel data_channel;

        struct QueryControl {
            std::string log_path = "result.txt";
            bool enable_dryrun = false;

            struct Execution {
                std::string mode = "sequential_per_thread";
                int threads = 1;
                int times = 1;
                int interval = 0;
            } execution;

            std::string query_type;

            struct FixedQuery {
                std::string sql;
                std::string output_file;
            };
            struct SuperTableQueryTemplate {
                std::string sql_template;
                std::string output_file;
            };

            struct Fixed {
                std::vector<FixedQuery> queries;
            } fixed;

            struct SuperTable {
                std::string database_name;
                std::string super_table_name;
                std::string placeholder;
                std::vector<SuperTableQueryTemplate> templates;
            } super_table;
        } query_control;
    } control;
};
