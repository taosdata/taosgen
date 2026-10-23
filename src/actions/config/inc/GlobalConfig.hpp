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
#include "SchemaConfig.hpp"
#include "DatabaseInfo.hpp"
#include "SuperTableInfo.hpp"
#include "DataFormat.hpp"
#include "DataChannel.hpp"
#include "PluginExtensions.hpp"

struct GlobalConfig {
    bool confirm_prompt = false;
    bool verbose = false;
    std::string log_dir = "log/";
    std::string log_file = "";
    std::string cfg_dir = "/etc/taos/";
    std::string yaml_cfg_dir = "./";
    // TDengineConfig connection_info;
    DataFormat data_format;
    DataChannel data_channel;
    DatabaseInfo database_info;
    SuperTableInfo super_table_info;

    SchemaConfig schema;
    PluginExtensions extensions;
};
