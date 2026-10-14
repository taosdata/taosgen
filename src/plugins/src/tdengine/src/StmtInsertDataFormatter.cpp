/*
 * Copyright (c) 2026 TAOS Data, Inc.
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

#include "StmtInsertDataFormatter.hpp"
#include "StmtContext.hpp"
#include "taos.h"
#include <sstream>
#include <limits>


StmtInsertDataFormatter::StmtInsertDataFormatter(const DataFormat& format) : format_(format) {
    format_options_ = get_format_opt<StmtFormatOptions>(format_, "stmt");
    if (!format_options_) {
        throw std::runtime_error("STMT formatter options not found in DataFormat");
    }
}

std::unique_ptr<ISinkContext> StmtInsertDataFormatter::init(const InsertDataConfig& config,
                                                            const ColumnConfigInstanceVector& col_instances,
                                                            const ColumnConfigInstanceVector& tag_instances) {
    // TODO:
    // 1. native            : INSERT INTO ? VALUES(?,cols-qmark)
    // 2. websocket  -stb   : INSERT INTO `db_name`.`stb_name`(tbname,ts,cols-name) VALUES(?,?,col-qmark)
    //               -ntb   : INSERT INTO `db_name`.`stb_name`(ts,cols-name) VALUES(?,cols-qmark)
    // 3. auto create table : INSERT INTO ? USING `db_name`.`stb_name` TAGS (tags-qmark) VALUES(?,cols-qmark)

    IInsertDataFormatter::init(config, col_instances, tag_instances);
    const auto* tc = get_plugin_config<TDengineConfig>(config.extensions, "tdengine");
    if (tc == nullptr) {
        throw std::runtime_error("TDengine configuration not found in insert extensions");
    }

    if (format_options_->auto_create_table) {
        mode_ = InsertMode::AutoCreateTable;
    } else if (tc->protocol_type == TDengineConfig::ProtocolType::WebSocket) {
        mode_ = InsertMode::SuperTable;
    } else {
        mode_ = InsertMode::SubTable;
    }

    std::ostringstream result;

    if (mode_ == InsertMode::SubTable) {
        result << "INSERT INTO ? VALUES(?";

        // Add question marks for each column
        for (size_t i = 0; i < col_instances.size(); i++) {
            result << ",?";
        }
        result << ")";
    }
    else if (mode_ == InsertMode::SuperTable) {
        result << "INSERT INTO `"
            << config.schema.name << "`(tbname,ts";

        // Add column names
        for (size_t i = 0; i < col_instances.size(); i++) {
            result << ",`" << col_instances[i].name() << "`";
        }

        result << ") VALUES(?,?";

        // Add question marks for each column
        for (size_t i = 0; i < col_instances.size(); i++) {
            result << ",?";
        }
        result << ")";
    }
    else if (mode_ == InsertMode::AutoCreateTable) {
        result << "INSERT INTO ? USING `"
            << config.schema.name << "` TAGS (";

        // Add question marks for each tag column
        size_t tag_count = tag_instances.size();
        for (size_t i = 0; i < tag_count; i++) {
            result << "?";
            if (i < tag_count - 1) {
                result << ",";
            }
        }

        result << ") VALUES(?";

        // Add question marks for each column
        for (size_t i = 0; i < col_instances.size(); i++) {
            result << ",?";
        }
        result << ")";
    }

    return std::make_unique<StmtContext>(result.str());
}

FormatResult StmtInsertDataFormatter::format(MemoryPool::MemoryBlock* batch,
                                             bool is_checkpoint_recover) const {
    if (format_options_->version != "v2") {
        throw std::invalid_argument("Unsupported stmt version: " + format_options_->version);
    }

    StmtV2InsertData data(batch, is_checkpoint_recover);
    auto payload = BaseInsertData::make_with_payload(batch, *col_instances_, *tag_instances_, std::move(data));

    return FormatResult(std::move(payload));
}
