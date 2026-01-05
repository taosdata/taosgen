#pragma once

#include "IFormatter.hpp"
#include "FormatterFactory.hpp"
#include "StmtV2InsertData.hpp"
#include "taos.h"
#include <sstream>
#include <limits>

class StmtInsertDataFormatter final : public IInsertDataFormatter {
public:
    explicit StmtInsertDataFormatter(const DataFormat& format) : format_(format) {}

    std::string prepare(const InsertDataConfig& config,
                        const ColumnConfigInstanceVector& col_instances,
                        const ColumnConfigInstanceVector& tag_instances) override {
        // TODO:
        // 1. native            : INSERT INTO ? VALUES(?,cols-qmark)
        // 2. websocket  -stb   : INSERT INTO `db_name`.`stb_name`(tbname,ts,cols-name) VALUES(?,?,col-qmark)
        //               -ntb   : INSERT INTO `db_name`.`stb_name`(ts,cols-name) VALUES(?,cols-qmark)
        // 3. auto create table : INSERT INTO ? USING `db_name`.`stb_name` TAGS (tags-qmark) VALUES(?,cols-qmark)

        const auto* tc = get_plugin_config<TDengineConfig>(config.extensions, "tdengine");
        if (tc == nullptr) {
            throw std::runtime_error("TDengine configuration not found in insert extensions");
        }

        if (format_.stmt.auto_create_table) {
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
                result << "," << col_instances[i].name();
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

        return result.str();
    }

    FormatResult format(const InsertDataConfig& config,
                        const ColumnConfigInstanceVector& col_instances,
                        const ColumnConfigInstanceVector& tag_instances,
                        MemoryPool::MemoryBlock* batch,
                        bool is_checkpoint_recover = false) const override {
        (void)config;

        if (format_.stmt.version != "v2") {
            throw std::invalid_argument("Unsupported stmt version: " + format_.stmt.version);
        }

        auto payload = std::make_unique<StmtV2InsertData>(batch, col_instances, tag_instances, is_checkpoint_recover);
        return FormatResult(std::move(payload));
    }

private:
    const DataFormat& format_;

    inline static bool registered_ = []() {
        FormatterFactory::register_formatter<InsertDataConfig>(
            "stmt",
            [](const DataFormat& format) {
                return std::make_unique<StmtInsertDataFormatter>(format);
            });
        return true;
    }();
};