#pragma once
#include <sstream>
#include <limits> 
#include "taos.h"
#include "IFormatter.h"
#include "FormatterFactory.h"


class StmtInsertDataFormatter final : public IInsertDataFormatter {
public:
    explicit StmtInsertDataFormatter(const DataFormat& format) : format_(format) {}

    std::string prepare(const InsertDataConfig& config,
                        const ColumnConfigInstanceVector& col_instances,
                        int mode = 1) const override {
        // TODO:
        // 1. native            : INSERT INTO ? VALUES(?,cols-qmark)
        // 2. websocket  -stb   : INSERT INTO `db_name`.`stb_name`(cols-name) VALUES(?,?,col-qmark)
        //               -ntb   : INSERT INTO `db_name`.`stb_name`(cols-name) VALUES(?,cols-qmark)
        // 3. auto create table : INSERT INTO ? USING `db_name`.`stb_name` TAGS (tags-qmark) VALUES(?,cols-qmark)

        std::ostringstream result;

        if (mode == 1) {
            result << "INSERT INTO ? VALUES(?";

            // Add question marks for each column
            for (size_t i = 0; i < col_instances.size(); i++) {
                result << ",?";
            }
            result << ")";
        }
        else if (mode == 2)
        {
            result << "INSERT INTO `"
                << config.target.tdengine.database_info.name << "`.`" 
                << config.target.tdengine.super_table_info.name << "`(tbname,ts";

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
        
        return result.str();
    }

    FormatResult format(const InsertDataConfig& config, 
                        const ColumnConfigInstanceVector& col_instances, 
                        MemoryPool::MemoryBlock* batch) const override {
        (void)config;

        if (format_.stmt_config.version != "v2") {
            throw std::invalid_argument("Unsupported stmt version: " + format_.stmt_config.version);
        }

        return StmtV2InsertData(batch, col_instances);
    }

private:
    const DataFormat& format_;

    inline static bool registered_ = []() {
        FormatterFactory::instance().register_formatter<InsertDataConfig>(
            "stmt",
            [](const DataFormat& format) {
                return std::make_unique<StmtInsertDataFormatter>(format);
            });
        return true;
    }();
};