#pragma once
#include <sstream>
#include <limits> 
#include "taos.h"
#include "IFormatter.h"
#include "FormatterFactory.h"


class StmtInsertDataFormatter final : public IInsertDataFormatter {
public:
    explicit StmtInsertDataFormatter(const DataFormat& format) : format_(format) {}

    std::string prepare(const InsertDataConfig& config, const ColumnConfigInstanceVector& col_instances) const {
        // TODO:
        // 1. native            : INSERT INTO ? VALUES(?,cols-qmark)
        // 2. websocket  -stb   : INSERT INTO `db_name`.`stb_name`(cols-name) VALUES(?,?,col-qmark)
        //               -ntb   : INSERT INTO `db_name`.`stb_name`(cols-name) VALUES(?,cols-qmark)
        // 3. auto create table : INSERT INTO ? USING `db_name`.`stb_name` TAGS (tags-qmark) VALUES(?,cols-qmark)

        std::ostringstream result;

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

        return result.str();
    }


    FormatResult format(const InsertDataConfig& config, 
                        const ColumnConfigInstanceVector& col_instances, 
                        MultiBatch&& batch) const {

        if (batch.table_batches.empty()) {
            return FormatResult("");
        } else {
            if (format_.stmt_config.version != "v2") {
                throw std::invalid_argument("Unsupported stmt version: " + format_.stmt_config.version);
            }

            return StmtV2InsertData(
                    batch.start_time,
                    batch.end_time,
                    batch.total_rows,
                    col_instances,
                    std::move(batch)
                );
        }
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