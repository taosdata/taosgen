#pragma once
#include "IFormatter.h"
#include "FormatterFactory.h"


class SqlDatabaseFormatter final : public IDatabaseFormatter {
public:
    explicit SqlDatabaseFormatter(const DataFormat& format) : format_(format) {}

    FormatResult format(const CreateDatabaseConfig& config) const override {
        std::vector<std::string> stmts;
        if (config.database_info.drop_if_exists) {
            stmts.push_back("DROP DATABASE IF EXISTS `" + config.database_info.name + "`");
        }
        std::string create_stmt = "CREATE DATABASE IF NOT EXISTS `" + config.database_info.name + "`";
        if (config.database_info.properties.has_value()) {
            create_stmt += " " + config.database_info.properties.value();
        }
        stmts.push_back(std::move(create_stmt));
        return stmts;
    }


private:
    const DataFormat& format_;

    // Register SqlDatabaseFormatter to FormatterFactory
    inline static bool registered_ = []() {
        FormatterFactory::instance().register_formatter<CreateDatabaseConfig>(
            "sql",
            [](const DataFormat& format) {
                return std::make_unique<SqlDatabaseFormatter>(format);
            });
        return true;
    }();
};