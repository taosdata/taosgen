#pragma once
#include "IFormatter.h"
#include "FormatterFactory.h"


class SqlDatabaseFormatter final : public IDatabaseFormatter {
public:
    explicit SqlDatabaseFormatter(const DataFormat& format) : format_(format) {}

    FormatResult format(const CreateDatabaseConfig& config, bool is_drop) const override {
        if (is_drop) {
            return "DROP DATABASE IF EXISTS `" + config.database_info.name + "`";
        } else {
            std::string result = "CREATE DATABASE IF NOT EXISTS `" + config.database_info.name + "`";
            if (config.database_info.properties.has_value()) {
                result += " " + config.database_info.properties.value();
            }
            return result;
        }
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