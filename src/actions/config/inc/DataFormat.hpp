#pragma once

#include <string>

struct DataFormat {
    std::string format_type = "sql";

    struct StmtConfig {
        std::string version = "v2"; // "v1" or "v2"

        StmtConfig() = default;
        StmtConfig(const std::string& v) : version(v) {}
    } stmt_config;

    struct SchemalessConfig {
        std::string protocol = "line"; // "line", "telnet", "json", or "taos-json"

        SchemalessConfig() = default;
        SchemalessConfig(const std::string& p) : protocol(p) {}
    } schemaless_config;

    struct CSVConfig {
        std::string delimiter = ",";          // Default delimiter is a comma
        std::string quote_character = "\"";   // Default quote character
        std::string escape_character = "\\"; // Default escape character

        CSVConfig() = default;
        CSVConfig(const std::string& d, const std::string& q, const std::string& e)
            : delimiter(d), quote_character(q), escape_character(e) {}
    } csv_config;

    DataFormat() = default;
    DataFormat(const std::string& type) : format_type(type) {}
    DataFormat(const std::string& type, const StmtConfig& stmt) : format_type(type), stmt_config(stmt) {}
    DataFormat(const std::string& type, const SchemalessConfig& schemaless) : format_type(type), schemaless_config(schemaless) {}
    DataFormat(const std::string& type, const CSVConfig& csv) : format_type(type), csv_config(csv) {}
};
