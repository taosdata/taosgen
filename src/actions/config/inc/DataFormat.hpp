#pragma once

#include <string>

struct DataFormat {
    std::string format_type = "sql";

    struct StmtConfig {
        std::string version = "v2"; // "v1" or "v2"

        StmtConfig() = default;
        StmtConfig(const std::string& v) : version(v) {}
    } stmt;

    struct SchemalessConfig {
        std::string protocol = "line"; // "line", "telnet", "json", or "taos-json"

        SchemalessConfig() = default;
        SchemalessConfig(const std::string& p) : protocol(p) {}
    } schemaless;

    struct CSVConfig {
        std::string delimiter = ",";          // Default delimiter is a comma
        std::string quote_character = "\"";   // Default quote character
        std::string escape_character = "\\"; // Default escape character

        CSVConfig() = default;
        CSVConfig(const std::string& d, const std::string& q, const std::string& e)
            : delimiter(d), quote_character(q), escape_character(e) {}
    } csv;

    struct MqttConfig {
        std::string topic = "tsbench/{table}";
        std::string compression = "";
        std::string encoding = "UTF-8";
        std::string content_type = "json";
        std::string tbname_key = "table";

        size_t qos = 0;
        bool retain = false;
        size_t records_per_message = 1;

        MqttConfig() = default;
        MqttConfig(const std::string& ct) : content_type(ct) {}
    } mqtt;

    struct KafkaConfig {
        std::string key_pattern = "{table}";
        std::string key_serializer = "string-utf8";
        std::string value_serializer = "json";
        std::string tbname_key = "table";
        std::string acks = "0";
        std::string compression = "none";
        size_t records_per_message = 1;

        KafkaConfig() = default;
        KafkaConfig(const std::string& kp,
                    const std::string& ks,
                    const std::string& vs,
                    const std::string& a,
                    const std::string& c,
                    size_t rpm)
            : key_pattern(kp),
              key_serializer(ks),
              value_serializer(vs),
              acks(a),
              compression(c),
              records_per_message(rpm) {}
    } kafka;

    DataFormat() = default;
    DataFormat(const std::string& type) : format_type(type) {}
    DataFormat(const std::string& type, const StmtConfig& stmt_) : format_type(type), stmt(stmt_) {}
    DataFormat(const std::string& type, const SchemalessConfig& schemaless_) : format_type(type), schemaless(schemaless_) {}
    DataFormat(const std::string& type, const CSVConfig& csv_) : format_type(type), csv(csv_) {}
};
