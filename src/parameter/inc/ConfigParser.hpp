#pragma once

#include "GlobalConfig.hpp"
#include "ActionConfigVariant.hpp"

#include <string>
#include <vector>
#include <optional>

#include <variant>
#include <yaml-cpp/yaml.h>


namespace YAML {

    template<typename T>
    std::set<T> merge_keys(const std::initializer_list<std::set<T>>& sets) {
        std::set<T> result;
        for (const auto& s : sets) {
            result.insert(s.begin(), s.end());
        }
        return result;
    }

    inline void check_unknown_keys(const YAML::Node& node, const std::set<std::string>& valid_keys, const std::string& context) {
        for (auto it = node.begin(); it != node.end(); ++it) {
            std::string key = it->first.as<std::string>();
            if (valid_keys.find(key) == valid_keys.end()) {
                throw std::runtime_error("Unknown configuration key in " + context + ": " + key);
            }
        }
    }

    inline void check_forbidden_keys(const YAML::Node& node, const std::set<std::string>& forbidden_keys, const std::string& context) {
        for (auto it = node.begin(); it != node.end(); ++it) {
            std::string key = it->first.as<std::string>();
            if (forbidden_keys.find(key) != forbidden_keys.end()) {
                throw std::runtime_error("Forbidden configuration key in " + context + ": " + key);
            }
        }
    }

    template<>
    struct convert<ConnectionInfo::ConnectionPoolConfig> {
        static bool decode(const Node& node, ConnectionInfo::ConnectionPoolConfig& rhs) {
            // Detect unknown configuration keys
            static const std::set<std::string> valid_keys = {
                "enabled", "max_size", "min_size", "connection_timeout"
            };
            check_unknown_keys(node, valid_keys, "connection_info::pool");

            if (node["enabled"]) {
                rhs.enabled = node["enabled"].as<bool>();
            }

            if (rhs.enabled) {
                if (node["max_size"]) {
                    rhs.max_size = node["max_size"].as<size_t>();
                    if (rhs.max_size == 0) {
                        throw std::runtime_error("max_size must be greater than 0");
                    }
                }
                if (node["min_size"]) {
                    rhs.min_size = node["min_size"].as<size_t>();
                }
                if (rhs.min_size > rhs.max_size) {
                    throw std::runtime_error("min_size cannot exceed max_size");
                }
                if (node["connection_timeout"]) {
                    rhs.connection_timeout = node["connection_timeout"].as<size_t>();
                }
            }

            return true;
        }
    };

    template<>
    struct convert<ConnectionInfo> {
        static bool decode(const Node& node, ConnectionInfo& rhs) {
            // Detect unknown configuration keys
            static const std::set<std::string> valid_keys = {
                "host", "port", "user", "password", "dsn", "pool"
            };
            check_unknown_keys(node, valid_keys, "connection_info");

            if (node["host"]) {
                rhs.host = node["host"].as<std::string>();
            }
            if (node["port"]) {
                rhs.port = node["port"].as<int>();
            }
            if (node["user"]) {
                rhs.user = node["user"].as<std::string>();
            }
            if (node["password"]) {
                rhs.password = node["password"].as<std::string>();
            }
            if (node["dsn"]) {
                rhs.dsn = node["dsn"].as<std::string>();
                rhs.parse_dsn(*rhs.dsn);
            }
            if (node["pool"]) {
                rhs.pool = node["pool"].as<ConnectionInfo::ConnectionPoolConfig>();
            }

            return true;
        }
    };

    template<>
    struct convert<MqttInfo> {
        static bool decode(const Node& node, MqttInfo& rhs) {
            // Detect unknown configuration keys
            static const std::set<std::string> valid_keys = {
                "host", "port", "user", "password", "client_id", "topic", "compression", "encoding",
                "timestamp_precision", "qos", "keep_alive", "clean_session", "retain",
                "max_buffered_messages", "batch_messages"
            };
            check_unknown_keys(node, valid_keys, "mqtt_info");

            if (node["host"]) {
                rhs.host = node["host"].as<std::string>();
            }
            if (node["port"]) {
                rhs.port = node["port"].as<int>();
            }
            if (node["user"]) {
                rhs.user = node["user"].as<std::string>();
            }
            if (node["password"]) {
                rhs.password = node["password"].as<std::string>();
            }
            if (node["topic"]) {
                rhs.topic = node["topic"].as<std::string>();
            } else {
                throw std::runtime_error("Missing required field 'topic' in mqtt_info.");
            }

            if (node["client_id"]) {
                rhs.client_id = node["client_id"].as<std::string>();
            }
            if (node["compression"]) {
                rhs.compression = node["compression"].as<std::string>();
            }
            if (node["encoding"]) {
                rhs.encoding = node["encoding"].as<std::string>();
            }
            if (node["timestamp_precision"]) {
                rhs.timestamp_precision = node["timestamp_precision"].as<std::string>();
                // Validate time precision value
                if (rhs.timestamp_precision != "ms" && rhs.timestamp_precision != "us" && rhs.timestamp_precision != "ns") {
                    throw std::runtime_error("Invalid timestamp precision value: " + rhs.timestamp_precision + " in mqtt_info.");
                }
            }
            if (node["qos"]) {
                rhs.qos = node["qos"].as<size_t>();
                if (rhs.qos > 2) {
                    throw std::runtime_error("Invalid QoS value: " + std::to_string(rhs.qos));
                }
            }
            if (node["keep_alive"]) {
                rhs.keep_alive = node["keep_alive"].as<size_t>();
            }
            if (node["clean_session"]) {
                rhs.clean_session = node["clean_session"].as<bool>();
            }
            if (node["retain"]) {
                rhs.retain = node["retain"].as<bool>();
            }
            if (node["max_buffered_messages"]) {
                rhs.max_buffered_messages = node["max_buffered_messages"].as<size_t>();
                if (rhs.max_buffered_messages == 0) {
                    throw std::runtime_error("max_buffered_messages must be greater than 0 in mqtt_info.");
                }
            }
            if (node["batch_messages"]) {
                rhs.batch_messages = node["batch_messages"].as<size_t>();
                if (rhs.batch_messages == 0) {
                    throw std::runtime_error("batch_messages must be greater than 0 in mqtt_info.");
                }
            }

            if (rhs.batch_messages > rhs.max_buffered_messages) {
                throw std::runtime_error("batch_messages cannot be greater than max_buffered_messages in mqtt_info.");
            }
            return true;
        }
    };

    template<>
    struct convert<DatabaseInfo> {
        static bool decode(const Node& node, DatabaseInfo& rhs) {
            // Detect unknown configuration keys
            static const std::set<std::string> valid_keys = {"name", "drop_if_exists", "precision", "properties"};
            check_unknown_keys(node, valid_keys, "database_info");

            if (!node["name"]) {
                throw std::runtime_error("Missing required field 'name' in database_info.");
            }
            rhs.name = node["name"].as<std::string>();
            if (node["drop_if_exists"]) {
                rhs.drop_if_exists = node["drop_if_exists"].as<bool>();
            }
            if (node["precision"]) {
                rhs.precision = node["precision"].as<std::string>();
                // Validate time precision value
                if (rhs.precision != "ms" && rhs.precision != "us" && rhs.precision != "ns") {
                    throw std::runtime_error("Invalid precision value: " + rhs.precision);
                }
            }
            if (node["properties"]) {
                rhs.properties = node["properties"].as<std::string>();
            }

            return true;
        }
    };


    template<>
    struct convert<ColumnConfig> {
        static bool decode(const Node& node, ColumnConfig& rhs) {
            // Detect unknown configuration keys
            static const std::set<std::string> common_keys = {
                "name", "type", "primary_key", "count", "properties", "gen_type", "null_ratio", "none_ratio"
            };
            static const std::set<std::string> random_allowed = {
                "distribution", "min", "max", "dec_min", "dec_max", "corpus", "chinese", "values"
            };
            static const std::set<std::string> order_allowed = {
                "min", "max"
            };
            static const std::set<std::string> expression_allowed = {
                "formula"
            };

            static const std::set<std::string> valid_keys = merge_keys<std::string>({
                common_keys, random_allowed, order_allowed, expression_allowed
            });
            check_unknown_keys(node, valid_keys, "columns or tags");

            if (!node["name"]) {
                throw std::runtime_error("Missing required field 'name' for columns or tags.");
            }
            if (!node["type"]) {
                throw std::runtime_error("Missing required field 'type' for columns or tags.");
            }

            rhs.name = node["name"].as<std::string>();
            rhs.type = node["type"].as<std::string>();
            rhs.parse_type();

            if (node["primary_key"]) rhs.primary_key = node["primary_key"].as<bool>();
            // if (node["len"]) rhs.len = node["len"].as<int>();
            if (node["count"]) rhs.count = node["count"].as<size_t>();
            // if (node["precision"]) rhs.precision = node["precision"].as<int>();
            // if (node["scale"]) rhs.scale = node["scale"].as<int>();
            if (node["properties"]) rhs.properties = node["properties"].as<std::string>();
            if (node["null_ratio"]) rhs.null_ratio = node["null_ratio"].as<float>();
            if (node["none_ratio"]) rhs.none_ratio = node["none_ratio"].as<float>();
            if (!node["gen_type"]) {
                rhs.gen_type = "random";
            } else {
                rhs.gen_type = node["gen_type"].as<std::string>();
            }

            if (*rhs.gen_type == "random") {
                // Detect forbidden keys in random
                check_unknown_keys(node, merge_keys<std::string>({common_keys, random_allowed}), "columns or tags::random");

                if (node["distribution"]) {
                    rhs.distribution = node["distribution"].as<std::string>();
                } else {
                    rhs.distribution = "uniform";
                }
                if (node["min"]) {
                    rhs.min = node["min"].as<double>();
                } else {
                    rhs.min = rhs.get_min_value();
                }
                if (node["max"]) {
                    rhs.max = node["max"].as<double>();
                } else {
                    rhs.max = rhs.get_max_value();
                }
                if (node["dec_min"]) rhs.dec_min = node["dec_min"].as<std::string>();
                if (node["dec_max"]) rhs.dec_max = node["dec_max"].as<std::string>();
                if (node["corpus"]) rhs.corpus = node["corpus"].as<std::string>();
                if (node["chinese"]) rhs.chinese = node["chinese"].as<bool>();
                if (node["values"]) {
                    if (rhs.type_tag == ColumnTypeTag::BOOL) {
                        auto str_values = node["values"].as<std::vector<std::string>>();
                        rhs.set_values_from_strings(str_values);
                    } else if (rhs.is_var_length()) {
                        auto str_values = node["values"].as<std::vector<std::string>>();
                        rhs.set_values_from_strings(str_values);
                    } else {
                        auto dbl_values = node["values"].as<std::vector<double>>();
                        rhs.set_values_from_doubles(dbl_values);
                    }

                    if (rhs.values_count < 1) {
                        throw std::runtime_error("values must contain at least one element for column: " + rhs.name);
                    }
                }
            } else if (*rhs.gen_type == "order") {
                // Detect forbidden keys in order
                check_unknown_keys(node, merge_keys<std::string>({common_keys, order_allowed}), "columns or tags::order");

                if (node["min"]) rhs.order_min = node["min"].as<int64_t>();
                if (node["max"]) rhs.order_max = node["max"].as<int64_t>();
            } else if (*rhs.gen_type == "expression") {
                // Detect forbidden keys in expression
                check_unknown_keys(node, merge_keys<std::string>({common_keys, expression_allowed}), "columns or tags::expression");

                if (node["formula"]) {
                    rhs.formula = node["formula"].as<std::string>();
                } else {
                    throw std::runtime_error("Missing required 'formula' for expression type column: " + rhs.name);
                }
            } else {
                throw std::runtime_error("Invalid gen_type: " + *rhs.gen_type);
            }
            return true;
        }
    };


    template<>
    struct convert<TableNameConfig> {
        static bool decode(const Node& node, TableNameConfig& rhs) {
            // Detect unknown configuration keys at the top level
            static const std::set<std::string> valid_keys = {
                "source_type", "generator", "csv"
            };
            check_unknown_keys(node, valid_keys, "table_name");

            if (!node["source_type"]) {
                throw std::runtime_error("Missing required 'source_type' in table_name.");
            }
            rhs.source_type = node["source_type"].as<std::string>();

            if (rhs.source_type == "generator") {
                if (!node["generator"]) {
                    throw std::runtime_error("Missing required 'generator' configuration for source_type 'generator' in table_name.");
                }

                const auto& generator = node["generator"];

                // Detect unknown keys in generator
                static const std::set<std::string> generator_keys = {"prefix", "count", "from"};
                check_unknown_keys(generator, generator_keys, "table_name::generator");

                if (generator["prefix"]) {
                    rhs.generator.prefix = generator["prefix"].as<std::string>();
                }
                if (generator["count"]) {
                    rhs.generator.count = generator["count"].as<int>();
                }
                if (generator["from"]) {
                    rhs.generator.from = generator["from"].as<int>();
                }
            } else if (rhs.source_type == "csv") {
                if (!node["csv"]) {
                    throw std::runtime_error("Missing required 'csv' configuration for source_type 'csv' in table_name.");
                }

                const auto& csv = node["csv"];

                // Detect unknown keys in csv
                static const std::set<std::string> csv_keys = {"file_path", "has_header", "delimiter", "tbname_index"};
                check_unknown_keys(csv, csv_keys, "table_name::csv");

                if (csv["file_path"]) {
                    rhs.csv.file_path = csv["file_path"].as<std::string>();
                } else {
                    throw std::runtime_error("Missing required 'file_path' configuration for table_name::csv.");
                }
                if (csv["has_header"]) {
                    rhs.csv.has_header = csv["has_header"].as<bool>();
                }
                if (csv["delimiter"]) {
                    rhs.csv.delimiter = csv["delimiter"].as<std::string>();
                }
                if (csv["tbname_index"]) {
                    rhs.csv.tbname_index = csv["tbname_index"].as<int>();
                }
            } else {
                throw std::runtime_error("Invalid 'source_type' in table_name: " + rhs.source_type);
            }

            return true;
        }
    };


    template<>
    struct convert<TagsConfig> {
        static bool decode(const Node& node, TagsConfig& rhs) {
            // Detect unknown configuration keys at the top level
            static const std::set<std::string> valid_keys = {
                "source_type", "generator", "csv"
            };
            check_unknown_keys(node, valid_keys, "tags");

            if (!node["source_type"]) {
                throw std::runtime_error("Missing required field 'source_type' in tags.");
            }
            rhs.source_type = node["source_type"].as<std::string>();
            if (rhs.source_type == "generator") {
                if (!node["generator"]) {
                    throw std::runtime_error("Missing required 'generator' configuration for source_type 'generator' in tags.");
                }

                const auto& generator = node["generator"];

                // Detect unknown keys in generator
                static const std::set<std::string> generator_keys = {"schema"};
                check_unknown_keys(generator, generator_keys, "tags::generator");

                if (generator["schema"]) {
                    rhs.generator.schema = generator["schema"].as<ColumnConfigVector>();
                }
            } else if (rhs.source_type == "csv") {
                if (!node["csv"]) {
                    throw std::runtime_error("Missing required 'csv' configuration for source_type 'csv' in tags.");
                }

                const auto& csv = node["csv"];

                // Detect unknown keys in csv
                static const std::set<std::string> csv_keys = {"schema", "file_path", "has_header", "delimiter", "exclude_indices"};
                check_unknown_keys(csv, csv_keys, "tags::csv");

                if (csv["schema"]) {
                    rhs.csv.schema = csv["schema"].as<ColumnConfigVector>();
                }

                if (csv["file_path"]) {
                    rhs.csv.file_path = csv["file_path"].as<std::string>();
                } else {
                    throw std::runtime_error("Missing required 'file_path' configuration for tags::csv");
                }

                if (csv["has_header"]) rhs.csv.has_header = csv["has_header"].as<bool>();
                if (csv["delimiter"]) rhs.csv.delimiter = csv["delimiter"].as<std::string>();
                if (csv["exclude_indices"]) {
                    rhs.csv.exclude_indices_str = csv["exclude_indices"].as<std::string>();
                    rhs.csv.parse_exclude_indices();
                }
            } else {
                throw std::runtime_error("Invalid source_type for tags in child_table_info.");
            }
            return true;
        }
    };


    template<>
    struct convert<SuperTableInfo> {
        static bool decode(const Node& node, SuperTableInfo& rhs) {
            // Detect unknown configuration keys
            static const std::set<std::string> valid_keys = {"name", "columns", "tags"};
            check_unknown_keys(node, valid_keys, "super_table_info");

            if (!node["name"]) {
                throw std::runtime_error("Missing required field 'name' in super_table_info.");
            }
            rhs.name = node["name"].as<std::string>();
            if (node["columns"]) {
                for (const auto& item : node["columns"]) {
                    rhs.columns.push_back(item.as<ColumnConfig>());
                }
            }
            if (node["tags"]) {
                for (const auto& item : node["tags"]) {
                    rhs.tags.push_back(item.as<ColumnConfig>());
                }
            }
            return true;
        }
    };


    template<>
    struct convert<ChildTableInfo> {
        static bool decode(const Node& node, ChildTableInfo& rhs) {
            // Detect unknown configuration keys
            static const std::set<std::string> valid_keys = {"table_name", "tags"};
            check_unknown_keys(node, valid_keys, "child_table_info");

            if (!node["table_name"]) {
                throw std::runtime_error("Missing required field 'table_name' in child_table_info.");
            }
            if (!node["tags"]) {
                throw std::runtime_error("Missing required field 'tags' in child_table_info.");
            }

            rhs.table_name = node["table_name"].as<TableNameConfig>();
            rhs.tags = node["tags"].as<TagsConfig>();

            return true;
        }
    };


    template<>
    struct convert<CreateChildTableConfig::BatchConfig> {
        static bool decode(const Node& node, CreateChildTableConfig::BatchConfig& rhs) {
            // Detect unknown configuration keys
            static const std::set<std::string> valid_keys = {"size", "concurrency"};
            check_unknown_keys(node, valid_keys, "batch");

            if (node["size"]) {
                rhs.size = node["size"].as<int>();
            }
            if (node["concurrency"]) {
                rhs.concurrency = node["concurrency"].as<int>();
            }
            return true;
        }
    };


    template<>
    struct convert<TimestampGeneratorConfig> {
        static bool decode(const Node& node, TimestampGeneratorConfig& rhs) {
            // Detect unknown configuration keys
            static const std::set<std::string> valid_keys = {
                "start_timestamp", "timestamp_precision", "timestamp_step"
            };
            check_unknown_keys(node, valid_keys, "timestamp_strategy");

            if (node["start_timestamp"]) {
                rhs.start_timestamp = node["start_timestamp"].as<std::string>();
            }
            if (node["timestamp_precision"]) {
                rhs.timestamp_precision = node["timestamp_precision"].as<std::string>();
            }
            if (node["timestamp_step"]) {
                rhs.timestamp_step = node["timestamp_step"].as<int>();
            }
            return true;
        }
    };


    template<>
    struct convert<TimestampOriginalConfig::OffsetConfig> {
        static bool decode(const Node& node, TimestampOriginalConfig::OffsetConfig& rhs) {
            // Detect unknown configuration keys
            static const std::set<std::string> valid_keys = {"offset_type", "value"};
            check_unknown_keys(node, valid_keys, "timestamp_strategy::original::offset_config");

            if (!node["offset_type"]) {
                throw std::runtime_error("Missing required field 'offset_type' in offset_config");
            }
            std::string offset_type = node["offset_type"].as<std::string>();
            rhs.offset_type = offset_type;

            if (!node["value"]) {
                throw std::runtime_error("Missing required field 'value' in offset_config");
            }

            std::variant<int64_t, std::string> value;
            if (offset_type == "relative") {
                value = node["value"].as<std::string>();
            } else if (offset_type == "absolute") {
                try {
                    value = node["value"].as<std::string>();
                } catch (const YAML::BadConversion&) {
                    value = node["value"].as<int64_t>();
                }
            } else {
                throw std::runtime_error("Invalid offset_type: " + offset_type);
            }
            rhs.value = value;

            return true;
        }
    };


    template<>
    struct convert<TimestampOriginalConfig> {
        static bool decode(const Node& node, TimestampOriginalConfig& rhs) {
            // Detect unknown configuration keys
            static const std::set<std::string> valid_keys = {
                "column_index", "precision", "offset_config"
            };
            check_unknown_keys(node, valid_keys, "timestamp_strategy::original");

            if (node["column_index"]) {
                rhs.timestamp_index = node["column_index"].as<size_t>();
            }

            if (node["precision"]) {
                rhs.timestamp_precision = node["precision"].as<std::string>();
                // Validate precision
                if (rhs.timestamp_precision != "s" &&
                    rhs.timestamp_precision != "ms" &&
                    rhs.timestamp_precision != "us" &&
                    rhs.timestamp_precision != "ns") {
                    throw std::runtime_error("Invalid timestamp precision: " + rhs.timestamp_precision);
                }
            }

            if (node["offset_config"]) {
                rhs.offset_config = node["offset_config"].as<TimestampOriginalConfig::OffsetConfig>();
                if (rhs.offset_config) {
                    rhs.offset_config->parse_offset(rhs.timestamp_precision);
                }
            }

            return true;
        }
    };


    template<>
    struct convert<ColumnsConfig> {
        static bool decode(const Node& node, ColumnsConfig& rhs) {
            // Detect unknown configuration keys at the top level
            static const std::set<std::string> valid_keys = {"source_type", "generator", "csv"};
            check_unknown_keys(node, valid_keys, "columns");

            if (!node["source_type"]) {
                throw std::runtime_error("Missing required field 'source_type' in columns.");
            }
            rhs.source_type = node["source_type"].as<std::string>();

            if (rhs.source_type == "generator") {
                if (!node["generator"]) {
                    throw std::runtime_error("Missing required 'generator' configuration for source_type 'generator' in columns.");
                }

                const auto& generator = node["generator"];

                // Detect unknown keys in generator
                static const std::set<std::string> generator_keys = {"schema", "timestamp_strategy"};
                check_unknown_keys(generator, generator_keys, "columns::generator");

                if (generator["schema"]) {
                    rhs.generator.schema = generator["schema"].as<ColumnConfigVector>();
                } else {
                    throw std::runtime_error("Missing required field 'schema' in columns::generator.");
                }

                if (generator["timestamp_strategy"]) {
                    const auto& ts = generator["timestamp_strategy"];

                    // Detect unknown keys in timestamp_strategy
                    static const std::set<std::string> ts_keys = {"generator"};
                    check_unknown_keys(ts, ts_keys, "columns::generator::timestamp_strategy");

                    if (!ts["generator"]) {
                        throw std::runtime_error("Missing required field 'generator' in columns::generator::timestamp_strategy");
                    }
                    rhs.generator.timestamp_strategy.timestamp_config = ts["generator"].as<TimestampGeneratorConfig>();
                }
            } else if (rhs.source_type == "csv") {
                if (!node["csv"]) {
                    throw std::runtime_error("Missing required 'csv' configuration for source_type 'csv'.");
                }

                const auto& csv = node["csv"];

                // Detect unknown keys in csv
                static const std::set<std::string> csv_keys = {
                    "schema", "file_path", "has_header", "repeat_read", "delimiter", "tbname_index", "timestamp_strategy"
                };
                check_unknown_keys(csv, csv_keys, "columns::csv");

                if (csv["schema"]) {
                    rhs.csv.schema = csv["schema"].as<ColumnConfigVector>();
                } else {
                    throw std::runtime_error("Missing required 'schema' configuration for columns::csv");
                }

                if (csv["file_path"]) {
                    rhs.csv.file_path = csv["file_path"].as<std::string>();
                } else {
                    throw std::runtime_error("Missing required 'file_path' configuration for columns::csv");
                }

                if (csv["has_header"]) {
                    rhs.csv.has_header = csv["has_header"].as<bool>();
                }
                if (csv["repeat_read"]) {
                    rhs.csv.repeat_read = csv["repeat_read"].as<bool>();
                }
                if (csv["delimiter"]) {
                    rhs.csv.delimiter = csv["delimiter"].as<std::string>();
                }
                if (csv["tbname_index"]) {
                    rhs.csv.tbname_index = csv["tbname_index"].as<int>();
                }
                if (csv["timestamp_strategy"]) {
                    const auto& ts = csv["timestamp_strategy"];

                    // Detect unknown keys in timestamp_strategy
                    static const std::set<std::string> ts_keys = {"strategy_type", "original", "generator"};
                    check_unknown_keys(ts, ts_keys, "columns::csv::timestamp_strategy");

                    if (!ts["strategy_type"]) {
                        throw std::runtime_error("Missing required field 'strategy_type' in columns::csv::timestamp_strategy");
                    }

                    rhs.csv.timestamp_strategy.strategy_type = ts["strategy_type"].as<std::string>();

                    if (rhs.csv.timestamp_strategy.strategy_type == "original") {
                        if (!ts["original"]) {
                            throw std::runtime_error("Missing required field 'original' in columns::csv::timestamp_strategy");
                        }
                        rhs.csv.timestamp_strategy.timestamp_config = ts["original"].as<TimestampOriginalConfig>();
                    } else if (rhs.csv.timestamp_strategy.strategy_type == "generator") {
                        if (!ts["generator"]) {
                            throw std::runtime_error("Missing required field 'generator' in columns::csv::timestamp_strategy");
                        }
                        rhs.csv.timestamp_strategy.timestamp_config = ts["generator"].as<TimestampGeneratorConfig>();
                    } else {
                        throw std::runtime_error("Invalid strategy_type in columns::csv::timestamp_strategy: " +
                            rhs.csv.timestamp_strategy.strategy_type);
                    }
                }
            } else {
                throw std::runtime_error("Invalid 'source_type' in columns: " + rhs.source_type);
            }

            return true;
        }
    };


    template<>
    struct convert<InsertDataConfig::Source> {
        static bool decode(const Node& node, InsertDataConfig::Source& rhs) {
            // Detect unknown configuration keys
            static const std::set<std::string> valid_keys = {"table_name", "tags", "columns"};
            check_unknown_keys(node, valid_keys, "insert-data::source");

            if (node["table_name"]) {
                rhs.table_name = node["table_name"].as<TableNameConfig>();
            } else {
                throw std::runtime_error("Missing required field 'table_name' in insert-data::source.");
            }

            if (node["tags"]) {
                rhs.tags = node["tags"].as<TagsConfig>();
            }

            if (node["columns"]) {
                rhs.columns = node["columns"].as<ColumnsConfig>();
            } else {
                throw std::runtime_error("Missing required field 'columns' in insert-data::source.");
            }
            return true;
        }
    };


    template<>
    struct convert<InsertDataConfig::Target::TDengine> {
        static bool decode(const Node& node, InsertDataConfig::Target::TDengine& rhs) {
            // Detect unknown configuration keys
            static const std::set<std::string> valid_keys = {
                "connection_info", "database_info", "super_table_info"
            };
            check_unknown_keys(node, valid_keys, "insert-data::target::tdengine");

            if (node["connection_info"]) {
                rhs.connection_info = node["connection_info"].as<ConnectionInfo>();
            } else {
                throw std::runtime_error("Missing required field 'connection_info' in insert-data::target::tdengine.");
            }

            if (node["database_info"]) {
                rhs.database_info = node["database_info"].as<DatabaseInfo>();
            } else {
                throw std::runtime_error("Missing required field 'database_info' in insert-data::target::tdengine.");
            }

            if (node["super_table_info"]) {
                rhs.super_table_info = node["super_table_info"].as<SuperTableInfo>();
            } else {
                throw std::runtime_error("Missing required field 'super_table_info' in insert-data::target::tdengine.");
            }
            return true;
        }
    };

    template<>
    struct convert<InsertDataConfig::Target::FileSystem> {
        static bool decode(const Node& node, InsertDataConfig::Target::FileSystem& rhs) {
            // Detect unknown configuration keys
            static const std::set<std::string> valid_keys = {
                "output_dir", "file_prefix", "timestamp_format", "timestamp_interval",
                "include_header", "tbname_col_alias", "compression_level"
            };
            check_unknown_keys(node, valid_keys, "insert-data::target::file_system");

            if (node["output_dir"]) {
                rhs.output_dir = node["output_dir"].as<std::string>();
            } else {
                throw std::runtime_error("Missing required field 'output_dir' in insert-data::target::file_system.");
            }

            if (node["file_prefix"]) {
                rhs.file_prefix = node["file_prefix"].as<std::string>();
            }
            if (node["timestamp_format"]) {
                rhs.timestamp_format = node["timestamp_format"].as<std::string>();
            } else {
                throw std::runtime_error("Missing required field 'timestamp_format' in insert-data::target::file_system.");
            }

            if (node["timestamp_interval"]) {
                rhs.timestamp_interval = node["timestamp_interval"].as<std::string>();
            }

            if (node["include_header"]) {
                rhs.include_header = node["include_header"].as<bool>();
            }

            if (node["tbname_col_alias"]) {
                rhs.tbname_col_alias = node["tbname_col_alias"].as<std::string>();
            }

            if (node["compression_level"]) {
                rhs.compression_level = node["compression_level"].as<std::string>();
            }
            return true;
        }
    };


    template<>
    struct convert<InsertDataConfig::Target> {
        static bool decode(const Node& node, InsertDataConfig::Target& rhs) {
            // Detect unknown configuration keys
            static const std::set<std::string> valid_keys = {
                "target_type", "tdengine", "file_system", "mqtt"
            };
            check_unknown_keys(node, valid_keys, "insert-data::target");

            if (node["target_type"]) {
                rhs.target_type = node["target_type"].as<std::string>();
            }

            if (rhs.target_type == "tdengine") {
                if (node["tdengine"]) {
                    rhs.tdengine = node["tdengine"].as<InsertDataConfig::Target::TDengine>();
                    rhs.timestamp_precision = rhs.tdengine.database_info.precision;
                } else {
                    throw std::runtime_error("Missing required field 'tdengine' in insert-data::target.");
                }
            } else if (rhs.target_type == "file_system") {
                if (node["file_system"]) {
                    rhs.file_system = node["file_system"].as<InsertDataConfig::Target::FileSystem>();
                } else {
                    throw std::runtime_error("Missing required field 'file_system' in insert-data::target.");
                }
            } else if (rhs.target_type == "mqtt") {
                if (node["mqtt"]) {
                    rhs.mqtt = node["mqtt"].as<MqttInfo>();
                    rhs.timestamp_precision = rhs.mqtt.timestamp_precision;
                } else {
                    throw std::runtime_error("Missing required field 'mqtt' in insert-data::target.");
                }
            } else {
                throw std::runtime_error("Invalid target_type in insert-data::target: " + rhs.target_type);
            }
            return true;
        }
    };


    template<>
    struct convert<DataFormat> {
        static bool decode(const Node& node, DataFormat& rhs) {
            // Detect unknown configuration keys
            static const std::set<std::string> valid_keys = {
                "format_type", "stmt", "schemaless", "csv"
            };
            check_unknown_keys(node, valid_keys, "data_format");

            if (node["format_type"]) {
                rhs.format_type = node["format_type"].as<std::string>();
            }

            if (rhs.format_type == "sql") {

            }
            else if (rhs.format_type == "stmt") {
                if (node["stmt"]) {
                    const auto& stmt = node["stmt"];

                    // Detect unknown keys in stmt
                    static const std::set<std::string> stmt_keys = {"version"};
                    check_unknown_keys(stmt, stmt_keys, "data_format::stmt");

                    if (stmt["version"]) {
                        rhs.stmt_config.version = stmt["version"].as<std::string>();
                    }
                } else {
                    throw std::runtime_error("Missing required 'stmt' configuration for format_type 'stmt' in data_format.");
                }
            }
            else if (rhs.format_type == "schemaless") {
                if (node["schemaless"]) {
                    const auto& sl = node["schemaless"];

                    // Detect unknown keys in schemaless
                    static const std::set<std::string> sl_keys = {"protocol"};
                    check_unknown_keys(sl, sl_keys, "data_format::schemaless");

                    if (sl["protocol"]) {
                        rhs.schemaless_config.protocol = sl["protocol"].as<std::string>();
                    }
                } else {
                    throw std::runtime_error("Missing required 'schemaless' configuration for format_type 'schemaless' in data_format.");
                }
            }
            else if (rhs.format_type == "csv") {
                if (node["csv"]) {
                    const auto& csv = node["csv"];

                    // Detect unknown keys in csv
                    static const std::set<std::string> csv_keys = {"delimiter", "quote_character", "escape_character"};
                    check_unknown_keys(csv, csv_keys, "data_format::csv");

                    if (csv["delimiter"]) {
                        rhs.csv_config.delimiter = csv["delimiter"].as<std::string>();
                    }

                    if (csv["quote_character"]) {
                        rhs.csv_config.quote_character = csv["quote_character"].as<std::string>();
                    }

                    if (csv["escape_character"]) {
                        rhs.csv_config.escape_character = csv["escape_character"].as<std::string>();
                    }
                } else {
                    throw std::runtime_error("Missing required 'csv' configuration for format_type 'csv' in data_format.");
                }
            }
            else if (rhs.format_type == "msg") {

            }
            else {
                throw std::runtime_error("Invalid format_type in data_format: " + rhs.format_type);
            }
            return true;
        }
    };


    template<>
    struct convert<DataChannel> {
        static bool decode(const Node& node, DataChannel& rhs) {
            // Detect unknown configuration keys
            static const std::set<std::string> valid_keys = {"channel_type"};
            check_unknown_keys(node, valid_keys, "data_channel");

            if (node["channel_type"]) {
                rhs.channel_type = node["channel_type"].as<std::string>();
            }
            return true;
        }
    };


    template<>
    struct convert<InsertDataConfig::Control::DataQuality> {
        static bool decode(const Node& node, InsertDataConfig::Control::DataQuality& rhs) {
            // Detect unknown configuration keys at the top level
            static const std::set<std::string> valid_keys = {"data_disorder"};
            check_unknown_keys(node, valid_keys, "insert-data::control::data_quality");

            if (node["data_disorder"]) {
                const auto& disorder = node["data_disorder"];

                // Detect unknown keys in data_disorder
                static const std::set<std::string> disorder_keys = {"enabled", "intervals"};
                check_unknown_keys(disorder, disorder_keys, "insert-data::control::data_quality::data_disorder");

                if (disorder["enabled"]) {
                    rhs.data_disorder.enabled = disorder["enabled"].as<bool>();
                }

                if (rhs.data_disorder.enabled) {
                    if (disorder["intervals"]) {
                        for (const auto& interval : disorder["intervals"]) {
                            // Detect unknown keys in each interval
                            static const std::set<std::string> interval_keys = {"time_start", "time_end", "ratio", "latency_range"};
                            check_unknown_keys(interval, interval_keys, "insert-data::control::data_quality::data_disorder::intervals");

                            InsertDataConfig::Control::DataQuality::DataDisorder::Interval i;
                            if (interval["time_start"]) {
                                i.time_start = interval["time_start"].as<std::string>();
                            } else {
                                throw std::runtime_error("Missing required field 'time_start' in data_disorder::intervals.");
                            }

                            if (interval["time_end"]) {
                                i.time_end = interval["time_end"].as<std::string>();
                            } else {
                                throw std::runtime_error("Missing required field 'time_end' in data_disorder::intervals.");
                            }

                            if (interval["ratio"]) {
                                i.ratio = interval["ratio"].as<double>();
                            } else {
                                throw std::runtime_error("Missing required field 'ratio' in data_disorder::intervals.");
                            }

                            if (interval["latency_range"]) {
                                i.latency_range = interval["latency_range"].as<int>();
                            } else {
                                throw std::runtime_error("Missing required field 'latency_range' in data_disorder::intervals.");
                            }

                            rhs.data_disorder.intervals.push_back(i);
                        }
                    }
                }
            }
            return true;
        }
    };


    template<>
    struct convert<InsertDataConfig::Control::DataGeneration> {
        static bool decode(const Node& node, InsertDataConfig::Control::DataGeneration& rhs) {
            // Detect unknown configuration keys at the top level
            static const std::set<std::string> valid_keys = {
                "interlace_mode", "data_cache", "flow_control", "generate_threads", "per_table_rows", "queue_capacity", "queue_warmup_ratio"
            };
            check_unknown_keys(node, valid_keys, "insert-data::control::data_generation");

            if (node["interlace_mode"]) {
                const auto& interlace = node["interlace_mode"];
                // Detect unknown keys in interlace_mode
                static const std::set<std::string> interlace_keys = {"enabled", "rows"};
                check_unknown_keys(interlace, interlace_keys, "insert-data::control::data_generation::interlace_mode");

                if (interlace["enabled"]) {
                    rhs.interlace_mode.enabled = interlace["enabled"].as<bool>();
                }

                if (rhs.interlace_mode.enabled) {
                    if (interlace["rows"]) {
                        rhs.interlace_mode.rows = interlace["rows"].as<size_t>();
                    }
                }
            }
            if (node["data_cache"]) {
                const auto& data_cache = node["data_cache"];

                // Detect unknown keys in data_cache
                static const std::set<std::string> data_cache_keys = {"enabled", "cache_size"};
                check_unknown_keys(data_cache, data_cache_keys, "insert-data::control::data_generation::data_cache");

                if (data_cache["enabled"]) {
                    rhs.data_cache.enabled = data_cache["enabled"].as<bool>();
                }

                if (rhs.data_cache.enabled) {
                    if (data_cache["cache_size"]) {
                        rhs.data_cache.cache_size = data_cache["cache_size"].as<size_t>();
                    }
                }
            }
            if (node["flow_control"]) {
                const auto& flow_control = node["flow_control"];

                // Detect unknown keys in flow_control
                static const std::set<std::string> flow_control_keys = {"enabled", "rate_limit"};
                check_unknown_keys(flow_control, flow_control_keys, "insert-data::control::data_generation::flow_control");

                if (flow_control["enabled"]) {
                    rhs.flow_control.enabled = flow_control["enabled"].as<bool>();
                }

                if(rhs.flow_control.enabled) {
                    if (flow_control["rate_limit"]) {
                        rhs.flow_control.rate_limit = flow_control["rate_limit"].as<int64_t>();
                    }
                }
            }
            if (node["generate_threads"]) {
                rhs.generate_threads = node["generate_threads"].as<size_t>();
            }
            if (node["per_table_rows"]) {
                int64_t val = node["per_table_rows"].as<int64_t>();
                if (val == -1) {
                    rhs.per_table_rows = std::numeric_limits<int64_t>::max();
                } else if (val <= 0) {
                    throw std::runtime_error("per_table_rows must be positive or -1 (for unlimited).");
                } else {
                    rhs.per_table_rows = val;
                }
            }
            if (node["queue_capacity"]) {
                rhs.queue_capacity = node["queue_capacity"].as<int>();
            }
            if (node["queue_warmup_ratio"]) {
                rhs.queue_warmup_ratio = node["queue_warmup_ratio"].as<double>();
            }
            return true;
        }
    };


    template<>
    struct convert<InsertDataConfig::Control::InsertControl> {
        static bool decode(const Node& node, InsertDataConfig::Control::InsertControl& rhs) {
            // Detect unknown configuration keys
            static const std::set<std::string> valid_keys = {
                "per_request_rows", "auto_create_table", "insert_threads", "thread_allocation",
                "log_path", "enable_dryrun", "preload_table_meta", "failure_handling"
            };
            check_unknown_keys(node, valid_keys, "insert-data::control::insert_control");

            if (node["per_request_rows"]) {
                rhs.per_request_rows = node["per_request_rows"].as<size_t>();
            }
            if (node["auto_create_table"]) {
                rhs.auto_create_table = node["auto_create_table"].as<bool>();
            }
            if (node["insert_threads"]) {
                rhs.insert_threads = node["insert_threads"].as<int>();
            }
            if (node["thread_allocation"]) {
                rhs.thread_allocation = node["thread_allocation"].as<std::string>();
            }
            if (node["log_path"]) {
                rhs.log_path = node["log_path"].as<std::string>();
            }
            if (node["enable_dryrun"]) {
                rhs.enable_dryrun = node["enable_dryrun"].as<bool>();
            }
            if (node["preload_table_meta"]) {
                rhs.preload_table_meta = node["preload_table_meta"].as<bool>();
            }
            if (node["failure_handling"]) {
                const auto& failure = node["failure_handling"];

                // Detect unknown keys in failure_handling
                static const std::set<std::string> failure_keys = {"max_retries", "retry_interval_ms", "on_failure"};
                check_unknown_keys(failure, failure_keys, "insert-data::control::insert_control::failure_handling");

                if (failure["max_retries"]) {
                    rhs.failure_handling.max_retries = failure["max_retries"].as<size_t>();
                }
                if (failure["retry_interval_ms"]) {
                    rhs.failure_handling.retry_interval_ms = failure["retry_interval_ms"].as<int>();
                }
                if (failure["on_failure"]) {
                    rhs.failure_handling.on_failure = failure["on_failure"].as<std::string>();
                    if (rhs.failure_handling.on_failure != "exit" && rhs.failure_handling.on_failure != "continue") {
                        throw std::runtime_error("Invalid value for on_failure in failure_handling: " + rhs.failure_handling.on_failure);
                    }
                }
            }
            return true;
        }
    };


    template<>
    struct convert<InsertDataConfig::Control::TimeInterval> {
        static bool decode(const Node& node, InsertDataConfig::Control::TimeInterval& rhs) {
            // Detect unknown configuration keys at the top level
            static const std::set<std::string> valid_keys = {
                "enabled", "interval_strategy", "wait_strategy", "fixed_interval", "dynamic_interval"
            };
            check_unknown_keys(node, valid_keys, "insert-data::control::time_interval");

            if (node["enabled"]) {
                rhs.enabled = node["enabled"].as<bool>();
            }

            if (rhs.enabled == true) {
                if (node["interval_strategy"]) {
                    rhs.interval_strategy = node["interval_strategy"].as<std::string>();
                }
                if (node["wait_strategy"]) {
                    rhs.wait_strategy = node["wait_strategy"].as<std::string>();
                }
                if (rhs.interval_strategy == "fixed") {
                    // if (!node["fixed_interval"]) {
                    //     throw std::runtime_error("Missing required field 'fixed_interval' in insert-data::control::time_interval.");
                    // }

                    if (node["fixed_interval"]) {
                        const auto& fixed = node["fixed_interval"];

                        // Detect unknown keys in fixed_interval
                        static const std::set<std::string> fixed_keys = {"base_interval", "random_deviation"};
                        check_unknown_keys(fixed, fixed_keys, "insert-data::control::time_interval::fixed_interval");

                        if (fixed["base_interval"]) {
                            rhs.fixed_interval.base_interval = fixed["base_interval"].as<int>();
                        }
                        if (fixed["random_deviation"]) {
                            rhs.fixed_interval.random_deviation = fixed["random_deviation"].as<int>();
                        }
                    }
                } else if (rhs.interval_strategy == "first_to_first" || rhs.interval_strategy == "last_to_first") {
                    // if (!node["dynamic_interval"]) {
                    //     throw std::runtime_error("Missing required field 'dynamic_interval' in insert-data::control::time_interval.");
                    // }

                    if (node["dynamic_interval"]) {
                        const auto& dynamic = node["dynamic_interval"];

                        // Detect unknown keys in dynamic_interval
                        static const std::set<std::string> dynamic_keys = {"min_interval", "max_interval"};
                        check_unknown_keys(dynamic, dynamic_keys, "insert-data::control::time_interval::dynamic_interval");

                        if (dynamic["min_interval"]) {
                            rhs.dynamic_interval.min_interval = dynamic["min_interval"].as<int>();
                        }
                        if (dynamic["max_interval"]) {
                            rhs.dynamic_interval.max_interval = dynamic["max_interval"].as<int>();
                        }
                    }
                }
            }
            return true;
        }
    };


    template<>
    struct convert<InsertDataConfig::Control> {
        static bool decode(const Node& node, InsertDataConfig::Control& rhs) {
            // Detect unknown configuration keys at the top level
            static const std::set<std::string> valid_keys = {
                "data_format", "data_channel", "data_quality", "data_generation", "insert_control", "time_interval"
            };
            check_unknown_keys(node, valid_keys, "insert-data::control");

            if (node["data_format"]) {
                rhs.data_format = node["data_format"].as<DataFormat>();
            }
            if (node["data_channel"]) {
                rhs.data_channel = node["data_channel"].as<DataChannel>();
            }
            if (node["data_quality"]) {
                rhs.data_quality = node["data_quality"].as<InsertDataConfig::Control::DataQuality>();
            }
            if (node["data_generation"]) {
                rhs.data_generation = node["data_generation"].as<InsertDataConfig::Control::DataGeneration>();
            }
            if (node["insert_control"]) {
                rhs.insert_control = node["insert_control"].as<InsertDataConfig::Control::InsertControl>();
            }
            if (node["time_interval"]) {
                rhs.time_interval = node["time_interval"].as<InsertDataConfig::Control::TimeInterval>();
            }
            return true;
        }
    };

    // template<>
    // struct convert<InsertDataConfig> {
    //     static bool decode(const Node& node, InsertDataConfig& rhs) {
    //         if (node["source"]) {
    //             rhs.source = node["source"].as<InsertDataConfig::Source>();
    //         }
    //         if (node["target"]) {
    //             rhs.target = node["target"].as<InsertDataConfig::Target>();
    //         }
    //         if (node["control"]) {
    //             rhs.control = node["control"].as<InsertDataConfig::Control>();
    //         }
    //         return true;
    //     }
    // };


    template<>
    struct convert<QueryDataConfig::Source> {
        static bool decode(const Node& node, QueryDataConfig::Source& rhs) {
            // Detect unknown configuration keys
            static const std::set<std::string> valid_keys = {"connection_info"};
            check_unknown_keys(node, valid_keys, "query-data::source");

            if (node["connection_info"]) {
                rhs.connection_info = node["connection_info"].as<ConnectionInfo>();
            } else {
                throw std::runtime_error("Missing required field 'connection_info' in query-data::source.");
            }
            return true;
        }
    };


    template<>
    struct convert<QueryDataConfig::Control::QueryControl::Execution> {
        static bool decode(const Node& node, QueryDataConfig::Control::QueryControl::Execution& rhs) {
            // Detect unknown configuration keys
            static const std::set<std::string> valid_keys = {"mode", "threads", "times", "interval"};
            check_unknown_keys(node, valid_keys, "query-data::control::query_control::execution");

            if (node["mode"]) {
                rhs.mode = node["mode"].as<std::string>();
            }
            if (node["threads"]) {
                rhs.threads = node["threads"].as<int>();
            }
            if (node["times"]) {
                rhs.times = node["times"].as<int>();
            }
            if (node["interval"]) {
                rhs.interval = node["interval"].as<int>();
            }
            return true;
        }
    };


    template<>
    struct convert<QueryDataConfig::Control::QueryControl::Fixed> {
        static bool decode(const Node& node, QueryDataConfig::Control::QueryControl::Fixed& rhs) {
            // Detect unknown configuration keys
            static const std::set<std::string> valid_keys = {"queries"};
            check_unknown_keys(node, valid_keys, "query-data::control::query_control::fixed");

            if (node["queries"]) {
                for (const auto& query_node : node["queries"]) {
                    // Detect unknown keys in each query
                    static const std::set<std::string> query_keys = {"sql", "output_file"};
                    check_unknown_keys(query_node, query_keys, "query-data::control::query_control::fixed::queries");

                    QueryDataConfig::Control::QueryControl::FixedQuery query;

                    if (query_node["sql"]) {
                        query.sql = query_node["sql"].as<std::string>();
                    } else {
                        throw std::runtime_error("Missing required field 'sql' in query-data::control::query_control::fixed::queries.");
                    }
                    if (query_node["output_file"]) {
                        query.output_file = query_node["output_file"].as<std::string>();
                    } else {
                        throw std::runtime_error("Missing required field 'output_file' in query-data::control::query_control::fixed::queries.");
                    }

                    rhs.queries.push_back(query);
                }
            }
            return true;
        }
    };


    template<>
    struct convert<QueryDataConfig::Control::QueryControl::SuperTable> {
        static bool decode(const Node& node, QueryDataConfig::Control::QueryControl::SuperTable& rhs) {
            // Detect unknown configuration keys at the top level
            static const std::set<std::string> valid_keys = {
                "database_name", "super_table_name", "placeholder", "templates"
            };
            check_unknown_keys(node, valid_keys, "query-data::control::query_control::super_table");

            if (node["database_name"]) {
                rhs.database_name = node["database_name"].as<std::string>();
            } else {
                throw std::runtime_error("Missing required field 'database_name' in query-data::control::query_control::super_table.");
            }

            if (node["super_table_name"]) {
                rhs.super_table_name = node["super_table_name"].as<std::string>();
            } else {
                throw std::runtime_error("Missing required field 'super_table_name' in query-data::control::query_control::super_table.");
            }

            if (node["placeholder"]) {
                rhs.placeholder = node["placeholder"].as<std::string>();
            } else {
                throw std::runtime_error("Missing required field 'placeholder' in query-data::control::query_control::super_table.");
            }

            if (node["templates"]) {
                for (const auto& template_node : node["templates"]) {
                    // Detect unknown keys in each template
                    static const std::set<std::string> template_keys = {"sql_template", "output_file"};
                    check_unknown_keys(template_node, template_keys, "query-data::control::query_control::super_table::templates");

                    QueryDataConfig::Control::QueryControl::SuperTableQueryTemplate query_template;

                    if (template_node["sql_template"]) {
                        query_template.sql_template = template_node["sql_template"].as<std::string>();
                    } else {
                        throw std::runtime_error("Missing required field 'sql_template' in query-data::control::query_control::super_table::templates.");
                    }

                    if (template_node["output_file"]) {
                        query_template.output_file = template_node["output_file"].as<std::string>();
                    } else {
                        throw std::runtime_error("Missing required field 'output_file' in query-data::control::query_control::super_table::templates.");
                    }

                    rhs.templates.push_back(query_template);
                }
            }
            return true;
        }
    };


    template<>
    struct convert<QueryDataConfig::Control::QueryControl> {
        static bool decode(const Node& node, QueryDataConfig::Control::QueryControl& rhs) {
            // Detect unknown configuration keys at the top level
            static const std::set<std::string> valid_keys = {
                "log_path", "enable_dryrun", "execution", "query_type", "fixed", "super_table"
            };
            check_unknown_keys(node, valid_keys, "query-data::control::query_control");

            if (node["log_path"]) {
                rhs.log_path = node["log_path"].as<std::string>();
            }
            if (node["enable_dryrun"]) {
                rhs.enable_dryrun = node["enable_dryrun"].as<bool>();
            }
            if (node["execution"]) {
                rhs.execution = node["execution"].as<QueryDataConfig::Control::QueryControl::Execution>();
            }
            if (node["query_type"]) {
                rhs.query_type = node["query_type"].as<std::string>();
                if (rhs.query_type == "fixed") {
                    if (!node["fixed"]) {
                        throw std::runtime_error("Missing required field 'fixed' in query_control.");
                    }
                    rhs.fixed = node["fixed"].as<QueryDataConfig::Control::QueryControl::Fixed>();
                } else if (rhs.query_type == "super_table") {
                    if (!node["super_table"]) {
                        throw std::runtime_error("Missing required fields 'super_table' in query_control.");
                    }
                    rhs.super_table = node["super_table"].as<QueryDataConfig::Control::QueryControl::SuperTable>();
                } else {
                    throw std::runtime_error("Invalid or missing 'query_type' in query_control.");
                }
            }
            return true;
        }
    };


    template<>
    struct convert<QueryDataConfig::Control> {
        static bool decode(const Node& node, QueryDataConfig::Control& rhs) {
            // Detect unknown configuration keys at the top level
            static const std::set<std::string> valid_keys = {
                "data_format", "data_channel", "query_control"
            };
            check_unknown_keys(node, valid_keys, "query-data::control");

            if (node["data_format"]) {
                rhs.data_format = node["data_format"].as<DataFormat>();
            } else {
                throw std::runtime_error("Missing required field 'data_format' in query-data::control.");
            }

            if (node["data_channel"]) {
                rhs.data_channel = node["data_channel"].as<DataChannel>();
            } else {
                throw std::runtime_error("Missing required field 'data_channel' in query-data::control.");
            }

            if (node["query_control"]) {
                rhs.query_control = node["query_control"].as<QueryDataConfig::Control::QueryControl>();
            } else {
                throw std::runtime_error("Missing required field 'query_control' in query-data::control.");
            }

            return true;
        }
    };


    template<>
    struct convert<SubscribeDataConfig::Source> {
        static bool decode(const Node& node, SubscribeDataConfig::Source& rhs) {
            // Detect unknown configuration keys
            static const std::set<std::string> valid_keys = {"connection_info"};
            check_unknown_keys(node, valid_keys, "subscribe-data::source");

            if (node["connection_info"]) {
                rhs.connection_info = node["connection_info"].as<ConnectionInfo>();
            } else {
                throw std::runtime_error("Missing required field 'connection_info' in subscribe-data::source.");
            }
            return true;
        }
    };


    template<>
    struct convert<SubscribeDataConfig::Control::SubscribeControl::Execution> {
        static bool decode(const Node& node, SubscribeDataConfig::Control::SubscribeControl::Execution& rhs) {
            // Detect unknown configuration keys
            static const std::set<std::string> valid_keys = {"consumer_concurrency", "poll_timeout"};
            check_unknown_keys(node, valid_keys, "subscribe-data::control::subscribe_control::execution");

            if (node["consumer_concurrency"]) {
                rhs.consumer_concurrency = node["consumer_concurrency"].as<int>();
            }
            if (node["poll_timeout"]) {
                rhs.poll_timeout = node["poll_timeout"].as<int>();
            }
            return true;
        }
    };


    template<>
    struct convert<SubscribeDataConfig::Control::SubscribeControl::Topic> {
        static bool decode(const Node& node, SubscribeDataConfig::Control::SubscribeControl::Topic& rhs) {
            // Detect unknown configuration keys
            static const std::set<std::string> valid_keys = {"name", "sql"};
            check_unknown_keys(node, valid_keys, "subscribe-data::control::subscribe_control::topic");

            if (node["name"]) {
                rhs.name = node["name"].as<std::string>();
            } else {
                throw std::runtime_error("Missing required field 'name' in subscribe-data::control::subscribe_control::topic.");
            }

            if (node["sql"]) {
                rhs.sql = node["sql"].as<std::string>();
            } else {
                throw std::runtime_error("Missing required field 'sql' in subscribe-data::control::subscribe_control::topic.");
            }
            return true;
        }
    };


    template<>
    struct convert<SubscribeDataConfig::Control::SubscribeControl::Commit> {
        static bool decode(const Node& node, SubscribeDataConfig::Control::SubscribeControl::Commit& rhs) {
            // Detect unknown configuration keys
            static const std::set<std::string> valid_keys = {"mode"};
            check_unknown_keys(node, valid_keys, "subscribe-data::control::subscribe_control::commit");

            if (node["mode"]) {
                rhs.mode = node["mode"].as<std::string>();
            }
            return true;
        }
    };


    template<>
    struct convert<SubscribeDataConfig::Control::SubscribeControl::GroupID> {
        static bool decode(const Node& node, SubscribeDataConfig::Control::SubscribeControl::GroupID& rhs) {
            // Detect unknown configuration keys
            static const std::set<std::string> valid_keys = {"strategy", "custom_id"};
            check_unknown_keys(node, valid_keys, "subscribe-data::control::subscribe_control::group_id");

            if (node["strategy"]) {
                rhs.strategy = node["strategy"].as<std::string>();
            } else {
                throw std::runtime_error("Missing required field 'strategy' in subscribe-data::control::subscribe_control::group_id.");
            }

            if (rhs.strategy == "custom") {
                if(node["custom_id"]) {
                    rhs.custom_id = node["custom_id"].as<std::string>();
                } else {
                    throw std::runtime_error("Missing required field 'custom_id' in subscribe-data::control::subscribe_control::group_id when strategy is 'custom'.");
                }
            }
            return true;
        }
    };


    template<>
    struct convert<SubscribeDataConfig::Control::SubscribeControl::Output> {
        static bool decode(const Node& node, SubscribeDataConfig::Control::SubscribeControl::Output& rhs) {
            // Detect unknown configuration keys
            static const std::set<std::string> valid_keys = {"path", "file_prefix", "expected_rows"};
            check_unknown_keys(node, valid_keys, "subscribe-data::control::subscribe_control::output");

            if (node["path"]) {
                rhs.path = node["path"].as<std::string>();
            } else {
                throw std::runtime_error("Missing required field 'path' in subscribe-data::control::subscribe_control::output.");
            }

            if (node["file_prefix"]) {
                rhs.file_prefix = node["file_prefix"].as<std::string>();
            } else {
                throw std::runtime_error("Missing required field 'file_prefix' in subscribe-data::control::subscribe_control::output.");
            }

            if (node["expected_rows"]) {
                rhs.expected_rows = node["expected_rows"].as<int>();
            } else {
                throw std::runtime_error("Missing required field 'expected_rows' in subscribe-data::control::subscribe_control::output.");
            }
            return true;
        }
    };


    template<>
    struct convert<SubscribeDataConfig::Control::SubscribeControl> {
        static bool decode(const Node& node, SubscribeDataConfig::Control::SubscribeControl& rhs) {
            // Detect unknown configuration keys at the top level
            static const std::set<std::string> valid_keys = {
                "log_path", "enable_dryrun", "execution", "topics", "commit", "group_id", "output", "advanced"
            };
            check_unknown_keys(node, valid_keys, "subscribe-data::control::subscribe_control");

            if (node["log_path"]) {
                rhs.log_path = node["log_path"].as<std::string>();
            }

            if (node["enable_dryrun"]) {
                rhs.enable_dryrun = node["enable_dryrun"].as<bool>();
            }

            if (node["execution"]) {
                rhs.execution = node["execution"].as<SubscribeDataConfig::Control::SubscribeControl::Execution>();
            }

            if (node["topics"]) {
                rhs.topics = node["topics"].as<std::vector<SubscribeDataConfig::Control::SubscribeControl::Topic>>();
            }

            if (node["commit"]) {
                rhs.commit = node["commit"].as<SubscribeDataConfig::Control::SubscribeControl::Commit>();
            }

            if (node["group_id"]) {
                rhs.group_id = node["group_id"].as<SubscribeDataConfig::Control::SubscribeControl::GroupID>();
            }

            if (node["output"]) {
                rhs.output = node["output"].as<SubscribeDataConfig::Control::SubscribeControl::Output>();
            }

            if (node["advanced"]) {
                rhs.advanced = node["advanced"].as<std::map<std::string, std::string>>();
            }
            return true;
        }
    };


    template<>
    struct convert<SubscribeDataConfig::Control> {
        static bool decode(const Node& node, SubscribeDataConfig::Control& rhs) {
            // Detect unknown configuration keys at the top level
            static const std::set<std::string> valid_keys = {
                "data_format", "data_channel", "subscribe_control"
            };
            check_unknown_keys(node, valid_keys, "subscribe-data::control");

            if (node["data_format"]) {
                rhs.data_format = node["data_format"].as<DataFormat>();
            } else {
                throw std::runtime_error("Missing required field 'data_format' in subscribe-data::control.");
            }

            if (node["data_channel"]) {
                rhs.data_channel = node["data_channel"].as<DataChannel>();
            } else {
                throw std::runtime_error("Missing required field 'data_channel' in subscribe-data::control.");
            }

            if (node["subscribe_control"]) {
                rhs.subscribe_control = node["subscribe_control"].as<SubscribeDataConfig::Control::SubscribeControl>();
            } else {
                throw std::runtime_error("Missing required field 'subscribe_control' in subscribe-data::control.");
            }
            return true;
        }
    };
}
