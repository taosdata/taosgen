#include "ParameterContext.hpp"
#include "PluginConfigRegistry.hpp"
#include "StepParserRegistry.hpp"
#include "KafkaRegistrar.hpp"
#include "InsertDataAction.hpp"

void register_kafka_plugin_config_hooks() {
    // YAML -> KafkaConfig
    PluginConfigRegistry::register_parser("kafka",
        [](const YAML::Node& node) -> std::any {
            if (node.IsDefined()) {
                return node.as<KafkaConfig>();
            } else {
                return KafkaConfig{};
            }
        });

    // Formatting configuration decoding
    PluginConfigRegistry::register_format_decoder("kafka",
        [](const YAML::Node& node, InsertDataConfig& cfg) {
            // Detect unknown configuration keys
            static const std::set<std::string> target_kafka = {
                "kafka", "key_pattern", "key_serializer", "value_serializer", "tbname_key",
                "acks", "compression", "records_per_message"
            };
            std::set<std::string> valid_keys = YAML::merge_keys<std::string>({YAML::insert_common_keys, target_kafka});
            YAML::check_unknown_keys(node, valid_keys, "kafka/produce");

            auto* kc = get_plugin_config_mut<KafkaConfig>(cfg.extensions, "kafka");
            if (!kc) {
                set_plugin_config(cfg.extensions, "kafka", KafkaConfig{});
                kc = get_plugin_config_mut<KafkaConfig>(cfg.extensions, "kafka");
            }
            if (!kc) return;

            if (node["kafka"]) {
                *kc = node["kafka"].as<KafkaConfig>();
            }

            auto& rhs = cfg.data_format.kafka;

            if (node["key_pattern"]) {
                rhs.key_pattern = node["key_pattern"].as<std::string>();
            }

            if (node["key_serializer"]) {
                rhs.key_serializer = node["key_serializer"].as<std::string>();
                const std::set<std::string> valid_serializers = {
                    "string-utf8", "int8", "uint8", "int16", "uint16",
                    "int32", "uint32", "int64", "uint64"
                };
                if (valid_serializers.find(rhs.key_serializer) == valid_serializers.end()) {
                    throw std::runtime_error("Unsupported key_serializer: " + rhs.key_serializer + ". Supported serializers are 'string-utf8', 'int8', 'uint8', 'int16', 'uint16', 'int32', 'uint32', 'int64', 'uint64'.");
                }
            }

            if (node["value_serializer"]) {
                rhs.value_serializer = node["value_serializer"].as<std::string>();
                if (rhs.value_serializer != "influx" && rhs.value_serializer != "json") {
                    throw std::runtime_error("Unsupported value_serializer: " + rhs.value_serializer + ". Supported serializers are 'json' and 'influx'.");
                }
            }

            if (node["tbname_key"]) {
                rhs.tbname_key = node["tbname_key"].as<std::string>();
            }

            if (node["acks"]) {
                rhs.acks = node["acks"].as<std::string>();
                if (rhs.acks != "all" && rhs.acks != "1" && rhs.acks != "0") {
                    throw std::runtime_error("Invalid acks value: " + rhs.acks + " in kafka config.");
                }
            }

            if (node["compression"]) {
                rhs.compression = node["compression"].as<std::string>();
                const std::set<std::string> valid_compressions = {"none", "gzip", "snappy", "lz4", "zstd"};
                if (valid_compressions.find(rhs.compression) == valid_compressions.end()) {
                    throw std::runtime_error("Invalid compression value: " + rhs.compression + " in kafka config.");
                }
            }

            if (node["records_per_message"]) {
                int64_t val = node["records_per_message"].as<int64_t>();
                if (val <= 0) {
                    throw std::runtime_error("records_per_message must be greater than 0 in kafka config.");
                }
                rhs.records_per_message = static_cast<size_t>(val);
            }

            cfg.data_format.format_type = "kafka";
            cfg.data_format.support_tags = true;
        });

    // CLI merger
    PluginConfigRegistry::register_cli_merger("kafka",
        [](const std::unordered_map<std::string, std::string>& cli, PluginExtensions& exts) {
            auto* kc = get_plugin_config_mut<KafkaConfig>(exts, "kafka");
            if (!kc) {
                set_plugin_config(exts, "kafka", KafkaConfig{});
                kc = get_plugin_config_mut<KafkaConfig>(exts, "kafka");
            }
            if (!kc) return;

            bool hostport = false;

            if (auto it = cli.find("--host"); it != cli.end() && !it->second.empty()) {
                kc->host = it->second;
                hostport = true;
            }
            if (auto it = cli.find("--port"); it != cli.end() && !it->second.empty()) {
                try {
                    kc->port = std::stoi(it->second);
                } catch (...) {
                    throw std::runtime_error("Invalid port number: " + it->second);
                }
                hostport = true;
            }
            if (hostport) {
                kc->update_bootstrap_servers_from_host_port();
            }

            if (auto it = cli.find("--user"); it != cli.end()) {
                kc->rdkafka_options["sasl.username"] = it->second;
            }
            if (auto it = cli.find("--password"); it != cli.end()) {
                kc->rdkafka_options["sasl.password"] = it->second;
            }
        });

    // ENV merger
    PluginConfigRegistry::register_env_merger("kafka",
        [](PluginExtensions& exts) {
            auto* kc = get_plugin_config_mut<KafkaConfig>(exts, "kafka");
            if (!kc) {
                set_plugin_config(exts, "kafka", KafkaConfig{});
                kc = get_plugin_config_mut<KafkaConfig>(exts, "kafka");
            }
            if (!kc) return;

            const char* env_host = std::getenv("TAOS_HOST");
            const char* env_port = std::getenv("TAOS_PORT");
            const char* env_user = std::getenv("TAOS_USER");
            const char* env_pass = std::getenv("TAOS_PASSWORD");

            bool hostport = false;

            if (env_host && *env_host) {
                kc->host = env_host;
                hostport = true;
            }
            if (env_port && *env_port) {
                try {
                    kc->port = std::stoi(env_port);
                } catch (...) {
                    throw std::runtime_error(std::string("Invalid TAOS_PORT: ") + env_port);
                }
                hostport = true;
            }
            if (hostport) {
                kc->update_bootstrap_servers_from_host_port();
            }

            if (env_user && *env_user) {
                kc->rdkafka_options["sasl.username"] = env_user;
            }
            if (env_pass && *env_pass) {
                kc->rdkafka_options["sasl.password"] = env_pass;
            }
        });

    // Register action step parser
    StepParserRegistry::register_parser("kafka/produce",
        [](ParameterContext& ctx, Job& job, Step& step) {
            ctx.parse_insert_action(job, step, "kafka");
        });

    // Register action to ActionFactory
    ActionFactory::instance().register_action(
        "kafka/produce",
        [](const GlobalConfig& global, const ActionConfigVariant& config) {
            return std::make_unique<InsertDataAction>(global, std::get<InsertDataConfig>(config));
        });
}