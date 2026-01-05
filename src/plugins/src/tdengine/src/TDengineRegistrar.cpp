#include "TDengineRegistrar.hpp"

void register_tdengine_plugin_config_hooks() {
    // YAML -> TDengineConfig
    PluginConfigRegistrar::register_parser("tdengine",
        [](const YAML::Node& node) -> std::any {
            if (node.IsDefined()) {
                return node.as<TDengineConfig>();
            } else {
                return TDengineConfig{};
            }
        });

    // Formatting configuration decoding
    PluginConfigRegistrar::register_format_decoder("tdengine",
        [](const YAML::Node& node, InsertDataConfig& cfg) {
            // Detect unknown configuration keys
            static const std::set<std::string> target_tdengine = {
                "tdengine", "format", "auto_create_table"
            };

            std::set<std::string> valid_keys = YAML::merge_keys<std::string>({YAML::insert_common_keys, target_tdengine});
            YAML::check_unknown_keys(node, valid_keys, "tdengine/insert");

            auto* tc = get_plugin_config_mut<TDengineConfig>(cfg.extensions, "tdengine");
            if (!tc) {
                set_plugin_config(cfg.extensions, "tdengine", TDengineConfig{});
                tc = get_plugin_config_mut<TDengineConfig>(cfg.extensions, "tdengine");
            }
            if (!tc) return;

            if (node["tdengine"]) {
                *tc = node["tdengine"].as<TDengineConfig>();
            }

            auto& rhs = cfg.data_format.stmt;

            if (node["format"]) {
                cfg.data_format.format_type = node["format"].as<std::string>();
                if (cfg.data_format.format_type != "sql" && cfg.data_format.format_type != "stmt") {
                    throw std::runtime_error("Invalid format type for tdengine target: " + cfg.data_format.format_type + ". It must be 'sql' or 'stmt'.");
                }
            } else {
                cfg.data_format.format_type = "stmt";
            }

            if (node["auto_create_table"]) {
                rhs.auto_create_table = node["auto_create_table"].as<bool>();
                cfg.data_format.sql.auto_create_table = rhs.auto_create_table;
            }

            cfg.data_format.support_tags = rhs.auto_create_table;
        });

    // CLI merger
    PluginConfigRegistrar::register_cli_merger("tdengine",
        [](const std::unordered_map<std::string, std::string>& cli, PluginExtensions& exts) {
            auto* cfg = get_plugin_config_mut<TDengineConfig>(exts, "tdengine");
            if (!cfg) {
                set_plugin_config(exts, "tdengine", TDengineConfig{});
                cfg = get_plugin_config_mut<TDengineConfig>(exts, "tdengine");
            }
            if (!cfg) return;

            if (auto it = cli.find("--host"); it != cli.end() && !it->second.empty()) {
                cfg->host = it->second;
            }
            if (auto it = cli.find("--port"); it != cli.end() && !it->second.empty()) {
                try { cfg->port = std::stoi(it->second); }
                catch (...) {
                    throw std::runtime_error("Invalid port number: " + it->second);
                }
            }
            if (auto it = cli.find("--user"); it != cli.end()) {
                cfg->user = it->second;
            }
            if (auto it = cli.find("--password"); it != cli.end()) {
                cfg->password = it->second;
            }
        });

    // ENV merger
    PluginConfigRegistrar::register_env_merger("tdengine",
        [](PluginExtensions& exts) {
            auto* cfg = get_plugin_config_mut<TDengineConfig>(exts, "tdengine");
            if (!cfg) {
                set_plugin_config(exts, "tdengine", TDengineConfig{});
                cfg = get_plugin_config_mut<TDengineConfig>(exts, "tdengine");
            }
            if (!cfg) return;

            const char* env_host = std::getenv("TAOS_HOST");
            const char* env_port = std::getenv("TAOS_PORT");
            const char* env_user = std::getenv("TAOS_USER");
            const char* env_pass = std::getenv("TAOS_PASSWORD");

            if (env_host && *env_host) {
                cfg->host = env_host;
            }
            if (env_port && *env_port) {
                try {
                    cfg->port = std::stoi(env_port);
                }
                catch (...) {
                    throw std::runtime_error(std::string("Invalid TAOS_PORT: ") + env_port);
                }
            }
            if (env_user && *env_user) {
                cfg->user = env_user;
            }
            if (env_pass && *env_pass) {
                cfg->password = env_pass;
            }
        });
}