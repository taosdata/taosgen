#include "ParameterContext.hpp"
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <sstream>


ParameterContext::ParameterContext() {}

// Define static member variable
const std::vector<ParameterContext::CommandOption> ParameterContext::valid_options = {
    {"--host", 'h', "Specify FQDN to connect server", true},
    {"--port", 'P', "The TCP/IP port number to use for the connection", true},
    {"--user", 'u', "The user name to use when connecting to the server", true},
    {"--password", 'p', "The password to use when connecting to the server", true},
    {"--config-file", 'c', "Specify config file path", true},
    {"--verbose", 'v', "Increase output verbosity", false},
    {"--version", 'V', "Output version information", false},
    {"--help", '?', "Display this help message", false}
    // Add more command options here
};

void ParameterContext::show_help() {
    std::cout << "Usage: taosgen [OPTIONS]...\n\n"
              << "Options:\n";

    // Calculate the longest option length for alignment
    size_t max_opt_len = 0;
    for (const auto& opt : valid_options) {
        size_t total_len = 4 + opt.long_opt.length(); // 4 = length of "-X, "
        max_opt_len = std::max(max_opt_len, total_len);
    }

    // Reserve fixed space for VALUE
    const size_t value_width = 8;
    const size_t desc_offset = max_opt_len + value_width;

    // Output help info for each option
    for (const auto& opt : valid_options) {
        // Output short and long option
        std::cout << "  -" << opt.short_opt << ", " << opt.long_opt;

        // Calculate current output length
        size_t current_len = 4 + opt.long_opt.length();

        // Output VALUE (if needed) and spaces
        if (opt.requires_value) {
            std::cout << "=VALUE";
            current_len += 6;
        }

        // Calculate padding for alignment
        size_t padding = desc_offset - current_len;
        std::cout << std::string(padding, ' ');

        // Output description
        std::cout << opt.description << "\n";
    }

    std::cout << "\nExamples:\n"
              << "  taosgen --config-file=example.yaml\n"
              << "  taosgen -h localhost -P 6030 -u root -p taosdata\n"
              << "\nFor more information, visit: https://docs.taosdata.com/\n\n";
}

void ParameterContext::show_version() {
    std::cout << "taosgen version: 0.3.0" << std::endl;
    std::cout << "git: 8cd2b390ca6e10b7b42d6972d86e50a7fe4d4601" << std::endl;
    std::cout << "build: Linux-x64 2025-08-21 21:51:41 +0800" << std::endl;
}

// Parse global config
void ParameterContext::parse_global(const YAML::Node& global_yaml) {
    auto& global_config = config_data.global;
    if (global_yaml["confirm_prompt"]) {
        global_config.confirm_prompt = global_yaml["confirm_prompt"].as<bool>();
    }
    if (global_yaml["verbose"]) {
        global_config.verbose = global_yaml["verbose"].as<bool>();
    }
    if (global_yaml["log_dir"]) {
        global_config.log_dir = global_yaml["log_dir"].as<std::string>();
    }
    if (global_yaml["cfg_dir"]) {
        global_config.cfg_dir = global_yaml["cfg_dir"].as<std::string>();
    }
    if (global_yaml["connection_info"]) {
        global_config.connection_info = global_yaml["connection_info"].as<ConnectionInfo>();
    }
    if (global_yaml["data_format"]) {
        global_config.data_format = global_yaml["data_format"].as<DataFormat>();
    }
    if (global_yaml["data_channel"]) {
        global_config.data_channel = global_yaml["data_channel"].as<DataChannel>();
    }
    if (global_yaml["database_info"]) {
        global_config.database_info = global_yaml["database_info"].as<DatabaseInfo>();
    }
    if (global_yaml["super_table_info"]) {
        global_config.super_table_info = global_yaml["super_table_info"].as<SuperTableInfo>();
    }
}

void ParameterContext::parse_jobs(const YAML::Node& jobs_yaml) {
    for (const auto& job_node : jobs_yaml) {
        Job job;
        job.key = job_node.first.as<std::string>(); // Get job identifier
        const auto& job_content = job_node.second;

        if (job_content["name"]) {
            job.name = job_content["name"].as<std::string>();
        }
        if (job_content["needs"]) {
            job.needs = job_content["needs"].as<std::vector<std::string>>();
        }
        if (job_content["steps"]) {
            parse_steps(job_content["steps"], job.steps);
        }
        config_data.jobs.push_back(job);
    }
}

void ParameterContext::parse_steps(const YAML::Node& steps_yaml, std::vector<Step>& steps) {
    for (const auto& step_node : steps_yaml) {
        Step step;
        step.name = step_node["name"].as<std::string>();
        step.uses = step_node["uses"].as<std::string>();
        if (step_node["with"]) {
            step.with = step_node["with"]; // Keep original YAML node

            // Parse action by uses field
            if (step.uses == "actions/create-database") {
                parse_create_database_action(step);
            } else if (step.uses == "actions/create-super-table") {
                parse_create_super_table_action(step);
            } else if (step.uses == "actions/create-child-table") {
                parse_create_child_table_action(step);
            } else if (step.uses == "actions/insert-data") {
                parse_insert_data_action(step);
            } else if (step.uses == "actions/query-data") {
                parse_query_data_action(step);
            } else if (step.uses == "actions/subscribe-data") {
                parse_subscribe_data_action(step);
            } else {
                throw std::runtime_error("Unknown action type: " + step.uses);
            }
            // Other action parsing logic can be extended here
        }
        steps.push_back(step);
    }
}

void ParameterContext::parse_create_database_action(Step& step) {
    CreateDatabaseConfig create_db_config;

    // Parse connection_info (optional)
    if (step.with["connection_info"]) {
        create_db_config.connection_info = step.with["connection_info"].as<ConnectionInfo>();
    } else {
        // Use global config if not specified
        create_db_config.connection_info = config_data.global.connection_info;
    }

    // Parse data_format (optional)
    if (step.with["data_format"]) {
        create_db_config.data_format = step.with["data_format"].as<DataFormat>();
    } else {
        // Use global config if not specified
        create_db_config.data_format = config_data.global.data_format;
    }

    // Parse data_channel (optional)
    if (step.with["data_channel"]) {
        create_db_config.data_channel = step.with["data_channel"].as<DataChannel>();
    } else {
        // Use global config if not specified
        create_db_config.data_channel = config_data.global.data_channel;
    }

    // Parse database_info (required)
    if (step.with["database_info"]) {
        create_db_config.database_info = step.with["database_info"].as<DatabaseInfo>();
    } else {
        // throw std::runtime_error("Missing required 'database_info' for create-database action.");
        create_db_config.database_info = config_data.global.database_info;
    }

    // Print parse result
    std::cout << "Parsed create-database action: " << create_db_config.database_info.name << std::endl;

    // Save result to Step's action_config field
    step.action_config = std::move(create_db_config);
}

void ParameterContext::parse_create_super_table_action(Step& step) {
    CreateSuperTableConfig create_stb_config;

    // Parse connection_info (optional)
    if (step.with["connection_info"]) {
        create_stb_config.connection_info = step.with["connection_info"].as<ConnectionInfo>();
    } else {
        // Use global config if not specified
        create_stb_config.connection_info = config_data.global.connection_info;
    }

    // Parse data_format (optional)
    if (step.with["data_format"]) {
        create_stb_config.data_format = step.with["data_format"].as<DataFormat>();
    } else {
        // Use global config if not specified
        create_stb_config.data_format = config_data.global.data_format;
    }

    // Parse data_channel (optional)
    if (step.with["data_channel"]) {
        create_stb_config.data_channel = step.with["data_channel"].as<DataChannel>();
    } else {
        // Use global config if not specified
        create_stb_config.data_channel = config_data.global.data_channel;
    }

    // Parse database_info (required)
    if (step.with["database_info"]) {
        create_stb_config.database_info = step.with["database_info"].as<DatabaseInfo>();
    } else {
        throw std::runtime_error("Missing required 'database_info' for create-super-table action.");
    }

    // Parse super_table_info (required)
    if (step.with["super_table_info"]) {
        create_stb_config.super_table_info = step.with["super_table_info"].as<SuperTableInfo>();
    } else {
        throw std::runtime_error("Missing required 'super_table_info' for create-super-table action.");
    }

    // Validate columns and tags in super_table_info
    if (create_stb_config.super_table_info.columns.empty()) {
        throw std::runtime_error("Missing required 'columns' in super_table_info.");
    }

    // Print parse result
    std::cout << "Parsed create-super-table action: " << create_stb_config.super_table_info.name << std::endl;

    // Save result to Step's action_config field
    step.action_config = std::move(create_stb_config);
}

void ParameterContext::parse_create_child_table_action(Step& step) {
    CreateChildTableConfig create_child_config;

    // Parse connection_info (optional)
    if (step.with["connection_info"]) {
        create_child_config.connection_info = step.with["connection_info"].as<ConnectionInfo>();
    } else {
        // Use global config if not specified
        create_child_config.connection_info = config_data.global.connection_info;
    }

    // Parse data_format (optional)
    if (step.with["data_format"]) {
        create_child_config.data_format = step.with["data_format"].as<DataFormat>();
    } else {
        // Use global config if not specified
        create_child_config.data_format = config_data.global.data_format;
    }

    // Parse data_channel (optional)
    if (step.with["data_channel"]) {
        create_child_config.data_channel = step.with["data_channel"].as<DataChannel>();
    } else {
        // Use global config if not specified
        create_child_config.data_channel = config_data.global.data_channel;
    }

    // Parse database_info (required)
    if (step.with["database_info"]) {
        create_child_config.database_info = step.with["database_info"].as<DatabaseInfo>();
    } else {
        throw std::runtime_error("Missing required 'database_info' for create-child-table action.");
    }

    // Parse super_table_info (required)
    if (step.with["super_table_info"]) {
        create_child_config.super_table_info = step.with["super_table_info"].as<SuperTableInfo>();
    } else {
        throw std::runtime_error("Missing required 'super_table_info' for create-child-table action.");
    }

    // Parse child_table_info (required)
    if (step.with["child_table_info"]) {
        create_child_config.child_table_info = step.with["child_table_info"].as<ChildTableInfo>();
    } else {
        throw std::runtime_error("Missing required 'child_table_info' for create-child-table action.");
    }

    // Parse batch (optional)
    if (step.with["batch"]) {
        create_child_config.batch = step.with["batch"].as<CreateChildTableConfig::BatchConfig>();
    }

    // Print parse result
    std::cout << "Parsed create-child-table action for super table: " << create_child_config.super_table_info.name << std::endl;

    // Save result to Step's action_config field
    step.action_config = std::move(create_child_config);
}

void ParameterContext::parse_insert_data_action(Step& step) {
    InsertDataConfig insert_config;

    if (step.with["source"]) {
        insert_config.source = step.with["source"].as<InsertDataConfig::Source>();
    } else {
        throw std::runtime_error("Missing required 'source' for insert-data action.");
    }

    if (step.with["target"]) {
        insert_config.target = step.with["target"].as<InsertDataConfig::Target>();
    } else {
        throw std::runtime_error("Missing required 'target' for insert-data action.");
    }

    if (step.with["control"]) {
        insert_config.control = step.with["control"].as<InsertDataConfig::Control>();
    } else {
        throw std::runtime_error("Missing required 'control' for insert-data action.");
    }

    // Print parse result
    std::cout << "Parsed insert-data action." << std::endl;

    // Save result to Step's action_config field
    step.action_config = std::move(insert_config);
}

void ParameterContext::parse_query_data_action(Step& step) {
    QueryDataConfig query_config;

    // Parse source (required)
    if (step.with["source"]) {
        query_config.source = step.with["source"].as<QueryDataConfig::Source>();
    } else {
        throw std::runtime_error("Missing required 'source' for query-data action.");
    }

    // Parse control (required)
    if (step.with["control"]) {
        query_config.control = step.with["control"].as<QueryDataConfig::Control>();
    } else {
        throw std::runtime_error("Missing required 'control' for query-data action.");
    }

    // Print parse result
    std::cout << "Parsed query-data action." << std::endl;

    // Save result to Step's action_config field
    step.action_config = std::move(query_config);
}

void ParameterContext::parse_subscribe_data_action(Step& step) {
    SubscribeDataConfig subscribe_config;

    // Parse source (required)
    if (step.with["source"]) {
        subscribe_config.source = step.with["source"].as<SubscribeDataConfig::Source>();
    } else {
        throw std::runtime_error("Missing required 'source' for subscribe-data action.");
    }

    // Parse control (required)
    if (step.with["control"]) {
        subscribe_config.control = step.with["control"].as<SubscribeDataConfig::Control>();
    } else {
        throw std::runtime_error("Missing required 'control' for subscribe-data action.");
    }

    // Print parse result
    std::cout << "Parsed subscribe-data action." << std::endl;

    // Save result to Step's action_config field
    step.action_config = std::move(subscribe_config);
}

void ParameterContext::merge_yaml(const YAML::Node& config) {
    // Parse global config
    if (config["global"]) {
        parse_global(config["global"]);
    }

    // Parse job concurrency
    if (config["concurrency"]) {
        config_data.concurrency = config["concurrency"].as<int>();
    }

    // Parse job list
    if (config["jobs"]) {
        parse_jobs(config["jobs"]);
    }
}

void ParameterContext::merge_yaml(const std::string& file_path) {
    try {
        // Load and parse the YAML file
        YAML::Node config = YAML::LoadFile(file_path);
        // Call the existing merge_yaml function with the parsed YAML node
        merge_yaml(config);
    } catch (const YAML::Exception& e) {
        throw std::runtime_error("Failed to parse YAML file '" + file_path + "': " + e.what());
    } catch (const std::exception& e) {
        throw std::runtime_error("Error processing YAML file '" + file_path + "': " + e.what());
    }
}

void ParameterContext::merge_yaml() {
    if (!cli_params.count("--config-file")) {
        throw std::runtime_error("Missing required parameter: --config-file or -c");
    }

    const std::string& config_file = cli_params["--config-file"];
    merge_yaml(config_file);
}

void ParameterContext::parse_commandline(int argc, char* argv[]) {
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        std::string key, value;

        // Handle long option format (--key=value)
        if (arg.substr(0, 2) == "--") {
            size_t pos = arg.find('=');
            if (pos != std::string::npos) {
                // Handle long option with value (--key=value)
                key = arg.substr(0, pos);
                value = arg.substr(pos + 1);
            } else {
                // Handle long option without value (--key)
                key = arg;
                value = "";
            }

            // Validate long option
            auto it = std::find_if(valid_options.begin(), valid_options.end(),
                [&key](const CommandOption& opt) { return opt.long_opt == key; });

            if (it == valid_options.end()) {
                throw std::runtime_error("Unknown option: " + key);
            }

            // Check if value is required
            if (it->requires_value) {
                if (pos == std::string::npos) {
                    // Try to get value from next argv
                    if (i + 1 >= argc) {
                        throw std::runtime_error("Option requires a value: " + key);
                    }
                    value = argv[++i];
                }
            }

            cli_params[key] = value;
        }
        // Handle short option format (-k value)
        else if (arg[0] == '-') {
            // Check short option format length
            if (arg.length() != 2) {
                throw std::runtime_error("Invalid short option format '" + arg + "'. Must be single character after '-'");
            }

            char short_opt = arg[1];
            auto it = std::find_if(valid_options.begin(), valid_options.end(),
                [short_opt](const CommandOption& opt) { return opt.short_opt == short_opt; });

            if (it == valid_options.end()) {
                throw std::runtime_error("Unknown option: " + arg);
            }

            if (it->requires_value) {
                if (i + 1 >= argc) {
                    throw std::runtime_error("Option requires a value: " + key);
                }
                key = it->long_opt;
                value = argv[++i];
            } else {
                key = it->long_opt;
                value = "";
            }

            cli_params[key] = value;
        }
    }
}

void ParameterContext::merge_commandline(int argc, char* argv[]) {
    parse_commandline(argc, argv);
    merge_commandline();
}

void ParameterContext::merge_commandline() {
    // Map command line parameters to global config
    auto& conn_info = config_data.global.connection_info;
    if (cli_params.count("--host")) conn_info.host = cli_params["--host"];
    if (cli_params.count("--port")) {
        try {
            conn_info.port = std::stoi(cli_params["--port"]);
        } catch (const std::exception& e) {
            throw std::runtime_error("Invalid port number: " + cli_params["--port"]);
        }
    }
    if (cli_params.count("--user")) conn_info.user = cli_params["--user"];
    if (cli_params.count("--password")) conn_info.password = cli_params["--password"];

    if (cli_params.count("--verbose")) {
        config_data.global.verbose = true;
    }
}

void ParameterContext::merge_environment_vars() {
    // Define environment variables to read
    std::vector<std::pair<std::string, std::string>> env_mappings = {
        {"TAOS_HOST", "host"},
        {"TAOS_PORT", "port"},
        {"TAOS_USER", "user"},
        {"TAOS_PASSWORD", "password"}
    };

    auto& conn_info = config_data.global.connection_info;
    // Iterate environment variables and update connection info
    for (const auto& [env_var, key] : env_mappings) {
        const char* env_value = std::getenv(env_var.c_str());
        if (env_value) {
            if (key == "host") conn_info.host = env_value;
            else if (key == "port") conn_info.port = std::stoi(env_value);
            else if (key == "user") conn_info.user = env_value;
            else if (key == "password") conn_info.password = env_value;
        }
    }
}

bool ParameterContext::init(int argc, char* argv[]) {
    parse_commandline(argc, argv);

    if (cli_params.count("--help")) {
        show_help();
        return false;
    } else if (cli_params.count("--version")) {
        show_version();
        return false;
    }

    // Merge by priority from low to high
    merge_yaml();
    merge_environment_vars();
    merge_commandline();
    return true;
}

const ConfigData& ParameterContext::get_config_data() const {
    return config_data;
}

const GlobalConfig& ParameterContext::get_global_config() const {
    return config_data.global;
}

const ConnectionInfo& ParameterContext::get_connection_info() const {
    return config_data.global.connection_info;
}

const DatabaseInfo& ParameterContext::get_database_info() const {
    return config_data.global.database_info;
}

const SuperTableInfo& ParameterContext::get_super_table_info() const {
    return config_data.global.super_table_info;
}

// template <typename T>
// T ParameterContext::get(const std::string& path) const {
//     // Get parameter value by priority
//     if (cli_params.count(path)) {
//         return cli_params.at(path);
//     }
//     if (env_params.count(path)) {
//         return env_params.at(path);
//     }
//     if (json_config.contains(path)) {
//         return json_config.at(path).get<T>();
//     }
//     throw std::runtime_error("Parameter not found: " + path);
// }


// void validate() {
//     // Scope validation
//     validate_scope_constraints();
//
//     // Type validation
//     validate_type_compatibility();
//
//     // Dependency validation
//     validate_dependencies();
//
//     // Conflict validation
//     validate_conflicts();
//
//     // Custom business rules
//     validate_business_rules();
//   }