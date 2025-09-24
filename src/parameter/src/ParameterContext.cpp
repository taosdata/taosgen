#include "ParameterContext.hpp"
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <sstream>
#include "CheckpointAction.hpp"

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
              << "  taosgen -h localhost -P 6041 -u root -p taosdata\n"
              << "\nFor more information, visit: https://docs.taosdata.com/\n\n";
}

void ParameterContext::show_version() {
    std::cout << "taosgen version: 0.5.0" << std::endl;
    std::cout << "git: " << TSGEN_BUILD_GIT << std::endl;
    std::cout << "build: " << TSGEN_BUILD_TARGET_OSTYPE << "-" << TSGEN_BUILD_TARGET_CPUTYPE << " " << TSGEN_BUILD_DATE << std::endl;
}

void ParameterContext::parse_tdengine(const YAML::Node& td_yaml) {
    auto& global_config = config_data.global;
    global_config.tdengine = td_yaml.as<TDengineConfig>();
}

void ParameterContext::parse_mqtt(const YAML::Node& td_yaml) {
    auto& global_config = config_data.global;
    global_config.mqtt = td_yaml.as<MqttConfig>();
}

void ParameterContext::parse_schema(const YAML::Node& schema_yaml) {
    auto& global_config = config_data.global;
    global_config.schema = schema_yaml.as<SchemaConfig>();

    // if (!global_config.schema.tbname.enabled && !global_config.schema.from_csv.enabled) {
    //     throw std::runtime_error("Missing required field 'tbname' or 'from_csv' in schema.");
    // }

    if (global_config.schema.columns.size() == 0) {
        throw std::runtime_error("Schema must have at least one column defined.");
    }
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
        global_config.connection_info = global_yaml["connection_info"].as<TDengineConfig>();
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
        job.tdengine = config_data.global.tdengine;
        job.mqtt = config_data.global.mqtt;
        job.schema = config_data.global.schema;

        job.key = job_node.first.as<std::string>(); // Get job identifier
        const auto& job_content = job_node.second;

        // Detect unknown configuration keys
        static const std::set<std::string> valid_keys = {
            "name", "needs", "steps"
        };
        YAML::check_unknown_keys(job_content, valid_keys, "job");

        if (job_content["name"]) {
            job.name = job_content["name"].as<std::string>();
        } else {
            job.name = job.key;
        }

        if (job_content["needs"]) {
            job.needs = job_content["needs"].as<std::vector<std::string>>();
        }

        if (job_content["steps"]) {
            parse_steps(job_content["steps"], job);
        }
        config_data.jobs.push_back(job);
    }

    prepare_work();
}

void ParameterContext::prepare_work() {
    for (auto& job : config_data.jobs) {
        if (job.find_create) {
            continue;
        }

        if (job.need_create) {
            bool has_create_db_dependency = false;
            for (const auto& dep_key : job.needs) {
                auto it = std::find_if(config_data.jobs.begin(), config_data.jobs.end(),
                    [&dep_key](const Job& j) { return j.key == dep_key; });
                if (it != config_data.jobs.end() && it->find_create) {
                    has_create_db_dependency = true;
                    break;
                }
            }

            if (has_create_db_dependency) {
                continue;
            }

            job.steps.insert(
                job.steps.begin(),
                Step{
                    .name = "Create Database",
                    .uses = "tdengine/create-database",
                    .with = YAML::Node(YAML::NodeType::Map),
                    .action_config = CreateDatabaseConfig{
                        .tdengine = job.tdengine,
                        .checkpoint_info = CheckpointInfo{
                            .enabled = false,
                            .interval_sec = 60
                        }
                    }
                }
            );
        }
    }
}

void ParameterContext::parse_steps(const YAML::Node& steps_yaml, Job& job) {
    for (const auto& step_node : steps_yaml) {
        Step step;

        // Detect unknown configuration keys
        static const std::set<std::string> valid_keys = {
            "name", "uses", "with"
        };
        YAML::check_unknown_keys(step_node, valid_keys, "job::steps");

        if (step_node["uses"]) {
            step.uses = step_node["uses"].as<std::string>();
        } else {
            throw std::runtime_error("Missing required field 'uses' for step in job: " + job.name);
        }

        if (step_node["name"]) {
            step.name = step_node["name"].as<std::string>();
        } else {
            step.name = step.uses;
        }

        if (step_node["with"]) {
            step.with = step_node["with"];
        } else {
            step.with = YAML::Node(YAML::NodeType::Map);
        }

        // Parse action by uses field
        if (step.uses == "tdengine/create-database") {
            parse_td_create_database_action(job, step);
        } else if (step.uses == "tdengine/create-super-table") {
            parse_td_create_super_table_action(job, step);
        } else if (step.uses == "tdengine/create-child-table") {
            parse_td_create_child_table_action(job, step);
        } else if (step.uses == "tdengine/insert-data") {
            parse_comm_insert_data_action(job, step, "tdengine");
            CheckpointAction::checkpoint_recover(config_data.global, std::get<InsertDataConfig>(step.action_config));
        } else if (step.uses == "mqtt/publish-data") {
            parse_comm_insert_data_action(job, step, "mqtt");
        }
        else if (step.uses == "actions/query-data") {
            parse_query_data_action(job, step);
        } else if (step.uses == "actions/subscribe-data") {
            parse_subscribe_data_action(job, step);
        } else {
            // throw std::runtime_error("Unknown action type: " + step.uses);
        }
        // Other action parsing logic can be extended here

        job.steps.push_back(step);
    }
}

void ParameterContext::parse_td_create_database_action(Job& job, Step& step) {
    CreateDatabaseConfig create_db_config = step.with.as<CreateDatabaseConfig>();

    create_db_config.tdengine = job.tdengine;

    // Parse database_info (required)
    if (step.with["checkpoint"]) {
        create_db_config.checkpoint_info = step.with["checkpoint"].as<CheckpointInfo>();
    }
    // Print parse result
    std::cout << "Parsed create-database action: " << create_db_config.tdengine.database << std::endl;

    // Save result to Step's action_config field
    step.action_config = std::move(create_db_config);
    job.find_create = true;
}

void ParameterContext::parse_td_create_super_table_action(Job& job, Step& step) {
    CreateSuperTableConfig create_stb_config = step.with.as<CreateSuperTableConfig>();

    create_stb_config.tdengine = job.tdengine;
    create_stb_config.schema = job.schema;


    if (step.with["schema"]) {
        const auto& schema = step.with["schema"];

        if (schema["name"]) {
            create_stb_config.schema.name = schema["name"].as<std::string>();
        }

        if (schema["from_csv"]) {
            create_stb_config.schema.from_csv = schema["from_csv"].as<FromCSVConfig>();
        }

        if (schema["tbname"]) {
            create_stb_config.schema.tbname = schema["tbname"].as<TableNameConfig>();
        }

        if (schema["columns"]) {
            create_stb_config.schema.columns = schema["columns"].as<ColumnConfigVector>();
        }

        if (schema["tags"]) {
            create_stb_config.schema.tags = schema["tags"].as<ColumnConfigVector>();
        }

        if (schema["generation"]) {
            create_stb_config.schema.generation = schema["generation"].as<GenerationConfig>();
        }
        create_stb_config.schema.apply();
    }

    // Validate columns and tags
    if (create_stb_config.schema.columns.empty()) {
        throw std::runtime_error("Missing required 'columns' in schema.");
    }

    // Print parse result
    std::cout << "Parsed create-super-table action: " << create_stb_config.schema.name << std::endl;

    // Save result to Step's action_config field
    job.schema = create_stb_config.schema;
    step.action_config = std::move(create_stb_config);
    job.need_create = true;
}

void ParameterContext::parse_td_create_child_table_action(Job& job, Step& step) {
    CreateChildTableConfig create_ctb_config = step.with.as<CreateChildTableConfig>();

    create_ctb_config.tdengine = job.tdengine;
    create_ctb_config.schema = job.schema;

    if (step.with["schema"]) {
        const auto& schema = step.with["schema"];

        if (schema["name"]) {
            create_ctb_config.schema.name = schema["name"].as<std::string>();
        }

        if (schema["from_csv"]) {
            create_ctb_config.schema.from_csv = schema["from_csv"].as<FromCSVConfig>();
        }

        if (schema["tbname"]) {
            create_ctb_config.schema.tbname = schema["tbname"].as<TableNameConfig>();
        }

        if (schema["columns"]) {
            create_ctb_config.schema.columns = schema["columns"].as<ColumnConfigVector>();
        }

        if (schema["tags"]) {
            create_ctb_config.schema.tags = schema["tags"].as<ColumnConfigVector>();
        }

        if (schema["generation"]) {
            create_ctb_config.schema.generation = schema["generation"].as<GenerationConfig>();
        }
        create_ctb_config.schema.apply();
    }

    // Parse batch (optional)
    if (step.with["batch"]) {
        create_ctb_config.batch = step.with["batch"].as<CreateChildTableConfig::BatchConfig>();
    }

    // Print parse result
    std::cout << "Parsed create-child-table action for super table: " << create_ctb_config.schema.name << std::endl;

    // Save result to Step's action_config field
    job.schema = create_ctb_config.schema;
    step.action_config = std::move(create_ctb_config);
}

void ParameterContext::parse_comm_insert_data_action(Job& job, Step& step, std::string target_type) {
    step.with["target"] = target_type;
    InsertDataConfig insert_config = step.with.as<InsertDataConfig>();

    insert_config.target_type = target_type;
    insert_config.tdengine = job.tdengine;
    insert_config.mqtt = job.mqtt;
    insert_config.schema = job.schema;

    if (step.with["tdengine"]) {
        insert_config.tdengine = step.with["tdengine"].as<TDengineConfig>();
    }

    if (step.with["mqtt"]) {
        insert_config.mqtt = step.with["mqtt"].as<MqttConfig>();
    }

    if (step.with["schema"]) {
        const auto& schema = step.with["schema"];

        if (schema["name"]) {
            insert_config.schema.name = schema["name"].as<std::string>();
        }

        if (schema["from_csv"]) {
            insert_config.schema.from_csv = schema["from_csv"].as<FromCSVConfig>();
        }

        if (schema["tbname"]) {
            insert_config.schema.tbname = schema["tbname"].as<TableNameConfig>();
        }

        if (schema["columns"]) {
            insert_config.schema.columns = schema["columns"].as<ColumnConfigVector>();
        }

        if (schema["tags"]) {
            insert_config.schema.tags = schema["tags"].as<ColumnConfigVector>();
        }

        if (schema["generation"]) {
            insert_config.schema.generation = schema["generation"].as<GenerationConfig>();
        }
        insert_config.schema.apply();
    }

    if (step.with["timestamp_precision"]) {
        insert_config.timestamp_precision = step.with["timestamp_precision"].as<std::string>();
    } else {
        insert_config.timestamp_precision = insert_config.schema.columns[0].ts.get_precision();
    }

    if (!insert_config.schema.generation.generate_threads.has_value()) {
        if (job.schema.generation.generate_threads.has_value()) {
            insert_config.schema.generation.generate_threads = job.schema.generation.generate_threads;
        } else {
            insert_config.schema.generation.generate_threads = insert_config.insert_threads;
        }
    }

    if (insert_config.schema.generation.data_cache.enabled) {
        if (insert_config.queue_capacity < insert_config.schema.generation.data_cache.num_cached_batches) {
            std::cerr << "[Config Warning] queue_capacity (" << insert_config.queue_capacity
                      << ") is less than num_cached_batches ("
                      << insert_config.schema.generation.data_cache.num_cached_batches
                      << "). Adjusting queue_capacity to "
                      << insert_config.schema.generation.data_cache.num_cached_batches << "." << std::endl;
            insert_config.queue_capacity = insert_config.schema.generation.data_cache.num_cached_batches;
        }
    }

    // Print parse result
    std::cout << "Parsed insert-data action." << std::endl;

    // Save result to Step's action_config field
    job.schema = insert_config.schema;
    step.action_config = std::move(insert_config);
}

void ParameterContext::parse_query_data_action(Job& /*job*/, Step& step) {
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

void ParameterContext::parse_subscribe_data_action(Job& /*job*/, Step& step) {
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

    if (config["tdengine"]) {
        parse_tdengine(config["tdengine"]);
    }

    if (config["mqtt"]) {
        parse_mqtt(config["mqtt"]);
    }

    if (config["schema"]) {
        parse_schema(config["schema"]);
    } else {
        load_default_schema();
    }

    // Parse global config
    // if (config["global"]) {
    //     parse_global(config["global"]);
    // }

    // Parse job concurrency
    if (config["concurrency"]) {
        config_data.concurrency = config["concurrency"].as<int>();
    }

    merge_commandline();

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
        config_data.global.yaml_cfg_dir = file_path;
        merge_yaml(config);
    } catch (const YAML::Exception& e) {
        throw std::runtime_error("Failed to parse YAML file '" + file_path + "': " + e.what());
    } catch (const std::exception& e) {
        throw std::runtime_error("Error processing YAML file '" + file_path + "': " + e.what());
    }
}

void ParameterContext::merge_yaml() {
    if (cli_params.count("--config-file")) {
        // throw std::runtime_error("Missing required parameter: --config-file or -c");
        const std::string& config_file = cli_params["--config-file"];
        merge_yaml(config_file);
    } else {
        load_default_config();
    }
}

void ParameterContext::load_default_schema() {
    YAML::Node schema = YAML::Load(R"(
name: meters
tbname:
  prefix: d
  count: 10000
  from: 0
columns:
  - name: ts
    type: timestamp
    start: 1735660800000
    precision : ms
    step: 1
  - name: current
    type: float
    min: 0
    max: 100
  - name: voltage
    type: int
    min: 200
    max: 240
  - name: phase
    type: float
    expr: _i * math.pi % 180
tags:
  - name: groupid
    type: int
    min: 1
    max: 10
  - name: location
    type: binary(24)
    values:
      - New York
      - Los Angeles
      - Chicago
      - Houston
      - Phoenix
      - Philadelphia
      - San Antonio
      - San Diego
      - Dallas
      - Austin
generation:
  concurrency: 8
  rows_per_table: 10000
  rows_per_batch: 10000
)");

    parse_schema(schema);
}

void ParameterContext::load_default_config() {
    YAML::Node config = YAML::Load(R"(
tdengine:
  dsn: taos://root:taosdata@127.0.0.1:6030/tsbench
  drop_if_exists: true
  props: precision 'ms' vgroups 4

schema:
  name: meters
  tbname:
    prefix: d
    count: 10000
    from: 0
  columns:
    - name: ts
      type: timestamp
      start: 1735660800000
      precision : ms
      step: 1
    - name: current
      type: float
      min: 0
      max: 100
    - name: voltage
      type: int
      min: 200
      max: 240
    - name: phase
      type: float
      expr: _i * math.pi % 180
  tags:
    - name: groupid
      type: int
      min: 1
      max: 10
    - name: location
      type: binary(24)
      values:
        - New York
        - Los Angeles
        - Chicago
        - Houston
        - Phoenix
        - Philadelphia
        - San Antonio
        - San Diego
        - Dallas
        - Austin
  generation:
    concurrency: 8
    rows_per_table: 10000
    rows_per_batch: 10000

jobs:
  # TDengine insert job
  insert-data:
    steps:
      - uses: tdengine/create-super-table
      - uses: tdengine/create-child-table
        with:
          batch:
            size: 1000
            concurrency: 10

      - uses: tdengine/insert-data
        with:
          concurrency: 8
)");

    merge_yaml(config);
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
    auto& tdengine = config_data.global.tdengine;
    auto& mqtt = config_data.global.mqtt;
    if (cli_params.count("--host"))
        tdengine.host = cli_params["--host"];

    if (cli_params.count("--port")) {
        try {
            tdengine.port = std::stoi(cli_params["--port"]);
        } catch (const std::exception& e) {
            throw std::runtime_error("Invalid port number: " + cli_params["--port"]);
        }
    }
    if (cli_params.count("--user")) {
        tdengine.user = cli_params["--user"];
        mqtt.user = cli_params["--user"];
    }

    if (cli_params.count("--password")) {
        tdengine.password = cli_params["--password"];
        mqtt.password = cli_params["--password"];
    }

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

    auto& tdengine = config_data.global.tdengine;
    auto& mqtt = config_data.global.mqtt;
    // Iterate environment variables and update connection info
    for (const auto& [env_var, key] : env_mappings) {
        const char* env_value = std::getenv(env_var.c_str());
        if (env_value) {
            if (key == "host") {
                tdengine.host = env_value;
            } else if (key == "port") {
                tdengine.port = std::stoi(env_value);
            } else if (key == "user") {
                tdengine.user = env_value;
                mqtt.user = env_value;
            } else if (key == "password") {
                tdengine.password = env_value;
                mqtt.password = env_value;
            }
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
    // merge_commandline();
    return true;
}

const ConfigData& ParameterContext::get_config_data() const {
    return config_data;
}

const GlobalConfig& ParameterContext::get_global_config() const {
    return config_data.global;
}

const TDengineConfig& ParameterContext::get_tdengine() const {
    return config_data.global.tdengine;
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