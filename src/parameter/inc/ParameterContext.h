#pragma once

#include "ConfigParser.h"
#include "ConfigData.h"
// #include "ParamDescriptor.h"

#include <yaml-cpp/yaml.h>
#include <unordered_map>
#include <vector>
#include <string>
#include <optional>
#include <variant>


class ParameterContext {
public:
    ParameterContext();

    bool init(int argc, char* argv[]);
    void show_help();

    // Merge parameter sources
    void parse_commandline(int argc, char* argv[]);
    void merge_commandline();
    void merge_commandline(int argc, char* argv[]);
    void merge_environment_vars();
    void merge_yaml();
    void merge_yaml(const YAML::Node& config); 
    void merge_yaml(const std::string& file_path);

    // Get parameter
    // template <typename T>
    // T get(const std::string& path) const;

    // Get global config and connection info
    const ConfigData& get_config_data() const;
    const GlobalConfig& get_global_config() const;
    const ConnectionInfo& get_connection_info() const;
    const DatabaseInfo& get_database_info() const;
    const SuperTableInfo& get_super_table_info() const;


private:
    GlobalConfig global_config;
    int concurrency = 1;
    ConfigData config_data; // Top-level config data

    // Command line and environment variable storage
    std::unordered_map<std::string, std::string> cli_params;
    std::unordered_map<std::string, std::string> env_params;

    // Helper methods
    void parse_global(const YAML::Node& global_yaml);
    void parse_jobs(const YAML::Node& jobs_yaml);
    void parse_steps(const YAML::Node& steps_yaml, std::vector<Step>& steps);
    void parse_create_database_action(Step& step);
    void parse_create_super_table_action(Step& step);
    void parse_create_child_table_action(Step& step);
    void parse_insert_data_action(Step& step);
    void parse_query_data_action(Step& step);
    void parse_subscribe_data_action(Step& step);
    // void parse_job(const YAML::Node& job_yaml, Job& job);
    // void parse_step(const YAML::Node& step_yaml, Step& step);


    // void validate();

private:
    // Command option structure definition
    struct CommandOption {
        std::string long_opt;    // Long option (e.g. "--host")
        char short_opt;          // Short option (e.g. 'h')
        std::string description; // Option description
        bool requires_value;     // Whether value is required
    };

    // List of valid command options
    static const std::vector<CommandOption> valid_options;

};