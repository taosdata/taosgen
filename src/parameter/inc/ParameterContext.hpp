#pragma once

#include "ConfigParser.hpp"
#include "ConfigData.hpp"
// #include "ParamDescriptor.hpp"

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
    void show_version();

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
    const TDengineConfig& get_connection_info() const;
    const DatabaseInfo& get_database_info() const;
    const SuperTableInfo& get_super_table_info() const;


private:
    int concurrency = 1;
    ConfigData config_data; // Top-level config data

    // Command line and environment variable storage
    std::unordered_map<std::string, std::string> cli_params;
    std::unordered_map<std::string, std::string> env_params;

    // Helper methods
    void parse_tdengine(const YAML::Node& td_yaml);
    void parse_schema(const YAML::Node& td_yaml);
    void parse_global(const YAML::Node& global_yaml);
    void parse_jobs(const YAML::Node& jobs_yaml);
    void parse_steps(const YAML::Node& steps_yaml, Job& job);

    void prepare_work();
    void parse_td_create_database_action(Job& job, Step& step);
    void parse_td_create_super_table_action(Job& job, Step& step);
    void parse_create_child_table_action(Job& job, Step& step);
    void parse_insert_data_action(Job& job, Step& step);
    void parse_query_data_action(Job& job, Step& step);
    void parse_subscribe_data_action(Job& job, Step& step);
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