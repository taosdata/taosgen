#pragma once

#include "TableNameConfig.hpp"
#include "TagsConfig.hpp"
#include "ColumnsConfig.hpp"
#include "TDengineInfo.hpp"
#include "MqttInfo.hpp"
#include "DatabaseInfo.hpp"
#include "SuperTableInfo.hpp"
#include "DataFormat.hpp"
#include "DataChannel.hpp"
#include <string>
#include <vector>

struct InsertDataConfig {
    struct Source {
        TableNameConfig table_name; // Child table name config
        TagsConfig tags;            // Tag column config
        ColumnsConfig columns;      // Normal column config
    } source;

    struct Target {
        std::string timestamp_precision = "ms";     // Timestamp precision: ms, us, ns
        std::string target_type = "tdengine";       // Data target type: tdengine or file_system

        struct TDengine {
            TDengineInfo connection_info;
            DatabaseInfo database_info;
            SuperTableInfo super_table_info;
        } tdengine;

        struct FileSystem {
            std::string output_dir;
            std::string file_prefix = "data";
            std::string timestamp_format;
            std::string timestamp_interval = "1d";
            bool include_header = true;
            std::string tbname_col_alias = "device_id";
            std::string compression_level = "none";
        } file_system;

        MqttInfo mqtt;

    } target;

    struct Control {
        DataFormat data_format;
        DataChannel data_channel;

        struct DataQuality {
            struct DataDisorder {
                bool enabled = false;

                struct Interval {
                    std::variant<int64_t, std::string> time_start;
                    std::variant<int64_t, std::string> time_end;
                    double ratio = 0.0;       // Ratio
                    int latency_range = 0;    // Latency range
                };
                std::vector<Interval> intervals; // Out-of-order time intervals
            } data_disorder;
        } data_quality;

        struct DataGeneration {
            struct InterlaceMode {
                bool enabled = false;
                size_t rows = 1; // Number of rows
            } interlace_mode;

            struct DataCache {
                bool enabled = false;
                size_t cache_size = 1000000; // Cache size
            } data_cache;

            struct FlowControl {
                bool enabled = false;
                int64_t rate_limit = 0;             // Rows generated per second
            } flow_control;

            size_t generate_threads = 1;
            int64_t per_table_rows = 10000;
            int queue_capacity = 100;
            double queue_warmup_ratio = 0.5;
        } data_generation;

        struct InsertControl {
            size_t per_request_rows = 30000;
            bool auto_create_table = false;
            int insert_threads = 8;
            std::string thread_allocation = "index_range"; // index_range or vgroup_binding
            std::string log_path = "result.txt";
            bool enable_dryrun = false;
            bool preload_table_meta = false;

            struct FailureHandling {
                size_t max_retries = 0;
                int retry_interval_ms = 1000;
                std::string on_failure = "exit"; // exit or warn_and_continue
            } failure_handling;
        } insert_control;

        struct TimeInterval {
            bool enabled = false;                    // Enable interval control
            std::string interval_strategy = "fixed"; // Time interval strategy type
            std::string wait_strategy = "sleep";     // Wait strategy type: sleep or busy_wait

            struct FixedInterval {
                int base_interval = 1000; // Fixed interval value in milliseconds
                int random_deviation = 0; // Random deviation
            } fixed_interval;

            struct DynamicInterval {
                int min_interval = -1; // Minimum interval threshold
                int max_interval = -1; // Maximum interval threshold
            } dynamic_interval;
        } time_interval;
    } control;
};