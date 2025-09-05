#pragma once
#include <cstdint>
#include "InsertDataConfig.hpp"

struct CheckpointActionConfig {
    bool enabled = false;
    int interval_sec = 60; // Checkpoint interval in seconds
    uint64_t start_timestamp = 0;
    int tableCount = 0;
    std::string timestamp_precision = "ms";
    int timestamp_step = 1;

    CheckpointActionConfig() = default;
    CheckpointActionConfig(const InsertDataConfig& config){
        enabled = config.control.checkpoint_info.enabled;
        interval_sec = config.control.checkpoint_info.interval_sec;
        if (enabled) {
            TimestampGeneratorConfig timestampConfig = config.source.columns.generator.timestamp_strategy.timestamp_config;
            this->start_timestamp = TimestampUtils::parse_timestamp(timestampConfig.start_timestamp, timestampConfig.timestamp_precision);
            this->timestamp_step = timestampConfig.timestamp_step;
            this->timestamp_precision = timestampConfig.timestamp_precision;
            this->tableCount = config.source.table_name.generator.count;
        }
    }
};
