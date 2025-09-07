#pragma once

#include <cstdint>
#include <string>
#include <variant>
#include <vector>
#include <optional>

struct GenerationConfig {
    struct InterlaceMode {
        bool enabled = false;
        size_t rows = 1;
    } interlace_mode;

    struct DataCache {
        bool enabled = false;
        size_t cache_size = 1000000;
    } data_cache;

    struct FlowControl {
        bool enabled = false;
        int64_t rate_limit = 0;
    } flow_control;

    struct DataDisorder {
        bool enabled = false;

        struct Interval {
            std::variant<int64_t, std::string> time_start;
            std::variant<int64_t, std::string> time_end;
            double ratio = 0.0;
            int latency_range = 0;
        };
        std::vector<Interval> intervals;
    } data_disorder;

    std::optional<size_t> generate_threads = 1;
    int64_t per_table_rows = 10000;
    size_t per_batch_rows = 10000;
};