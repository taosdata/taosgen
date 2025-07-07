#pragma once

#include <vector>
#include <optional>
#include <memory>
#include <unordered_map>
#include <queue>
#include "InsertDataConfig.h"
#include "RowGenerator.h"
#include "TimestampGenerator.h"
#include "ColumnsCSV.h"
#include "TableNameCSV.h"


class RowDataGenerator {
public:
    RowDataGenerator(const std::string& table_name, 
                    const ColumnsConfig& columns_config,
                    const ColumnConfigInstanceVector& instances,
                    const InsertDataConfig::Control& control,
                    const std::string& target_precision);
    
    // Get next row data
    std::optional<RowData> next_row();
    
    // Check if there is more data
    bool has_more() const;
    
    // Get the number of generated rows
    int generated_rows() const { return generated_rows_; }
    
    // Reset generator state
    void reset();

private:
    // Delayed queue element
    struct DelayedRow {
        int64_t deliver_timestamp;
        RowData row;
        bool operator<(const DelayedRow& other) const {
            return deliver_timestamp > other.deliver_timestamp;
        }
    };

    struct DisorderInterval {
        int64_t start_time;
        int64_t end_time;
        double ratio;
        int latency_range;
    };


    // Initialize cache
    void init_cache();
    
    // Initialize disorder
    void init_disorder();

    // Initialize raw data source
    void init_raw_source();

    // Apply disorder strategy
    bool apply_disorder(RowData& row);
    
    // Process delay queue
    void process_delay_queue();
    
    // Fetch a row from raw source
    std::optional<RowData> fetch_raw_row();

    // Initialize generator components
    void init_generator();
    
    // Initialize CSV reader
    void init_csv_reader();
    
    // Get data from generator
    RowData generate_from_generator();
    
    // Get data from CSV
    std::optional<RowData> generate_from_csv();
    
    const std::string& table_name_;
    const ColumnsConfig& columns_config_;
    const ColumnConfigInstanceVector& instances_;
    const InsertDataConfig::Control& control_;
    const std::string& target_precision_;

    // Data source components
    std::unique_ptr<RowGenerator> row_generator_;
    std::unique_ptr<ColumnsCSV> columns_csv_;
    std::unique_ptr<TimestampGenerator> timestamp_generator_;
    
    // CSV data
    std::vector<RowData> csv_rows_;
    size_t csv_row_index_ = 0;
    std::string csv_precision_;
    
    // State management
    int generated_rows_ = 0;
    int total_rows_ = 0;
    bool use_generator_ = false;

    // Disorder management
    std::priority_queue<DelayedRow> delay_queue_;
    std::vector<RowData> cache_;

    std::vector<DisorderInterval> disorder_intervals_;
    
    // Timestamp state
    int64_t current_timestamp_ = 0;
    // int64_t last_timestamp_ = 0;
    // int64_t timestamp_step_ = 1;
    // int64_t precision_factor_ = 1;
    // std::unordered_map<std::string, int64_t> last_timestamps_;
};