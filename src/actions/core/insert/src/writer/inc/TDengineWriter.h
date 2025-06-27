#pragma once
#include "IWriter.h"
#include "DatabaseConnector.h"
#include "TimeIntervalStrategy.h"
#include "FormatResult.h"
#include "InsertDataConfig.h"
#include <atomic>
#include <memory>

class TDengineWriter : public IWriter {
public:
    explicit TDengineWriter(const InsertDataConfig& config);
    ~TDengineWriter();

    // Connect to database
    bool connect() override;
    
    // Prepare for write operation
    bool prepare(const std::string& sql) override;

    // Execute write operation
    void write(const BaseInsertData& data) override;
    
    // Close connection
    void close() noexcept override;
    
    // Get timestamp precision
    std::string get_timestamp_precision() const override { return timestamp_precision_; }

private:
    // Handle data insertion
    template<typename T>
    bool handle_insert(const T& data);
    
    std::string get_format_description() const;

    // Apply time interval strategy
    void apply_time_interval_strategy(int64_t current_start, int64_t current_end);

private:
    const InsertDataConfig& config_;
    // const ColumnConfigInstanceVector& col_instances_;
    std::string timestamp_precision_;
    TimeIntervalStrategy time_strategy_;

    std::unique_ptr<DatabaseConnector> connector_;

    // State tracking
    bool first_write_ = true;
    int64_t last_start_time_ = 0;
    int64_t last_end_time_ = 0;
    
    // Failure retry state
    size_t current_retry_count_ = 0;
};