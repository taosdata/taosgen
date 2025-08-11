#pragma once

#include <atomic>
#include <memory>
#include "IWriter.hpp"
#include "ConnectorSource.hpp"
#include "TimeIntervalStrategy.hpp"
#include "FormatResult.hpp"
#include "InsertDataConfig.hpp"
#include "ActionMetrics.hpp"

class TDengineWriter : public IWriter {
public:
    explicit TDengineWriter(const InsertDataConfig& config);
    ~TDengineWriter();

    // Connect to database
    bool connect(std::optional<ConnectorSource>& conn_source) override;

    bool select_db(const std::string& db_name) override;

    // Prepare for write operation
    bool prepare(const std::string& sql) override;

    // Execute write operation
    void write(const BaseInsertData& data) override;

    // Close connection
    void close() noexcept override;

    // Get timestamp precision
    std::string get_timestamp_precision() const override { return timestamp_precision_; }

    const ActionMetrics& get_play_metrics() const override { return play_metrics_; }
    const ActionMetrics& get_write_metrics() const override { return write_metrics_; }

    std::chrono::steady_clock::time_point start_write_time() const noexcept override {
        return start_write_time_;
    }

    std::chrono::steady_clock::time_point end_write_time() const noexcept override {
        return end_write_time_;
    }

    bool is_literal_strategy() const override {
        return time_strategy_.strategy_type() == IntervalStrategyType::Literal;
    }

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
    std::chrono::steady_clock::time_point start_write_time_;
    std::chrono::steady_clock::time_point end_write_time_;

    std::unique_ptr<DatabaseConnector> connector_;

    // State tracking
    bool first_write_ = true;
    int64_t last_start_time_ = 0;
    int64_t last_end_time_ = 0;

    // Failure retry state
    size_t current_retry_count_ = 0;

    // Performance statistics
    ActionMetrics play_metrics_;
    ActionMetrics write_metrics_;
};