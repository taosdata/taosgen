#pragma once
#include <variant>
#include <chrono>
#include "FormatResult.hpp"
#include "InsertDataConfig.hpp"
#include "ActionMetrics.hpp"

class IWriter {
public:
    virtual ~IWriter() = default;
    
    // Connect/open resource
    virtual bool connect() = 0;
    
    // Select database
    virtual bool select_db(const std::string& db_name) = 0;

    // Prepare for write operation
    virtual bool prepare(const std::string& sql) = 0;

    // Execute write operation
    virtual void write(const BaseInsertData& data) = 0;
    
    // Close/release resource
    virtual void close() noexcept = 0;
    
    // Get timestamp precision
    virtual std::string get_timestamp_precision() const = 0;

    virtual const ActionMetrics& get_play_metrics() const = 0; 
    virtual const ActionMetrics& get_write_metrics() const = 0; 
    virtual std::chrono::steady_clock::time_point start_write_time() const noexcept = 0;
    virtual std::chrono::steady_clock::time_point end_write_time() const noexcept = 0;
    virtual bool is_literal_strategy() const = 0;
};