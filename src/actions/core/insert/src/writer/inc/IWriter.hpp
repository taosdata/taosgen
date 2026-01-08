#pragma once

#include "FormatResult.hpp"
#include "InsertDataConfig.hpp"
#include "ISinkContext.hpp"
#include "ActionMetrics.hpp"
#include "ConnectorSource.hpp"
#include <memory>
#include <variant>
#include <chrono>

class IWriter {
public:
    virtual ~IWriter() = default;

    // Connect/open resource
    virtual bool connect(std::optional<ConnectorSource>& conn_source) = 0;

    // Prepare for write operation
    virtual bool prepare(std::unique_ptr<const ISinkContext> context) = 0;

    // Execute write operation
    virtual bool write(const BaseInsertData& data) = 0;

    // Close/release resource
    virtual void close() noexcept = 0;

    // Get timestamp precision
    virtual std::string get_timestamp_precision() const = 0;

    virtual const ActionMetrics& get_play_metrics() const = 0;
    virtual const ActionMetrics& get_write_metrics() const = 0;
    virtual std::chrono::steady_clock::time_point start_write_time() const noexcept = 0;
    virtual std::chrono::steady_clock::time_point end_write_time() const noexcept = 0;
};