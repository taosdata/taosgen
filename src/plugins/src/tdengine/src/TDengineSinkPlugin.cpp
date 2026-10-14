/*
 * Copyright (c) 2026 TAOS Data, Inc.
 *
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#include "TDengineSinkPlugin.hpp"
#include "ConnectorFactory.hpp"
#include "TimeRecorder.hpp"
#include "StmtContext.hpp"
#include "LogUtils.hpp"
#include <stdexcept>

TDengineSinkPlugin::TDengineSinkPlugin(const InsertDataConfig& config,
                                       const ColumnConfigInstanceVector& col_instances,
                                       const ColumnConfigInstanceVector& tag_instances,
                                       size_t no,
                                       std::shared_ptr<ActionRegisterInfo> action_info)
    : BaseSinkPlugin(config, col_instances, tag_instances, std::move(action_info)) {

    if (config_.data_format.format_type == "sql") {
        formatter_ = std::make_unique<SqlInsertDataFormatter>(config_.data_format);
    } else if (config_.data_format.format_type == "stmt") {
        formatter_ = std::make_unique<StmtInsertDataFormatter>(config_.data_format);
    } else if (config_.data_format.format_type == "schemaless") {
        formatter_ = std::make_unique<SchemalessInsertDataFormatter>(config_.data_format);
    } else {
        throw std::invalid_argument("Unsupported format type: " + config_.data_format.format_type);
    }

    context_ = formatter_->init(config_, col_instances_, tag_instances_);

    tc_ = get_plugin_config<TDengineConfig>(config_.extensions, "tdengine");
    if (!tc_) {
        throw std::runtime_error("TDengine configuration not found in PluginExtensions");
    }

    if (no == 0) {
        LogUtils::info("Inserting data into: {}", tc_->get_sink_info());
    }
}

TDengineSinkPlugin::~TDengineSinkPlugin() {
    close();
}

bool TDengineSinkPlugin::connect() {
    if (!connector_) {
        connector_ = ConnectorFactory::create(*tc_);
    }

    if (connector_->is_connected()) {
        return true;
    }

    return connector_->connect();
}

bool TDengineSinkPlugin::connect_with_source(std::optional<ConnectorSource>& conn_source) {
    if (connector_ && connector_->is_connected()) {
        return true;
    }

    try {
        // Create connector
        if (conn_source) {
            connector_ = conn_source->get_connector();
            return connector_->is_connected();
        } else {
            connector_ = ConnectorFactory::create(*tc_);
            return connector_->connect();
        }
    } catch (const std::exception& e) {
        LogUtils::error("TDengineSinkPlugin connection failed: {}", e.what());
        connector_.reset();
        return false;
    }
}

void TDengineSinkPlugin::close() noexcept {
    if (connector_) {
        try {
            connector_->close();
        } catch (const std::exception& e) {
            LogUtils::error("Exception during TDengineSinkPlugin close: {}", e.what());
        }
        connector_.reset();
    }
}

bool TDengineSinkPlugin::is_connected() const {
    return connector_ && connector_->is_connected();
}

bool TDengineSinkPlugin::prepare() {
    if (!is_connected()) {
        throw std::runtime_error("TDengineSinkPlugin is not connected");
    }

    if (const auto* stmt = dynamic_cast<const StmtContext*>(context_.get())) {
        return connector_->prepare(stmt->sql);
    }

    return true;
}

FormatResult TDengineSinkPlugin::format(MemoryPool::MemoryBlock* block, bool is_checkpoint_recover) const {
    if (!formatter_) {
        throw std::runtime_error("Formatter is not initialized");
    }

    return formatter_->format(block, is_checkpoint_recover);
}

bool TDengineSinkPlugin::write(const BaseInsertData& data) {
    if (!connector_) {
        throw std::runtime_error("TDengineSinkPlugin is not connected");
    }

    // Apply time interval strategy
    apply_time_interval_strategy(data.start_time, data.end_time);

    // Execute write
    bool success = false;
    try {
        if (data.type == SQL_TYPE_ID) {
            success = execute_with_retry([&] {
                return handle_insert<SqlInsertData>(data);
            }, "sql_insert", data);
        } else if (data.type == STMTV2_TYPE_ID) {
            success = execute_with_retry([&] {
                return handle_insert<StmtV2InsertData>(data);
            }, "stmt_v2_insert", data);
        } else if (data.type == SCHEMALESS_TYPE_ID) {
            success = execute_with_retry([&] {
                return handle_insert<SchemalessInsertData>(data);
            }, "schemaless_insert", data);
        } else {
            throw std::runtime_error(
                "Unsupported data type for TDengineSinkPlugin: " +
                std::string(data.type.name())
            );
        }
    } catch (const std::exception& e) {
        // Handling after retry failure
        if (config_.failure_handling.on_failure == "exit") {
            throw;
        }
        // Otherwise, ignore the error and continue
    }

    update_write_state(data, success);
    notify(data, success);
    return success;
}

std::string TDengineSinkPlugin::get_retry_error_context(std::string_view fallback_reason) const {
    if (!connector_) {
        return BaseSinkPlugin::get_retry_error_context(fallback_reason);
    }

    const auto& error = connector_->last_error();
    if (error.message.empty()) {
        return BaseSinkPlugin::get_retry_error_context(fallback_reason);
    }

    if (error.code) {
        return fmt::format(
            "error_code=0x{:08x} error={:?}",
            static_cast<uint32_t>(*error.code),
            error.message
        );
    }

    return fmt::format("error={:?}", error.message);
}

void TDengineSinkPlugin::set_connector(std::unique_ptr<DatabaseConnector> connector) {
    connector_ = std::move(connector);
}

template<typename PayloadT>
bool TDengineSinkPlugin::handle_insert(const BaseInsertData& data) {
    if (time_strategy_.is_literal_strategy()) {
        update_play_metrics(data);
    }

    TimeRecorder timer;
    const auto* payload = data.payload_as<PayloadT>();
    if (!payload) {
        throw std::runtime_error("TDengineSinkPlugin: missing payload for requested type");
    }

    bool success = connector_->execute(*payload);
    write_metrics_.add_sample(timer.elapsed());

    return success;
}
