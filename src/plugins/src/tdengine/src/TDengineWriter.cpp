#include "TDengineWriter.hpp"
#include "ConnectorFactory.hpp"
#include "TimeRecorder.hpp"
#include "StmtContext.hpp"
#include <stdexcept>
#include <thread>
#include <iostream>

TDengineWriter::TDengineWriter(const InsertDataConfig& config,
                               size_t no,
                               std::shared_ptr<ActionRegisterInfo> action_info)
    : BaseWriter(config, action_info) {

    const auto* tc = get_plugin_config<TDengineConfig>(config_.extensions, "tdengine");
    if (tc == nullptr) {
        throw std::runtime_error("TDengine configuration not found in insert extensions");
    }

    if (no == 0) {
        LogUtils::info("Inserting data into: {}", tc->get_sink_info());
    }
}

TDengineWriter::~TDengineWriter() {
    close();
}

bool TDengineWriter::connect(std::optional<ConnectorSource>& conn_source) {
    if (connector_ && connector_->is_connected()) {
        return true;
    }

    try {
        // Create connector
        if (conn_source) {
            connector_ = conn_source->get_connector();
            return connector_->is_connected();
        } else {
            const auto* tc = get_plugin_config<TDengineConfig>(config_.extensions, "tdengine");
            if (tc == nullptr) {
                throw std::runtime_error("TDengine configuration not found in insert extensions");
            }

            connector_ = ConnectorFactory::create(*tc);
            return connector_->connect();
        }
    } catch (const std::exception& e) {
        LogUtils::error("TDengineWriter connection failed: {}", e.what());
        connector_.reset();
        return false;
    }
}

bool TDengineWriter::prepare(std::unique_ptr<const ISinkContext> context) {
    if (!connector_) {
        throw std::runtime_error("TDengineWriter is not connected");
    }
    if (const auto* stmt = dynamic_cast<const StmtContext*>(context.get())) {
        return connector_->prepare(stmt->sql);
    }
    return true;
}

bool TDengineWriter::write(const BaseInsertData& data) {
    if (!connector_) {
        throw std::runtime_error("TDengineWriter is not connected");
    }

    // Apply time interval strategy
    apply_time_interval_strategy(data.start_time, data.end_time);

    // Execute write
    bool success = false;
    try {
        if (data.type == SQL_TYPE_ID) {
            success = execute_with_retry([&] {
                return handle_insert<SqlInsertData>(data);
            }, "sql insert");
        } else if (data.type == STMTV2_TYPE_ID) {
            success = execute_with_retry([&] {
                return handle_insert<StmtV2InsertData>(data);
            }, "stmt v2 insert");
        } else {
            throw std::runtime_error(
                "Unsupported data type for TDengineWriter: " +
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


template<typename PayloadT>
bool TDengineWriter::handle_insert(const BaseInsertData& data) {
    if (time_strategy_.is_literal_strategy()) {
        update_play_metrics(data);
    }

    TimeRecorder timer;
    const auto* payload = data.payload_as<PayloadT>();
    if (!payload) {
        throw std::runtime_error("TDengineWriter: missing payload for requested type");
    }

    bool success = connector_->execute(*payload);
    write_metrics_.add_sample(timer.elapsed());

    return success;
}

void TDengineWriter::close() noexcept {
    if (connector_) {
        try {
            connector_->close();
        } catch (const std::exception& e) {
            // Ignore exception on close
        }
        connector_.reset();
    }
}