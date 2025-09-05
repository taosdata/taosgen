#include "TDengineWriter.hpp"
#include "ConnectorFactory.hpp"
#include "TimeRecorder.hpp"
#include <stdexcept>
#include <thread>
#include <iostream>

TDengineWriter::TDengineWriter(const InsertDataConfig& config, const ColumnConfigInstanceVector& col_instances, size_t no, std::shared_ptr<ActionRegisterInfo> action_info)
    : BaseWriter(config, col_instances, action_info), no_(no) {}

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
            connector_ = ConnectorFactory::create(
                config_.control.data_channel,
                config_.target.tdengine.connection_info
            );
            return connector_->connect();
        }
    } catch (const std::exception& e) {
        std::cerr << "TDengineWriter connection failed: " << e.what() << std::endl;
        connector_.reset();
        return false;
    }
}

bool TDengineWriter::select_db(const std::string& db_name) {
    if (!connector_) {
        throw std::runtime_error("TDengineWriter is not connected");
    }
    return connector_->select_db(db_name);
}

bool TDengineWriter::prepare(const std::string& sql) {
    if (!connector_) {
        throw std::runtime_error("TDengineWriter is not connected");
    }
    return connector_->prepare(sql);
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
        switch(data.type) {
            case BaseInsertData::DataType::SQL:
                success = execute_with_retry([&] {
                    return handle_insert(static_cast<const SqlInsertData&>(data));
                }, "sql insert");
                break;

            case BaseInsertData::DataType::STMT:
                success = execute_with_retry([&] {
                    return handle_insert(static_cast<const StmtV2InsertData&>(data));
                }, "stmt insert");
                break;
            default:
                throw std::runtime_error("Unsupported data type: " + std::to_string(static_cast<int>(data.type)));
        }
    } catch (const std::exception& e) {
        // Handling after retry failure
        if (config_.control.insert_control.failure_handling.on_failure == "exit") {
            throw;
        }
        // Otherwise, ignore the error and continue
    }

    update_write_state(data, success);
    notify(data, success);
    return success;
}


template<typename T>
bool TDengineWriter::handle_insert(const T& data) {
    if (time_strategy_.is_literal_strategy()) {
        update_play_metrics(data);
    }

    TimeRecorder timer;
    bool success = connector_->execute(data);
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