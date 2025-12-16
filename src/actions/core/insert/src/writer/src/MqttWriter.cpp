#include "MqttWriter.hpp"
#include "ConnectorFactory.hpp"
#include "TimeRecorder.hpp"
#include <stdexcept>
#include <thread>
#include <iostream>

MqttWriter::MqttWriter(const InsertDataConfig& config,
                       const ColumnConfigInstanceVector& col_instances,
                       const ColumnConfigInstanceVector& tag_instances,
                       size_t no)
    : BaseWriter(config, col_instances, tag_instances) {
    client_ = std::make_unique<MqttClient>(config.mqtt, config.data_format.mqtt, no);
}

MqttWriter::~MqttWriter() {
    close();
}

bool MqttWriter::connect(std::optional<ConnectorSource>& conn_source) {
    (void)conn_source;

    if (client_ && client_->is_connected()) {
        return true;
    }

    try {
        return client_->connect();
    } catch (const std::exception& e) {
        LogUtils::error("MqttWriter connection failed: {}", e.what());
        return false;
    }
}

bool MqttWriter::select_db(const std::string& db_name) {
    if (!client_ || !client_->is_connected()) {
        throw std::runtime_error("MqttWriter is not connected");
    }
    return client_->select_db(db_name);
}

bool MqttWriter::prepare(const std::string& sql) {
    if (!client_ || !client_->is_connected()) {
        throw std::runtime_error("MqttWriter is not connected");
    }
    return client_->prepare(sql);
}

bool MqttWriter::write(const BaseInsertData& data) {
    if (!client_ || !client_->is_connected()) {
        throw std::runtime_error("MqttWriter is not connected");
    }

    // Apply time interval strategy
    apply_time_interval_strategy(data.start_time, data.end_time);

    // Execute write
    bool success = false;
    try {
        switch(data.type) {
            case BaseInsertData::DataType::MQTT:
                success = execute_with_retry([&] {
                    return handle_insert(static_cast<const MqttInsertData&>(data));
                }, "mqtt message insert");
                break;
            default:
                throw std::runtime_error("Unsupported data type for MqttWritter: " + std::to_string(static_cast<int>(data.type)));
        }
    } catch (const std::exception& e) {
        // Handling after retry failure
        if (config_.failure_handling.on_failure == "exit") {
            throw;
        }
        // Otherwise, ignore the error and continue
    }

    update_write_state(data, success);

    return success;
}

template<typename T>
bool MqttWriter::handle_insert(const T& data) {
    if (time_strategy_.is_literal_strategy()) {
        update_play_metrics(data);
    }

    TimeRecorder timer;
    bool success = client_->execute(data);
    write_metrics_.add_sample(timer.elapsed());

    return success;
}

void MqttWriter::close() noexcept {
    if (client_) {
        try {
            client_->close();
        } catch (const std::exception& e) {
            LogUtils::error("Exception during MqttWriter close: {}", e.what());
        }
        client_.reset();
    }
}

void MqttWriter::set_client(std::unique_ptr<MqttClient> client) {
    client_ = std::move(client);
}

MqttClient* MqttWriter::get_client() {
    return client_.get();
}