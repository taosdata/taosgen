#include "MqttWriter.hpp"
#include "ConnectorFactory.hpp"
#include "TimeRecorder.hpp"
#include <stdexcept>
#include <thread>
#include <iostream>

MqttWriter::MqttWriter(const InsertDataConfig& config, size_t no)
    : BaseWriter(config) {

    const auto* mc = get_plugin_config<MqttConfig>(config.extensions, "mqtt");
    if (mc == nullptr) {
        throw std::runtime_error("Mqtt configuration not found in insert extensions");
    }

    if (no == 0) {
        LogUtils::info("Inserting data into: {}{}", config.target_type, mc->get_sink_info());
    }

    client_ = std::make_unique<MqttClient>(*mc, config.data_format.mqtt, no);
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

bool MqttWriter::prepare(const std::string& context) {
    if (!client_ || !client_->is_connected()) {
        throw std::runtime_error("MqttWriter is not connected");
    }
    return client_->prepare(context);
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
        if (data.type == MQTT_TYPE_ID) {
            success = execute_with_retry([&] {
                return handle_insert(static_cast<const MqttInsertData&>(data));
            }, "mqtt message insert");
        } else {
            throw std::runtime_error(
                "Unsupported data type for MqttWriter: " +
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