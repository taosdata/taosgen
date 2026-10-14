/*
 * Copyright (c) 2025 TAOS Data, Inc.
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

#include "MqttSinkPlugin.hpp"
#include "TimeRecorder.hpp"
#include "LogUtils.hpp"
#include <stdexcept>

MqttSinkPlugin::MqttSinkPlugin(const InsertDataConfig& config,
                               const ColumnConfigInstanceVector& col_instances,
                               const ColumnConfigInstanceVector& tag_instances,
                               size_t no)
    : BaseSinkPlugin(config, col_instances, tag_instances) {

    formatter_ = std::make_unique<MqttInsertDataFormatter>(config_.data_format);
    context_ = formatter_->init(config_, col_instances_, tag_instances_);

    const auto* mc = get_plugin_config<MqttConfig>(config.extensions, "mqtt");
    if (mc == nullptr) {
        throw std::runtime_error("MQTT configuration not found in PluginExtensions");
    }

    const auto* mf = get_format_opt<MqttFormatOptions>(config.data_format, "mqtt");
    if (mf == nullptr) {
        throw std::runtime_error("MQTT format options not found in DataFormat");
    }

    if (no == 0) {
        LogUtils::info("Inserting data into: {}", mc->get_sink_info());
    }

    client_ = std::make_unique<MqttClient>(*mc, *mf, no);
}

MqttSinkPlugin::~MqttSinkPlugin() {
    close();
}

bool MqttSinkPlugin::connect() {
    if (client_ && client_->is_connected()) {
        return true;
    }

    try {
        return client_->connect();
    } catch (const std::exception& e) {
        LogUtils::error("MqttSinkPlugin connection failed: {}", e.what());
        return false;
    }
}

void MqttSinkPlugin::close() noexcept {
    if (client_) {
        try {
            client_->close();
        } catch (const std::exception& e) {
            LogUtils::error("Exception during MqttSinkPlugin close: {}", e.what());
        }
        client_.reset();
    }
}

bool MqttSinkPlugin::is_connected() const {
    return client_ && client_->is_connected();
}

FormatResult MqttSinkPlugin::format(MemoryPool::MemoryBlock* block, bool is_checkpoint_recover) const {
    if (!formatter_) {
        throw std::runtime_error("Formatter is not initialized");
    }

    return formatter_->format(block, is_checkpoint_recover);
}

bool MqttSinkPlugin::write(const BaseInsertData& data) {
    if (!client_ || !client_->is_connected()) {
        throw std::runtime_error("MqttSinkPlugin is not connected");
    }

    // Apply time interval strategy
    apply_time_interval_strategy(data.start_time, data.end_time);

    // Execute write
    bool success = false;
    try {
        if (data.type == MQTT_TYPE_ID) {
            success = execute_with_retry([&] {
                return handle_insert<MqttInsertData>(data);
            }, "mqtt_message_insert", data);
        } else {
            throw std::runtime_error(
                "Unsupported data type for MqttSinkPlugin: " +
                std::string(data.type.name())
            );
        }
    } catch (const std::exception& e) {
        if (config_.failure_handling.on_failure == "exit") {
            throw;
        }
    }

    update_write_state(data, success);
    return success;
}

template<typename PayloadT>
bool MqttSinkPlugin::handle_insert(const BaseInsertData& data) {
    if (time_strategy_.is_literal_strategy()) {
        update_play_metrics(data);
    }

    TimeRecorder timer;
    const auto* payload = data.payload_as<PayloadT>();
    if (!payload) {
        throw std::runtime_error("MqttSinkPlugin: missing payload for requested type");
    }

    bool success = client_->execute(*payload);
    write_metrics_.add_sample(timer.elapsed());

    return success;
}

void MqttSinkPlugin::set_client(std::unique_ptr<MqttClient> client) {
    client_ = std::move(client);
}

MqttClient* MqttSinkPlugin::get_client() {
    return client_.get();
}
