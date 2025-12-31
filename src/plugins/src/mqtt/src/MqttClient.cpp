#include "MqttClient.hpp"
#include "LogUtils.hpp"
#include <sstream>
#include <iomanip>

#include <mqtt/async_client.h>
#include <mqtt/message.h>
#include <mqtt/connect_options.h>
#include <mqtt/properties.h>

PahoMqttClient::PahoMqttClient(const MqttConfig& config, const DataFormat::MqttConfig& format, size_t no)
    : config_(config), format_(format), no_(no) {

    LogUtils::debug("Creating MQTT client #{}", no_);
    std::string client_id = config.generate_client_id(no);
    mqtt::create_options create_opts;
    create_opts.set_server_uri(config.uri);
    create_opts.set_client_id(client_id);
    create_opts.set_max_buffered_messages(static_cast<int>(config.max_buffered_messages));

    client_ = std::make_unique<mqtt::async_client>(create_opts);
    default_props_.add(mqtt::property(mqtt::property::USER_PROPERTY, "content-type", format.content_type));

    if (!format.compression.empty()) {
        default_props_.add(mqtt::property(mqtt::property::USER_PROPERTY, "compression", format.compression));
    }

    if (!format.encoding.empty()) {
        default_props_.add(mqtt::property(mqtt::property::USER_PROPERTY, "encoding", format.encoding));
    }

    token_wait_thread_ = std::thread(&PahoMqttClient::token_wait_func, this);
}

PahoMqttClient::~PahoMqttClient() {
    LogUtils::debug("Destroying MQTT client #{}", no_);
    close();
}

bool PahoMqttClient::connect() {
    mqtt::connect_options conn_opts;
    conn_opts.set_user_name(config_.user);
    conn_opts.set_password(config_.password);
    conn_opts.set_keep_alive_interval(config_.keep_alive);
    conn_opts.set_clean_session(config_.clean_session);
    conn_opts.set_automatic_reconnect(true);

    try {
        client_->connect(conn_opts)->wait();
        return true;
    } catch (const mqtt::exception& e) {
        LogUtils::error("MQTT connection failed: {}", e.what());
        return false;
    }
}

bool PahoMqttClient::is_connected() const {
    return client_ && client_->is_connected();
}

void PahoMqttClient::disconnect() {
    if (client_ && is_connected()) {
        try {
            client_->disconnect()->wait();
        } catch (const mqtt::exception& e) {
            LogUtils::error("MQTT disconnect failed: {}", e.what());
        }
    }
}

bool PahoMqttClient::publish(const MqttInsertData& data) {
    const MqttMessageBatch& batch_msgs = data.data;
    auto it = batch_msgs.begin();

    while (it != batch_msgs.end()) {
        const auto& [topic, payload] = *it;

        auto pubmsg = mqtt::make_message(
            topic,
            payload.data(),
            payload.size(),
            format_.qos,
            format_.retain,
            default_props_
        );

        try {
            auto token = client_->publish(pubmsg);
            token_queue_.put(std::move(token));
            ++it;
        } catch (const mqtt::exception& e) {
            std::string msg = e.what();
            if (msg.find("No more messages can be buffered") != std::string::npos ||
                msg.find("MQTT error [-12]") != std::string::npos) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            } else {
                throw std::runtime_error(std::string("MQTT publish failed: ") + msg);
            }
        }
    }
    return true;
}

void PahoMqttClient::close() {
    if (closed_.exchange(true)) {
        return;
    }

    // stop token waiter after draining current tokens
    token_queue_.put(nullptr);
    if (token_wait_thread_.joinable()) {
        token_wait_thread_.join();
    }

    // disconnect
    disconnect();
}

// MqttClient implementation
MqttClient::MqttClient(const MqttConfig& config,
                       const DataFormat::MqttConfig& format,
                       size_t no) {
    client_ = std::make_unique<PahoMqttClient>(config, format, no);
}

MqttClient::~MqttClient() = default;

bool MqttClient::connect() {
    return client_->connect();
}

bool MqttClient::is_connected() const {
    return client_ && client_->is_connected();
}

void MqttClient::close() {
    if (client_) {
        client_->close();
    }
}

bool MqttClient::prepare(const std::string& context) {
    (void)context;
    return true;
}

bool MqttClient::execute(const MqttInsertData& data) {
    if (!is_connected()) {
        return false;
    }

    return client_->publish(data);
}