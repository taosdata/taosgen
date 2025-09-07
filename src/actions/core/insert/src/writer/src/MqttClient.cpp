#include "MqttClient.hpp"
#include <sstream>
#include <iomanip>

#include <mqtt/async_client.h>
#include <mqtt/message.h>
#include <mqtt/connect_options.h>
#include <mqtt/properties.h>

PahoMqttClient::PahoMqttClient(const std::string& uri, const std::string& client_id, size_t max_buffered_messages,
                               const std::string& content_type,
                               const std::string& compression,
                               const std::string& encoding) {
    // Example: "tcp://localhost:1883"
    mqtt::create_options create_opts;
    create_opts.set_server_uri(uri);
    create_opts.set_client_id(client_id);
    create_opts.set_max_buffered_messages(static_cast<int>(max_buffered_messages));

    client_ = std::make_unique<mqtt::async_client>(create_opts);

    default_props_.add(mqtt::property(mqtt::property::USER_PROPERTY, "content-type", content_type));
    if (!compression.empty()) {
        default_props_.add(mqtt::property(mqtt::property::USER_PROPERTY, "compression", compression));
    }
    if (!encoding.empty()) {
        default_props_.add(mqtt::property(mqtt::property::USER_PROPERTY, "encoding", encoding));
    }
}

PahoMqttClient::~PahoMqttClient() {
    disconnect();
}

bool PahoMqttClient::connect(const std::string& user, const std::string& password,
                            int keep_alive, bool clean_session) {
    mqtt::connect_options conn_opts;
    conn_opts.set_user_name(user);
    conn_opts.set_password(password);
    conn_opts.set_keep_alive_interval(keep_alive);
    conn_opts.set_clean_session(clean_session);
    conn_opts.set_automatic_reconnect(true);

    try {
        client_->connect(conn_opts)->wait();
        return true;
    } catch (const mqtt::exception& e) {
        std::cerr << "MQTT connection failed: " << e.what() << std::endl;
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
            std::cerr << "MQTT disconnect failed: " << e.what() << std::endl;
        }
    }
}

void PahoMqttClient::publish(const std::string& topic, const std::string& payload, int qos, bool retain) {
    // Create MQTT message
    auto pubmsg = mqtt::make_message(
        topic,
        payload.data(),
        payload.size(),
        qos,
        retain,
        default_props_
    );

    // Asynchronous publish with retry on buffer full
    while (true) {
        try {
            auto token = client_->publish(pubmsg);
            // token->wait();
            break;

        } catch (const mqtt::exception& e) {
            std::string msg = e.what();
            // std::cerr << "MQTT publish error: " << msg << std::endl;
            // std::cerr << "Reason code: " << e.get_reason_code() << std::endl;

            if (msg.find("No more messages can be buffered") != std::string::npos ||
                msg.find("MQTT error [-12]") != std::string::npos) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                continue;
            } else {
                throw std::runtime_error(std::string("MQTT publish failed: ") + msg);
            }
        }
    }
}

void PahoMqttClient::publish_batch(const MessageBatch& batch_msgs, int qos, bool retain) {
    // std::vector<mqtt::delivery_token_ptr> tokens;
    for (const auto& [topic, payload] : batch_msgs) {
        auto pubmsg = mqtt::make_message(
            topic,
            payload.data(),
            payload.size(),
            qos,
            retain,
            default_props_
        );
        auto token = client_->publish(pubmsg);
        token->wait();
        // tokens.push_back(client_->publish(pubmsg));
    }
    // for (auto& tok : tokens) {
    //     tok->wait();
    // }
}

// MqttClient implementation
MqttClient::MqttClient(const MqttConfig& config,
                       const ColumnConfigInstanceVector& col_instances, size_t no)
    : config_(config),
      col_instances_(col_instances),
      compression_type_(string_to_compression(config.compression)),
      encoding_type_(string_to_encoding(config.encoding))
{
    std::string client_id = config.generate_client_id(no);

    compression_str_ = (compression_type_ != CompressionType::NONE)
        ? compression_to_string(compression_type_) : "";
    encoding_str_ = (encoding_type_ != EncodingType::UTF8)
        ? encoding_to_string(encoding_type_) : "";

    // Initialize MQTT client
    client_ = std::make_unique<PahoMqttClient>(config.uri, client_id, config.max_buffered_messages,
        "application/json", compression_str_, encoding_str_);
}

MqttClient::~MqttClient() = default;

bool MqttClient::connect() {
    return client_->connect(config_.user, config_.password,
                          config_.keep_alive, config_.clean_session);
}

bool MqttClient::is_connected() const {
    return client_ && client_->is_connected();
}

void MqttClient::close() {
    if (client_) {
        client_->disconnect();
    }
}

bool MqttClient::select_db(const std::string& db_name) {
    current_db_ = db_name;
    return true;
}

bool MqttClient::prepare(const std::string& sql) {
    prepare_sql_ = sql;
    return true;
}

bool MqttClient::execute(const MsgInsertData& data) {
    if (!is_connected()) {
        return false;
    }

    const auto& msg_batches = data.data;
    for (const auto& batch : msg_batches) {
        client_->publish_batch(batch, config_.qos, config_.retain);
    }

    return true;
}

void MqttClient::publish_message(
    const std::string& topic,
    const nlohmann::ordered_json& json_data
) {
    // 1. Serialize to string
    std::string payload = json_data.dump();

    // 2. Encoding conversion
    if (encoding_type_ != EncodingType::UTF8) {
        payload = EncodingConverter::convert(
            payload,
            EncodingType::UTF8,
            encoding_type_
        );
    }

    // 3. Compression
    if (compression_type_ != CompressionType::NONE) {
        payload = Compressor::compress(payload, compression_type_);
    }

    // 4. Publish message
    client_->publish(topic, payload, config_.qos, config_.retain);
}