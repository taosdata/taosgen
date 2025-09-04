#pragma once

#include "BaseInsertData.hpp"
#include "MsgInsertData.hpp"
#include "InsertDataConfig.hpp"
#include "FormatResult.hpp"
#include "MqttConfig.hpp"
#include "Compressor.hpp"
#include "CompressionType.hpp"
#include "EncodingConverter.hpp"
#include "EncodingType.hpp"

#include <mqtt/async_client.h>
#include <mqtt/properties.h>
#include <nlohmann/json.hpp>
#include <memory>
#include <vector>
#include <functional>

// Forward declaration
namespace mqtt {
    class async_client;
    class message;
    class connect_options;
    class properties;
    using const_message_ptr = std::shared_ptr<const message>;
}

// MQTT client interface
class IMqttClient {
public:
    virtual ~IMqttClient() = default;

    virtual bool connect(const std::string& user, const std::string& password,
                       int keep_alive, bool clean_session) = 0;
    virtual bool is_connected() const = 0;
    virtual void disconnect() = 0;
    virtual void publish(const std::string& topic, const std::string& payload, int qos, bool retain) = 0;
    virtual void publish_batch(const MessageBatch& batch_msgs, int qos, bool retain) = 0;
};

// MQTT client implementation wrapper
class PahoMqttClient : public IMqttClient {
public:
    PahoMqttClient(const std::string& host, int port, const std::string& client_id, size_t max_buffered_messages,
        const std::string& content_type, const std::string& compression, const std::string& encoding);

    ~PahoMqttClient();

    bool connect(const std::string& user, const std::string& password,
                int keep_alive, bool clean_session) override;
    bool is_connected() const override;
    void disconnect() override;
    void publish(const std::string& topic, const std::string& payload, int qos, bool retain) override;
    void publish_batch(const MessageBatch& batch_msgs, int qos, bool retain) override;

private:
    std::unique_ptr<mqtt::async_client> client_;
    mqtt::properties default_props_;
};

class MqttClient {
public:
    MqttClient(const MqttConfig& config, const ColumnConfigInstanceVector& col_instances, size_t no = 0);
    ~MqttClient();

    // Connect to MQTT broker
    bool connect();
    bool is_connected() const;
    void close();
    bool select_db(const std::string& db_name);
    bool prepare(const std::string& sql);
    bool execute(const MsgInsertData& data);

    void set_client(std::unique_ptr<IMqttClient> client) {
        client_ = std::move(client);
    }

    // Serialize a row of data to JSON
    nlohmann::ordered_json serialize_row_to_json(
        const MemoryPool::TableBlock& table,
        size_t row_index
    ) const;

    // Handle message publishing
    void publish_message(
        const std::string& topic,
        const nlohmann::ordered_json& json_data
    );

private:
    const MqttConfig& config_;
    const ColumnConfigInstanceVector& col_instances_;
    CompressionType compression_type_;
    EncodingType encoding_type_;

    std::string compression_str_;
    std::string encoding_str_;

    std::unique_ptr<IMqttClient> client_;

    std::string current_db_;
    std::string prepare_sql_;
};