#pragma once

#include "BaseInsertData.hpp"
#include "StmtV2InsertData.hpp"
#include "InsertDataConfig.hpp"
#include "MqttInfo.hpp"
#include "TopicGenerator.hpp"
#include "Compressor.hpp"
#include "CompressionType.hpp"
#include "EncodingConverter.hpp"
#include "EncodingType.hpp"

// #include <mqtt/async_client.h>
// #include <mqtt/message.h>
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
    virtual void publish(const std::string& topic, const std::string& payload,
                        int qos, bool retain, const std::string& content_type,
                        const std::string& compression, const std::string& encoding) = 0;
};

// MQTT client implementation wrapper
class PahoMqttClient : public IMqttClient {
public:
    PahoMqttClient(const std::string& host, const std::string& client_id);
    ~PahoMqttClient();

    bool connect(const std::string& user, const std::string& password,
                int keep_alive, bool clean_session) override;
    bool is_connected() const override;
    void disconnect() override;
    void publish(const std::string& topic, const std::string& payload,
                int qos, bool retain, const std::string& content_type,
                const std::string& compression, const std::string& encoding) override;

private:
    std::unique_ptr<mqtt::async_client> client_;
};

class MqttClient {
public:
    MqttClient(const MqttInfo& config, const ColumnConfigInstanceVector& col_instances);
    ~MqttClient();

    // Connect to MQTT broker
    bool connect();
    bool is_connected() const;
    void close();
    bool select_db(const std::string& db_name);
    bool prepare(const std::string& sql);
    bool execute(const StmtV2InsertData& data);

    void set_client(std::unique_ptr<IMqttClient> client) {
        client_ = std::move(client);
    }

    // Serialize a row of data to JSON
    nlohmann::json serialize_row_to_json(
        const MemoryPool::TableBlock& table,
        size_t row_index
    ) const;

    // Handle message publishing
    void publish_message(
        const std::string& topic,
        const nlohmann::json& json_data
    );

private:
    const MqttInfo& config_;
    const ColumnConfigInstanceVector& col_instances_;
    CompressionType compression_type_;
    EncodingType encoding_type_;

    TopicGenerator topic_generator_;
    std::unique_ptr<IMqttClient> client_;

    std::string current_db_;
    std::string prepare_sql_;
};