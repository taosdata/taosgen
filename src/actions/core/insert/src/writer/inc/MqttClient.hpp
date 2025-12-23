#pragma once

#include "MqttConfig.hpp"
#include "MqttInsertData.hpp"

#include <mqtt/async_client.h>
#include <mqtt/properties.h>
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

    virtual bool connect() = 0;
    virtual bool is_connected() const = 0;
    virtual void disconnect() = 0;
    virtual bool publish(const MqttInsertData& data) = 0;
};

// MQTT client implementation wrapper
class PahoMqttClient : public IMqttClient {
public:
    PahoMqttClient(const MqttConfig& config, const DataFormat::MqttConfig& format, size_t no = 0);

    ~PahoMqttClient();

    bool connect() override;
    bool is_connected() const override;
    void disconnect() override;
    bool publish(const MqttInsertData& data) override;

private:
    const MqttConfig& config_;
    const DataFormat::MqttConfig& format_;
    size_t no_;
    std::unique_ptr<mqtt::async_client> client_;
    std::thread token_wait_thread_;
    mqtt::properties default_props_;
    mqtt::thread_queue<mqtt::delivery_token_ptr> token_queue_;

    void token_wait_func() {
        while (true) {
            mqtt::delivery_token_ptr token = token_queue_.get();
            if (!token)
                break;
            token->wait();
        }
    }
};

class MqttClient {
public:
    MqttClient(const MqttConfig& config, const DataFormat::MqttConfig& format, size_t no = 0);
    ~MqttClient();

    // Connect to MQTT broker
    bool connect();
    bool is_connected() const;
    void close();
    bool select_db(const std::string& db_name);
    bool prepare(const std::string& sql);
    bool execute(const MqttInsertData& data);

    void set_client(std::unique_ptr<IMqttClient> client) {
        client_ = std::move(client);
    }

private:
    std::unique_ptr<IMqttClient> client_;
};