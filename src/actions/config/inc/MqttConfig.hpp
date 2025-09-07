#pragma once

#include <string>

struct MqttConfig {
    std::string uri = "tcp://localhost:1883";
    std::string user = "root";
    std::string password = "taosdata";
    std::string client_id= "taosgen";

    std::string topic = "taosgen/{table}";
    std::string compression = "";
    std::string encoding = "UTF-8";
    std::string timestamp_precision = "ms";
    size_t qos = 0;
    size_t keep_alive = 5;
    bool clean_session = true;
    bool retain = false;
    size_t max_buffered_messages = 10000;
    size_t batch_messages = 10000;

    std::string generate_client_id(size_t no = 0) const {
        return client_id + "-" + std::to_string(no);
    }

    MqttConfig() = default;

    MqttConfig(
        std::string uri_,
        std::string user_,
        std::string password_
    )
        : uri(std::move(uri_))
        , user(std::move(user_))
        , password(std::move(password_))
    {}

private:
    std::string topic_;
};