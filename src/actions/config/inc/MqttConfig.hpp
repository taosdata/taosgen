#pragma once

#include <string>

struct MqttConfig {
    std::string uri = "tcp://localhost:1883";
    std::string user = "root";
    std::string password = "taosdata";
    std::string client_id = "taosgen";

    bool clean_session = true;
    size_t keep_alive = 5;
    size_t max_buffered_messages = 10000;

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