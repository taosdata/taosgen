#pragma once

#include <string>
#include <optional>
#include <random>
#include <algorithm>

struct MqttInfo {
    std::string host = "localhost";
    int port = 1883;
    std::string user = "root";
    std::string password = "taosdata";
    std::string client_id;
    std::string topic;
    std::string compression = "";
    std::string encoding = "UTF-8";
    std::string timestamp_precision = "ms";
    size_t qos = 0;
    size_t keep_alive = 5;
    bool clean_session = true;
    bool retain = false;

    static constexpr int CLIENT_ID_SUFFIX_LEN = 10;
    static constexpr const char* CLIENT_ID_PREFIX = "taosgen-publisher-";
    static constexpr const char* CLIENT_ID_CHARSET = "0123456789abcdefghijklmnopqrstuvwxyz";
    static constexpr int CLIENT_ID_CHARSET_LEN = static_cast<int>(std::char_traits<char>::length(CLIENT_ID_CHARSET));

    static std::string generate_client_id() {
        static thread_local std::mt19937 gen{std::random_device{}()};
        std::uniform_int_distribution<> dist(0, CLIENT_ID_CHARSET_LEN - 1);
        std::string suffix(CLIENT_ID_SUFFIX_LEN, 'a');
        std::generate_n(suffix.begin(), CLIENT_ID_SUFFIX_LEN, [&]() { return CLIENT_ID_CHARSET[dist(gen)]; });
        return std::string(CLIENT_ID_PREFIX) + suffix;
    }

    MqttInfo()
        : client_id(generate_client_id())
    {}

    MqttInfo(
        std::string host_,
        int port_,
        std::string user_,
        std::string password_
    )
        : host(std::move(host_))
        , port(port_)
        , user(std::move(user_))
        , password(std::move(password_))
        , client_id(generate_client_id())
    {}

private:
    std::string topic_;
};