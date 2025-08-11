#pragma once

#include <string>
#include <optional>


struct ConnectionInfo {
    std::string host = "localhost";
    int port = 6030;
    std::string user = "root";
    std::string password = "taosdata";
    std::optional<std::string> dsn;

    struct ConnectionPoolConfig {
        bool enabled = true;
        size_t max_pool_size = 100;
        size_t min_pool_size = 2;
        size_t connection_timeout = 1000;       // unit: ms
    } pool_config;

    static int default_port(const std::string& channel_type);

    ConnectionInfo() = default;
    ConnectionInfo(
        std::string host_,
        int port_,
        std::string user_,
        std::string password_
    );

    /**
     * Parse DSN string and fill host/port/user/password fields
     * @param input_dsn Input DSN string
     * @throws std::runtime_error If parsing fails
     */
    void parse_dsn(const std::string& input_dsn);
};
