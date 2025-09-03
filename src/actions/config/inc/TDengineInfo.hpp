#pragma once

#include <string>
#include <optional>

struct TDengineInfo {
    std::string dsn = "taos+ws://root:taosdata@localhost:6041/tsbench";
    std::string host = "localhost";
    int port = 6041;
    std::string user = "root";
    std::string password = "taosdata";
    std::string database = "tsbench";
    bool drop_if_exists = true;
    std::optional<std::string> properties;

    struct ConnectionPool {
        bool enabled = true;
        size_t max_size = 100;
        size_t min_size = 2;
        size_t timeout = 1000;       // unit: ms
    } pool;

    enum class ProtocolType {
        Native,
        WebSocket,
        RESTful
    } protocol_type = ProtocolType::WebSocket;

    TDengineInfo();
    TDengineInfo(std::string input_dsn);

    void init();
    void parse_dsn();
};
