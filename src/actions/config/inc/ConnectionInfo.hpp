#pragma once

#include <string>
#include <optional>


struct ConnectionInfo {
    std::string host = "localhost";
    int port = 6030;
    std::string user = "root";
    std::string password = "taosdata";
    std::optional<std::string> dsn;

    static int default_port(const std::string& channel_type) {
        if (channel_type == "websocket") {
            return 6041;
        }

        return 6030;
    }

    /**
     * Parse DSN string and fill host/port/user/password fields
     * @param input_dsn Input DSN string
     * @throws std::runtime_error If parsing fails
     */
    void parse_dsn(const std::string& input_dsn);
};
