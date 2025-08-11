#include "ConnectionInfo.hpp"

#include <vector>
#include <stdexcept>
#include <algorithm>
#include "StringUtils.hpp"

int ConnectionInfo::default_port(const std::string& channel_type) {
    if (channel_type == "websocket") {
        return 6041;
    }
    return 6030;
}

ConnectionInfo::ConnectionInfo(
    std::string host_,
    int port_,
    std::string user_,
    std::string password_
)
    : host(std::move(host_))
    , port(port_)
    , user(std::move(user_))
    , password(std::move(password_)) {}

/**
 * Parse DSN string and fill host/port/user/password fields
 */
void ConnectionInfo::parse_dsn(const std::string& input_dsn) {
    dsn = input_dsn;

    // 1. Check protocol "://"
    const size_t protocol_pos = dsn->find("://");
    if (protocol_pos == std::string::npos) {
        throw std::runtime_error("DSN invalid: missing '://' protocol separator");
    }

    // 2. Extract host:port part (after protocol)
    const std::string post_protocol = dsn->substr(protocol_pos + 3);
    const size_t path_start = post_protocol.find_first_of("?");
    const std::string host_port = post_protocol.substr(0, path_start);

    // 3. Parse host and port
    size_t colon_pos = host_port.find(':');
    host = host_port.substr(0, colon_pos);
    if (colon_pos != std::string::npos) {
        const std::string port_str = host_port.substr(colon_pos + 1);
        try {
            port = std::stoi(port_str);
            if (port <= 0 || port > 65535) {  // Validate port range
                throw std::runtime_error("Invalid port number: " + port_str);
            }
        } catch (const std::exception& e) {
            throw std::runtime_error("Port parse error: " + std::string(e.what()));
        }
    }  // If port is not specified, keep the original value

    // 4. Parse query parameters
    const size_t query_start = post_protocol.find('?', path_start);
    if (query_start != std::string::npos) {
        const std::string query = post_protocol.substr(query_start + 1);
        std::vector<std::string> pairs;
        size_t pos = 0;

        // Split key-value pairs
        while (pos < query.size()) {
            const size_t end = query.find('&', pos);
            if (end == std::string::npos) {
                pairs.push_back(query.substr(pos));
                break;
            } else {
                pairs.push_back(query.substr(pos, end - pos));
                pos = end + 1;
            }
        }

        // Process each key-value pair
        for (const auto& pair : pairs) {
            const size_t eq_pos = pair.find('=');
            if (eq_pos == std::string::npos) continue;  // Ignore invalid format

            const std::string key = StringUtils::to_lower(pair.substr(0, eq_pos));
            std::string value = StringUtils::to_lower(pair.substr(eq_pos + 1));

            // Map key to corresponding field
            user = key;
            password = value;
        }
    }
}