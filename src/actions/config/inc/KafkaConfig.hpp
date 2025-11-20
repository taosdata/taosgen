#pragma once
#include <string>
#include <vector>
#include <optional>
#include <unordered_map>

struct KafkaConfig {
    std::string topic = "tsbench-topic";
    std::string bootstrap_servers = "localhost:9092";
    std::string client_id = "taosgen";

    std::string generate_client_id(size_t no = 0) const {
        return client_id + "-" + std::to_string(no);
    }

    // --- Generic librdkafka Options ---
    // Allows specifying any librdkafka-supported optional parameters.
    // Examples:
    // - security.protocol: "sasl_ssl"
    // - sasl.mechanism: "PLAIN"
    // - sasl.username: "user"
    // - sasl.password: "password"
    std::unordered_map<std::string, std::string> rdkafka_options;
};
