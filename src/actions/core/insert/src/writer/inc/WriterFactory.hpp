#pragma once
#include "IWriter.hpp"
#include "TDengineWriter.hpp"
// #include "CsvWriter.hpp"
#include "MqttWriter.hpp"
#include "KafkaWriter.hpp"
#include <memory>
#include <stdexcept>

class WriterFactory {
public:
    static std::unique_ptr<IWriter> create(const InsertDataConfig& config,
                                           size_t no = 0,
                                           std::shared_ptr<ActionRegisterInfo> action_info = nullptr) {

        if (config.target_type == "tdengine") {
            return std::make_unique<TDengineWriter>(config, no, action_info);
        } else if (config.target_type == "csv") {
            // return std::make_unique<CsvWriter>(config);
            throw std::invalid_argument("Unsupported target type: " + config.target_type);
        } else if (config.target_type == "mqtt") {
            return std::make_unique<MqttWriter>(config, no);
        } else if (config.target_type == "kafka") {
            return std::make_unique<KafkaWriter>(config, no);
        }

        throw std::invalid_argument("Unsupported target type: " + config.target_type);
    }
};