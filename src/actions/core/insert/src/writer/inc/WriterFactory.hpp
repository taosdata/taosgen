#pragma once
#include "IWriter.hpp"
#include "TDengineWriter.hpp"
// #include "CsvWriter.hpp"
#include "MqttWriter.hpp"
#include <memory>
#include <stdexcept>

class WriterFactory {
public:
    static std::unique_ptr<IWriter> create(const InsertDataConfig& config, const ColumnConfigInstanceVector& col_instances, size_t no = 0, std::shared_ptr<ActionRegisterInfo> action_info = nullptr) {
        if (config.target.target_type == "tdengine") {
            return std::make_unique<TDengineWriter>(config, col_instances, no, action_info);
        } else if (config.target.target_type == "csv") {
            // return std::make_unique<CsvWriter>(config);
            throw std::invalid_argument("Unsupported target type: " + config.target.target_type);
        } else if (config.target.target_type == "mqtt") {
            return std::make_unique<MqttWriter>(config, col_instances, no);
        }

        throw std::invalid_argument("Unsupported target type: " + config.target.target_type);
    }
};