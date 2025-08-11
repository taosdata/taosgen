#pragma once
#include "IWriter.hpp"
#include "TDengineWriter.hpp"
// #include "FileSystemWriter.hpp"
#include <memory>
#include <stdexcept>

class WriterFactory {
public:
    static std::unique_ptr<IWriter> create(const InsertDataConfig& config) {
        if (config.target.target_type == "tdengine") {
            return std::make_unique<TDengineWriter>(config);
        } else if (config.target.target_type == "file_system") {
            // return std::make_unique<FileSystemWriter>(config);
            throw std::invalid_argument("Unsupported target type: " + config.target.target_type);
        }

        throw std::invalid_argument("Unsupported target type: " + config.target.target_type);
    }
};