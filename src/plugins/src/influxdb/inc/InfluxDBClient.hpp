#pragma once

#include "InfluxDBConfig.hpp"
#include "InfluxDBFormatOptions.hpp"
#include "InfluxDBInsertData.hpp"
#include <string>
#include <memory>

// Forward declare CURL handle
typedef void CURL;

class InfluxDBClient {
public:
    InfluxDBClient(const InfluxDBConfig& config, const InfluxDBFormatOptions& format_options);
    ~InfluxDBClient();

    bool connect();
    bool is_connected() const;
    void close();
    bool execute(const InfluxDBInsertData& data);

private:
    std::string build_write_url() const;
    std::string build_auth_header() const;

    const InfluxDBConfig& config_;
    const InfluxDBFormatOptions& format_options_;
    CURL* curl_ = nullptr;
    bool is_connected_ = false;
    std::string write_url_;
    std::string auth_header_;
};
