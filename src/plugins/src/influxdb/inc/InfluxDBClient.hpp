#pragma once

#include "InfluxDBConfig.hpp"
#include "InfluxDBFormatOptions.hpp"
#include "InfluxDBInsertData.hpp"
#include <curl/curl.h>
#include <string>
#include <memory>

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
    bool send_chunk(const char* data, size_t size);

    const InfluxDBConfig& config_;
    const InfluxDBFormatOptions& format_options_;
    CURL* curl_ = nullptr;
    bool is_connected_ = false;
    std::string write_url_;
    std::string auth_header_;
};
