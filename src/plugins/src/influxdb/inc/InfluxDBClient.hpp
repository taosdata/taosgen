#pragma once

/*
 * Copyright (c) 2026 TAOS Data, Inc.
 *
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#include "InfluxDBConfig.hpp"
#include "InfluxDBFormatOptions.hpp"
#include "InfluxDBInsertData.hpp"
#include <curl/curl.h>
#include <string>
#include <memory>

class IInfluxDBClient {
public:
    virtual ~IInfluxDBClient() = default;
    virtual bool connect() = 0;
    virtual bool is_connected() const = 0;
    virtual void close() = 0;
    virtual bool execute(const InfluxDBInsertData& data) = 0;
};

class CurlInfluxDBClient : public IInfluxDBClient {
public:
    CurlInfluxDBClient(const InfluxDBConfig& config, const InfluxDBFormatOptions& format_options);
    ~CurlInfluxDBClient() override;

    bool connect() override;
    bool is_connected() const override;
    void close() override;
    bool execute(const InfluxDBInsertData& data) override;

    const std::string& write_url() const { return write_url_; }

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

class InfluxDBClient {
public:
    InfluxDBClient(const InfluxDBConfig& config, const InfluxDBFormatOptions& format_options);
    ~InfluxDBClient();

    bool connect();
    bool is_connected() const;
    void close();
    bool execute(const InfluxDBInsertData& data);

    void set_client(std::unique_ptr<IInfluxDBClient> client) {
        client_ = std::move(client);
    }

private:
    std::unique_ptr<IInfluxDBClient> client_;
};
