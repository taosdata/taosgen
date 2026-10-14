#pragma once

/*
 * Copyright (c) 2025 TAOS Data, Inc.
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

#include "KafkaConfig.hpp"
#include "KafkaFormatOptions.hpp"
#include "KafkaInsertData.hpp"
#include "ColumnConfigInstance.hpp"
#include <memory>

// Forward declaration
namespace RdKafka {
    class Producer;
    class Conf;
}

// Kafka client interface
class IKafkaClient {
public:
    virtual ~IKafkaClient() = default;
    virtual bool connect() = 0;
    virtual bool is_connected() const = 0;
    virtual void close() = 0;
    virtual bool produce(const KafkaMessageBatch& msgs) = 0;
};

// librdkafka client implementation
class RdKafkaClient : public IKafkaClient {
public:
    explicit RdKafkaClient(const KafkaConfig& config, const KafkaFormatOptions& format, size_t no = 0);
    ~RdKafkaClient() override;

    bool connect() override;
    bool is_connected() const override;
    void close() override;
    bool produce(const KafkaMessageBatch& msgs) override;

private:
    const KafkaConfig& config_;
    const KafkaFormatOptions& format_;
    size_t no_;
    std::unique_ptr<RdKafka::Producer> producer_;
    bool is_connected_ = false;
};


class KafkaClient {
public:
    KafkaClient(const KafkaConfig& config, const KafkaFormatOptions& format, size_t no = 0);
    ~KafkaClient();

    bool connect();
    bool is_connected() const;
    void close();
    bool execute(const KafkaInsertData& data);

    void set_client(std::unique_ptr<IKafkaClient> client) {
        client_ = std::move(client);
    }

private:
    std::unique_ptr<IKafkaClient> client_;
};