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

#include "IngestionServer.hpp"
#include "SignalManager.hpp"
#include <algorithm>
#include <chrono>
#include <grpcpp/grpcpp.h>
#include <limits>
#include <stdexcept>
#include <thread>
#include <utility>

namespace {

class UnavailableIngestionWriter : public IIngestionWriter {
public:
    IngestionWriteResult write(IngestionMemoryBlockPtr, size_t) override {
        return IngestionWriteResult{
            false,
            0,
            "ingress TDengine writer is not implemented yet"
        };
    }
};

std::unique_ptr<IIngestionWriter> make_unavailable_writer() {
    return std::unique_ptr<IIngestionWriter>(new UnavailableIngestionWriter());
}

std::string selected_listen_address(const std::string& configured_address, int selected_port) {
    if (selected_port <= 0) {
        return configured_address;
    }

    const std::string::size_type pos = configured_address.rfind(':');
    if (pos == std::string::npos) {
        return configured_address;
    }

    return configured_address.substr(0, pos + 1) + std::to_string(selected_port);
}

int concurrency_to_grpc_option(size_t concurrency) {
    const size_t capped = std::min(concurrency, static_cast<size_t>(std::numeric_limits<int>::max()));
    return static_cast<int>(std::max<size_t>(capped, 1));
}

int message_size_to_grpc_option(size_t message_size) {
    const size_t capped = std::min(message_size, static_cast<size_t>(std::numeric_limits<int>::max()));
    return static_cast<int>(capped);
}

} // namespace

IngestionServer::IngestionServer(SchemaConfig schema, IngressConfig config)
    : IngestionServer(std::move(schema), std::move(config), make_unavailable_writer()) {}

IngestionServer::IngestionServer(SchemaConfig schema,
                                 IngressConfig config,
                                 std::unique_ptr<IIngestionWriter> writer)
    : config_(std::move(config)),
      runtime_(std::move(schema), config_, std::move(writer)),
      service_(runtime_),
      listen_address_(config_.listen) {}

IngestionServer::~IngestionServer() {
    shutdown();
}

void IngestionServer::start() {
    bool expected = false;
    if (!serving_.compare_exchange_strong(expected, true)) {
        throw std::runtime_error("ingress server is already serving.");
    }

    try {
        grpc::ServerBuilder builder;
        if (config_.service_concurrency > 0) {
            const int concurrency = concurrency_to_grpc_option(config_.service_concurrency);
            builder.SetSyncServerOption(grpc::ServerBuilder::SyncServerOption::NUM_CQS, concurrency);
            builder.SetSyncServerOption(grpc::ServerBuilder::SyncServerOption::MIN_POLLERS, concurrency);
            builder.SetSyncServerOption(grpc::ServerBuilder::SyncServerOption::MAX_POLLERS, concurrency);
        }
        const int max_message_bytes = message_size_to_grpc_option(config_.max_message_bytes);
        builder.SetMaxReceiveMessageSize(max_message_bytes);
        builder.SetMaxSendMessageSize(max_message_bytes);
        builder.RegisterService(&service_);

        int selected_port = 0;
        builder.AddListeningPort(config_.listen, grpc::InsecureServerCredentials(), &selected_port);
        server_ = builder.BuildAndStart();
        if (!server_) {
            throw std::runtime_error("failed to start ingress gRPC server on " + config_.listen + ".");
        }

        listen_address_ = selected_listen_address(config_.listen, selected_port);
    } catch (...) {
        server_.reset();
        serving_.store(false);
        throw;
    }
}

void IngestionServer::wait_until_interrupted() {
    while (serving_.load() && !SignalManager::interrupted()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    shutdown();
}

void IngestionServer::shutdown() {
    const bool was_serving = serving_.exchange(false);
    runtime_.stop();
    if (was_serving) {
        if (server_) {
            server_->Shutdown();
            server_->Wait();
            server_.reset();
        }
    }
}
