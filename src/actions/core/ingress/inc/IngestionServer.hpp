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

#include "IngestionGrpcService.hpp"
#include <atomic>
#include <memory>
#include <string>

namespace grpc {
class Server;
}

class IngestionServer {
public:
    IngestionServer(SchemaConfig schema, IngressConfig config);
    IngestionServer(SchemaConfig schema,
                    IngressConfig config,
                    std::unique_ptr<IIngestionWriter> writer);
    ~IngestionServer();

    IngestionServer(const IngestionServer&) = delete;
    IngestionServer& operator=(const IngestionServer&) = delete;

    void start();
    void wait_until_interrupted();
    void shutdown();

    bool serving() const noexcept {
        return serving_.load();
    }

    std::string listen_address() const {
        return listen_address_;
    }

    IngestionRuntimeStats stats() const {
        return runtime_.stats();
    }

private:
    IngressConfig config_;
    IngestionRuntime runtime_;
    IngestionGrpcService service_;
    std::unique_ptr<grpc::Server> server_;
    std::atomic<bool> serving_{false};
    std::string listen_address_;
};
