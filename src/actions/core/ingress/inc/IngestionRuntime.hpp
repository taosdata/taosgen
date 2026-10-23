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

#include "IngestionErrors.hpp"
#include "IngestionMemoryBlockBuilder.hpp"
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>

namespace taosgen::ingress {
class IngestRequest;
} // namespace taosgen::ingress

enum class IngestionAckStage {
    Accepted,
    Enqueued,
    Failed,
};

struct IngestionSubmitResult {
    bool success = false;
    IngestionAckStage acked_stage = IngestionAckStage::Failed;
    size_t accepted_rows = 0;
    size_t accepted_tables = 0;
    size_t queue_depth = 0;
    std::string message;
};

struct IngestionRuntimeStats {
    size_t accepted_rows = 0;
    size_t written_rows = 0;
    size_t failed_rows = 0;
    size_t queue_depth = 0;
    size_t inflight_requests = 0;
};

struct IngestionWriteResult {
    bool success = false;
    size_t written_rows = 0;
    std::string message;
    bool fatal = false;
};

struct IngestionMemoryBlockDeleter {
    void operator()(MemoryPool::MemoryBlock* block) const noexcept {
        if (block) {
            block->release();
        }
    }
};

using IngestionMemoryBlockPtr = std::unique_ptr<MemoryPool::MemoryBlock, IngestionMemoryBlockDeleter>;

class IIngestionWriter {
public:
    virtual ~IIngestionWriter() = default;

    // The writer receives ownership of the block and must not manually release it.
    // The block is released automatically when the handle leaves scope.
    virtual IngestionWriteResult write(IngestionMemoryBlockPtr block, size_t worker_index) = 0;
};

class IngestionRuntime {
public:
    IngestionRuntime(SchemaConfig schema,
                     IngressConfig config,
                     std::unique_ptr<IIngestionWriter> writer);
    ~IngestionRuntime();

    IngestionRuntime(const IngestionRuntime&) = delete;
    IngestionRuntime& operator=(const IngestionRuntime&) = delete;

    // Direct/internal API retained for non-gRPC callers and tests. The gRPC
    // service submits protobuf requests through submit_proto().
    IngestionSubmitResult submit(IngestRequestData request);
    IngestionSubmitResult submit_proto(const taosgen::ingress::IngestRequest& request,
                                       AckMode ack_mode);
    IngestionRuntimeStats stats() const;
    void stop();
    bool stopped() const noexcept {
        return stopping_.load();
    }
    AckMode default_ack_mode() const noexcept {
        return config_.default_ack_mode;
    }

private:
    struct Task {
        IngestionMemoryBlockPtr block;
        size_t accepted_rows = 0;
        size_t accepted_tables = 0;
        std::string session_id;
        std::string request_id;
    };

    IngressConfig config_;
    IngestionMemoryBlockBuilder builder_;
    MemoryPool pool_;
    std::unique_ptr<IIngestionWriter> writer_;

    mutable std::mutex queue_mutex_;
    std::condition_variable queue_not_empty_;
    std::condition_variable queue_not_full_;
    std::queue<std::unique_ptr<Task>> queue_;
    std::vector<std::thread> workers_;
    size_t reserved_queue_slots_ = 0;
    std::atomic<bool> stopping_{false};
    std::once_flag stop_once_;
    std::atomic<size_t> queue_depth_{0};
    std::atomic<size_t> inflight_requests_{0};
    std::atomic<size_t> accepted_rows_{0};
    std::atomic<size_t> written_rows_{0};
    std::atomic<size_t> failed_rows_{0};

    static size_t pool_block_count(const IngressConfig& config);

    bool reserve_queue_slot(std::chrono::milliseconds timeout);
    void release_reserved_queue_slot();
    void enqueue_reserved(std::unique_ptr<Task> task);
    void drain_queued_tasks();
    std::unique_ptr<Task> dequeue();
    void worker_loop(size_t worker_index);
};
