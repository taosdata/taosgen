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

#include "IngestionRuntime.hpp"
#include "IngestionCapacity.hpp"
#include "LogUtils.hpp"
#include "taosgen/ingress/ingestion.pb.h"
#include <algorithm>
#include <chrono>
#include <stdexcept>
#include <utility>

namespace {

constexpr size_t kMinRuntimeBlocks = 1;

size_t effective_writer_concurrency(const IngressConfig& config) {
    validate_ingress_capacity_config(config);
    return std::max<size_t>(config.concurrency, 1);
}

size_t effective_queue_capacity(const IngressConfig& config) {
    validate_ingress_capacity_config(config);
    return std::max<size_t>(config.queue_capacity, 1) * effective_writer_concurrency(config);
}

} // namespace

IngestionRuntime::IngestionRuntime(SchemaConfig schema,
                                   IngressConfig config,
                                   std::unique_ptr<IIngestionWriter> writer)
    : config_(std::move(config)),
      builder_(schema, config_),
      pool_(pool_block_count(config_),
            effective_ingress_max_tables_per_request(config_),
            effective_ingress_max_rows_per_table(config_),
            builder_.column_instances(),
            builder_.tag_instances()),
      writer_(std::move(writer)) {
    if (!writer_) {
        throw std::runtime_error("ingress runtime writer must not be null.");
    }

    const size_t worker_count = effective_writer_concurrency(config_);
    workers_.reserve(worker_count);
    try {
        for (size_t i = 0; i < worker_count; ++i) {
            workers_.emplace_back([this, i] {
                worker_loop(i);
            });
        }
    } catch (...) {
        stopping_.store(true);
        queue_not_empty_.notify_all();
        queue_not_full_.notify_all();
        for (auto& worker : workers_) {
            if (worker.joinable()) {
                worker.join();
            }
        }
        throw;
    }
}

IngestionRuntime::~IngestionRuntime() {
    stop();
}

IngestionSubmitResult IngestionRuntime::submit(IngestRequestData request) {
    if (stopping_.load()) {
        throw IngestionUnavailableError("ingress runtime is stopped.");
    }

    if (request.ack_mode == AckMode::AcceptOnly) {
        // Validation-only fast path: keep this branch free of MemoryBlock allocation
        // and tag cache mutation so accept_only does not perturb write-path state.
        const IngestValidationResult result = builder_.validate(request);
        accepted_rows_.fetch_add(result.row_count);
        return IngestionSubmitResult{
            true,
            IngestionAckStage::Accepted,
            result.row_count,
            result.table_count,
            queue_depth_.load(),
            "accepted"
        };
    }

    const auto timeout = std::chrono::milliseconds(std::max(config_.request_timeout_ms, 0));
    if (!reserve_queue_slot(timeout)) {
        throw IngestionQueueFullError("ingress runtime queue is full.");
    }

    size_t accepted_rows = 0;
    size_t accepted_tables = 0;

    try {
        IngestionMemoryBlockPtr block(builder_.build(std::move(request), pool_));
        auto task = std::make_unique<Task>();
        task->accepted_rows = block->total_rows;
        task->accepted_tables = block->used_tables;
        task->block = std::move(block);
        task->session_id = std::move(request.session_id);
        task->request_id = std::move(request.request_id);

        accepted_rows = task->accepted_rows;
        accepted_tables = task->accepted_tables;
        enqueue_reserved(std::move(task));
    } catch (...) {
        release_reserved_queue_slot();
        throw;
    }

    accepted_rows_.fetch_add(accepted_rows);

    IngestionSubmitResult enqueued_result{
        true,
        IngestionAckStage::Enqueued,
        accepted_rows,
        accepted_tables,
        queue_depth_.load(),
        "enqueued"
    };

    return enqueued_result;
}

IngestionSubmitResult IngestionRuntime::submit_proto(const taosgen::ingress::IngestRequest& request,
                                                     AckMode ack_mode) {
    if (stopping_.load()) {
        throw IngestionUnavailableError("ingress runtime is stopped.");
    }

    if (ack_mode == AckMode::AcceptOnly) {
        // Validation-only fast path: keep this branch free of MemoryBlock allocation
        // and tag cache mutation so accept_only does not perturb write-path state.
        const IngestValidationResult result = builder_.validate_proto(request);
        const size_t accepted_rows = result.row_count;
        const size_t accepted_tables = result.table_count;
        accepted_rows_.fetch_add(accepted_rows);
        return IngestionSubmitResult{
            true,
            IngestionAckStage::Accepted,
            accepted_rows,
            accepted_tables,
            queue_depth_.load(),
            "accepted"
        };
    }

    const auto timeout = std::chrono::milliseconds(std::max(config_.request_timeout_ms, 0));
    if (!reserve_queue_slot(timeout)) {
        throw IngestionQueueFullError("ingress runtime queue is full.");
    }

    size_t accepted_rows = 0;
    size_t accepted_tables = 0;

    try {
        IngestionMemoryBlockPtr block(builder_.build_proto(request, pool_));
        auto task = std::make_unique<Task>();
        task->accepted_rows = block->total_rows;
        task->accepted_tables = block->used_tables;
        task->block = std::move(block);
        task->session_id = request.session_id();
        task->request_id = request.request_id();

        accepted_rows = task->accepted_rows;
        accepted_tables = task->accepted_tables;
        enqueue_reserved(std::move(task));
    } catch (...) {
        release_reserved_queue_slot();
        throw;
    }

    accepted_rows_.fetch_add(accepted_rows);

    return IngestionSubmitResult{
        true,
        IngestionAckStage::Enqueued,
        accepted_rows,
        accepted_tables,
        queue_depth_.load(),
        "enqueued"
    };
}

IngestionRuntimeStats IngestionRuntime::stats() const {
    return IngestionRuntimeStats{
        accepted_rows_.load(),
        written_rows_.load(),
        failed_rows_.load(),
        queue_depth_.load(),
        inflight_requests_.load()
    };
}

void IngestionRuntime::stop() {
    std::call_once(stop_once_, [this] {
        {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            stopping_.store(true);
        }

        queue_not_empty_.notify_all();
        queue_not_full_.notify_all();

        drain_queued_tasks();

        // Shutdown is intentionally abort-style: stop accepting new work, wait for
        // workers to observe the stop request, then release whatever remains queued.
        // Ingress does not promise drain-on-stop.
        for (auto& worker : workers_) {
            if (worker.joinable()) {
                worker.join();
            }
        }

        while (true) {
            std::unique_ptr<Task> task;
            {
                std::lock_guard<std::mutex> lock(queue_mutex_);
                if (queue_.empty()) {
                    break;
                }
                task = std::move(queue_.front());
                queue_.pop();
            }

            if (task) {
                failed_rows_.fetch_add(task->accepted_rows);
            }
        }
        queue_depth_.store(0);
        queue_not_full_.notify_all();
    });
}

void IngestionRuntime::drain_queued_tasks() {
    while (true) {
        std::unique_ptr<Task> task;
        {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            if (queue_.empty()) {
                break;
            }
            task = std::move(queue_.front());
            queue_.pop();
            queue_depth_.fetch_sub(1);
        }

        if (task) {
            failed_rows_.fetch_add(task->accepted_rows);
        }
    }

    queue_not_full_.notify_all();
}

size_t IngestionRuntime::pool_block_count(const IngressConfig& config) {
    return std::max(effective_queue_capacity(config), kMinRuntimeBlocks);
}

bool IngestionRuntime::reserve_queue_slot(std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(queue_mutex_);
    const auto has_capacity = [this] {
        return stopping_.load() ||
               queue_.size() + reserved_queue_slots_ + inflight_requests_.load() < effective_queue_capacity(config_);
    };

    if (!queue_not_full_.wait_for(lock, timeout, has_capacity)) {
        return false;
    }
    if (stopping_.load()) {
        throw IngestionUnavailableError("ingress runtime is stopped.");
    }

    ++reserved_queue_slots_;
    return true;
}

void IngestionRuntime::release_reserved_queue_slot() {
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        if (reserved_queue_slots_ > 0) {
            --reserved_queue_slots_;
        }
    }
    queue_not_full_.notify_one();
}

void IngestionRuntime::enqueue_reserved(std::unique_ptr<Task> task) {
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        if (reserved_queue_slots_ == 0) {
            throw std::runtime_error("ingress runtime queue slot was not reserved.");
        }
        if (stopping_.load()) {
            throw IngestionUnavailableError("ingress runtime is stopped.");
        }

        queue_.push(std::move(task));
        --reserved_queue_slots_;
        queue_depth_.fetch_add(1);
    }
    queue_not_empty_.notify_one();
}

std::unique_ptr<IngestionRuntime::Task> IngestionRuntime::dequeue() {
    std::unique_lock<std::mutex> lock(queue_mutex_);
    queue_not_empty_.wait(lock, [this] {
        return stopping_.load() || !queue_.empty();
    });

    if (stopping_.load() || queue_.empty()) {
        return nullptr;
    }

    auto task = std::move(queue_.front());
    queue_.pop();
    queue_depth_.fetch_sub(1);
    inflight_requests_.fetch_add(1);
    queue_not_full_.notify_one();
    return task;
}

void IngestionRuntime::worker_loop(size_t worker_index) {
    while (true) {
        auto task = dequeue();
        if (!task) {
            break;
        }

        try {
            if (!config_.dry_run) {
                IngestionWriteResult write_result = writer_->write(std::move(task->block), worker_index);
                if (write_result.success) {
                    written_rows_.fetch_add(write_result.written_rows);
                } else {
                    failed_rows_.fetch_add(task->accepted_rows);
                    LogUtils::error("ingress write failed: session_id={}, request_id={}, message={}",
                                    task->session_id,
                                    task->request_id,
                                    write_result.message);
                }
                if (write_result.fatal) {
                    stopping_.store(true);
                    queue_not_empty_.notify_all();
                    queue_not_full_.notify_all();
                    drain_queued_tasks();
                }
            } else {
                task->block.reset();
            }
        } catch (const std::exception& e) {
            failed_rows_.fetch_add(task->accepted_rows);
            LogUtils::error("ingress writer exception: session_id={}, request_id={}, message={}",
                            task->session_id,
                            task->request_id,
                            e.what());
        } catch (...) {
            failed_rows_.fetch_add(task->accepted_rows);
            LogUtils::error("ingress writer exception: session_id={}, request_id={}, message=unknown non-standard exception",
                            task->session_id,
                            task->request_id);
        }

        inflight_requests_.fetch_sub(1);
        queue_not_full_.notify_one();
    }
}
