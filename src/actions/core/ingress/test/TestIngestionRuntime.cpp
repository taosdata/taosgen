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
#undef NDEBUG
#include <cassert>
#include <condition_variable>
#include <chrono>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>

namespace {

class FakeWriter : public IIngestionWriter {
public:
    explicit FakeWriter(bool fail = false) : fail_(fail) {}

    IngestionWriteResult write(IngestionMemoryBlockPtr block, size_t) override {
        write_calls.fetch_add(1);
        size_t rows = block ? block->total_rows : 0;
        if (fail_) {
            completed_calls.fetch_add(1);
            return IngestionWriteResult{false, 0, "fake failure"};
        }
        written_rows.fetch_add(rows);
        completed_calls.fetch_add(1);
        return IngestionWriteResult{true, rows, "fake written"};
    }

    std::atomic<size_t> write_calls{0};
    std::atomic<size_t> completed_calls{0};
    std::atomic<size_t> written_rows{0};

private:
    bool fail_ = false;
};

class BlockingWriter : public IIngestionWriter {
public:
    IngestionWriteResult write(IngestionMemoryBlockPtr block, size_t) override {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            entered_ = true;
            ++write_calls_;
        }
        entered_cv_.notify_all();

        {
            std::unique_lock<std::mutex> lock(mutex_);
            release_cv_.wait(lock, [this] {
                return release_;
            });
        }

        const size_t rows = block ? block->total_rows : 0;
        return IngestionWriteResult{true, rows, "blocking written"};
    }

    void wait_until_entered() {
        std::unique_lock<std::mutex> lock(mutex_);
        entered_cv_.wait(lock, [this] {
            return entered_;
        });
    }

    void release() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            release_ = true;
        }
        release_cv_.notify_all();
    }

    size_t write_calls() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return write_calls_;
    }

private:
    mutable std::mutex mutex_;
    std::condition_variable entered_cv_;
    std::condition_variable release_cv_;
    bool entered_ = false;
    bool release_ = false;
    size_t write_calls_ = 0;
};

class FatalBlockingWriter : public IIngestionWriter {
public:
    IngestionWriteResult write(IngestionMemoryBlockPtr block, size_t) override {
        std::unique_lock<std::mutex> lock(mutex_);
        ++write_calls_;
        if (write_calls_ > 1) {
            assert(false && "fatal shutdown should not dequeue queued tasks");
        }
        entered_ = true;
        entered_cv_.notify_all();
        release_cv_.wait(lock, [this] {
            return release_;
        });
        lock.unlock();

        const size_t rows = block ? block->total_rows : 0;
        return IngestionWriteResult{true, rows, "fatal written", true};
    }

    void wait_until_entered() {
        std::unique_lock<std::mutex> lock(mutex_);
        entered_cv_.wait(lock, [this] {
            return entered_;
        });
    }

    void release() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            release_ = true;
        }
        release_cv_.notify_all();
    }

    size_t write_calls() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return write_calls_;
    }

private:
    mutable std::mutex mutex_;
    std::condition_variable entered_cv_;
    std::condition_variable release_cv_;
    bool entered_ = false;
    bool release_ = false;
    size_t write_calls_ = 0;
};

SchemaConfig make_schema() {
    SchemaConfig schema;
    schema.name = "meters";
    schema.columns = {
        ColumnConfig("ts", "timestamp"),
        ColumnConfig("current", "double"),
        ColumnConfig("status", "int")
    };
    schema.tags = {
        ColumnConfig("device_id", "binary(64)")
    };
    schema.apply(std::string("ms"));
    return schema;
}

IngressConfig make_config() {
    IngressConfig config;
    config.rows_per_batch = 4;
    config.max_tables_per_request = 2;
    config.queue_capacity = 2;
    config.request_timeout_ms = 1000;
    config.concurrency = 1;
    return config;
}

IngestTableBatch make_table_batch(const std::string& table_name) {
    return IngestTableBatch{
        table_name,
        IngestTimestampValues{{1000, 2000}, {}},
        {
            IngestColumnValues{std::vector<double>{12.3, 12.4}, {}},
            IngestColumnValues{std::vector<int32_t>{1, 2}, {}}
        },
        RowType{}
    };
}

IngestRequestData make_request(AckMode mode, size_t table_count = 1) {
    IngestRequestData request;
    request.session_id = "sess-1";
    request.request_id = "req-1";
    request.ack_mode = mode;
    for (size_t i = 0; i < table_count; ++i) {
        request.tables.push_back(make_table_batch("d" + std::to_string(1001 + i)));
    }
    return request;
}

void test_enqueue_returns_before_write_complete() {
    auto writer = std::make_unique<BlockingWriter>();
    auto* writer_ptr = writer.get();
    IngestionRuntime runtime(make_schema(), make_config(), std::move(writer));

    const auto start = std::chrono::steady_clock::now();
    IngestionSubmitResult result = runtime.submit(make_request(AckMode::EnqueueOnly));
    const auto elapsed = std::chrono::steady_clock::now() - start;

    assert(result.success);
    assert(result.acked_stage == IngestionAckStage::Enqueued);
    assert(result.accepted_rows == 2);
    assert(elapsed < std::chrono::milliseconds(100));

    writer_ptr->wait_until_entered();
    assert(writer_ptr->write_calls() == 1);
    writer_ptr->release();
    runtime.stop();

    IngestionRuntimeStats stats = runtime.stats();
    assert(stats.accepted_rows == 2);
    assert(stats.written_rows == 2);

    std::cout << "test_enqueue_returns_before_write_complete passed.\n";
}

void test_accept_only_does_not_write() {
    auto writer = std::make_unique<FakeWriter>();
    auto* writer_ptr = writer.get();
    IngestionRuntime runtime(make_schema(), make_config(), std::move(writer));

    IngestionSubmitResult result = runtime.submit(make_request(AckMode::AcceptOnly));
    assert(result.success);
    assert(result.acked_stage == IngestionAckStage::Accepted);
    assert(result.accepted_rows == 2);
    assert(writer_ptr->write_calls == 0);
    runtime.stop();

    std::cout << "test_accept_only_does_not_write passed.\n";
}

void test_accept_only_does_not_commit_tags() {
    IngressConfig config = make_config();
    config.auto_create_table = true;

    auto writer = std::make_unique<FakeWriter>();
    auto* writer_ptr = writer.get();
    IngestionRuntime runtime(make_schema(), config, std::move(writer));

    IngestRequestData accept_request = make_request(AckMode::AcceptOnly);
    accept_request.tables[0].tags = RowType{std::string("device-a")};
    IngestionSubmitResult accepted = runtime.submit(accept_request);
    assert(accepted.success);
    assert(accepted.acked_stage == IngestionAckStage::Accepted);

    IngestRequestData enqueue_request = make_request(AckMode::EnqueueOnly);
    enqueue_request.tables[0].tags = RowType{std::string("device-b")};
    IngestionSubmitResult enqueued = runtime.submit(enqueue_request);
    assert(enqueued.success);
    assert(enqueued.acked_stage == IngestionAckStage::Enqueued);

    for (int i = 0; i < 100; ++i) {
        if (writer_ptr->write_calls == 1) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    assert(writer_ptr->write_calls == 1);
    runtime.stop();

    IngestionRuntimeStats stats = runtime.stats();
    assert(stats.accepted_rows == 4);
    assert(stats.written_rows == 2);

    std::cout << "test_accept_only_does_not_commit_tags passed.\n";
}

void test_accept_only_rejects_integer_tag_out_of_range() {
    SchemaConfig schema = make_schema();
    schema.tags = {
        ColumnConfig("device_id", "tinyint")
    };

    IngressConfig config = make_config();
    config.auto_create_table = true;

    auto writer = std::make_unique<FakeWriter>();
    auto* writer_ptr = writer.get();
    IngestionRuntime runtime(std::move(schema), config, std::move(writer));

    IngestRequestData request = make_request(AckMode::AcceptOnly);
    request.tables[0].tags = RowType{int32_t(1000)};

    bool threw = false;
    try {
        (void)runtime.submit(request);
    } catch (const std::runtime_error& e) {
        threw = true;
        assert(std::string(e.what()).find("value out of range") != std::string::npos);
    }
    assert(threw);
    assert(writer_ptr->write_calls == 0);
    runtime.stop();

    std::cout << "test_accept_only_rejects_integer_tag_out_of_range passed.\n";
}

void test_accept_only_accepts_varlen_tag_overflow() {
    IngressConfig config = make_config();
    config.auto_create_table = true;

    auto writer = std::make_unique<FakeWriter>();
    auto* writer_ptr = writer.get();
    IngestionRuntime runtime(make_schema(), config, std::move(writer));

    IngestRequestData request = make_request(AckMode::AcceptOnly);
    request.tables[0].tags = RowType{std::string(128, 'x')};

    IngestionSubmitResult result = runtime.submit(request);
    assert(result.success);
    assert(result.acked_stage == IngestionAckStage::Accepted);
    assert(result.accepted_rows == 2);
    assert(writer_ptr->write_calls == 0);
    runtime.stop();

    std::cout << "test_accept_only_accepts_varlen_tag_overflow passed.\n";
}

void test_dequeue_only_still_writes_async() {
    auto writer = std::make_unique<FakeWriter>();
    auto* writer_ptr = writer.get();
    IngestionRuntime runtime(make_schema(), make_config(), std::move(writer));

    IngestionSubmitResult result = runtime.submit(make_request(AckMode::EnqueueOnly));
    assert(result.success);
    assert(result.acked_stage == IngestionAckStage::Enqueued);
    assert(result.accepted_rows == 2);

    for (int i = 0; i < 100; ++i) {
        if (writer_ptr->write_calls == 1) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    assert(writer_ptr->write_calls == 1);
    runtime.stop();

    IngestionRuntimeStats stats = runtime.stats();
    assert(stats.written_rows == 2);

    std::cout << "test_dequeue_only_still_writes_async passed.\n";
}

void test_write_failure() {
    auto writer = std::make_unique<FakeWriter>(true);
    auto* writer_ptr = writer.get();
    IngestionRuntime runtime(make_schema(), make_config(), std::move(writer));

    IngestionSubmitResult result = runtime.submit(make_request(AckMode::EnqueueOnly));
    assert(result.success);
    assert(result.acked_stage == IngestionAckStage::Enqueued);
    assert(result.accepted_rows == 2);

    for (int i = 0; i < 100; ++i) {
        if (writer_ptr->completed_calls == 1) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    assert(writer_ptr->completed_calls == 1);
    runtime.stop();
    IngestionRuntimeStats stats = runtime.stats();
    assert(stats.failed_rows == 2);
    assert(stats.written_rows == 0);

    std::cout << "test_write_failure passed.\n";
}

void test_dry_run_skips_tdengine_write() {
    auto writer = std::make_unique<FakeWriter>();
    auto* writer_ptr = writer.get();
    IngressConfig config = make_config();
    config.dry_run = true;
    IngestionRuntime runtime(make_schema(), config, std::move(writer));

    IngestionSubmitResult result = runtime.submit(make_request(AckMode::EnqueueOnly));
    assert(result.success);
    assert(result.acked_stage == IngestionAckStage::Enqueued);

    for (int i = 0; i < 100; ++i) {
        IngestionRuntimeStats stats = runtime.stats();
        if (stats.queue_depth == 0 && stats.inflight_requests == 0) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    runtime.stop();

    IngestionRuntimeStats stats = runtime.stats();
    assert(writer_ptr->write_calls == 0);
    assert(stats.accepted_rows == 2);
    assert(stats.written_rows == 0);
    assert(stats.failed_rows == 0);

    std::cout << "test_dry_run_skips_tdengine_write passed.\n";
}

void test_queue_full_waits_until_timeout() {
    IngressConfig config = make_config();
    config.queue_capacity = 1;
    config.request_timeout_ms = 50;

    auto writer = std::make_unique<BlockingWriter>();
    auto* writer_ptr = writer.get();
    IngestionRuntime runtime(make_schema(), config, std::move(writer));

    std::thread first_submit([&runtime] {
        (void)runtime.submit(make_request(AckMode::EnqueueOnly));
    });

    writer_ptr->wait_until_entered();

    const auto start = std::chrono::steady_clock::now();
    bool threw = false;
    try {
        (void)runtime.submit(make_request(AckMode::EnqueueOnly));
    } catch (const IngestionQueueFullError&) {
        threw = true;
    }
    const auto elapsed = std::chrono::steady_clock::now() - start;
    assert(threw);
    assert(elapsed >= std::chrono::milliseconds(40));

    writer_ptr->release();
    first_submit.join();
    runtime.stop();

    std::cout << "test_queue_full_waits_until_timeout passed.\n";
}

void test_fatal_shutdown_keeps_join_and_drops_queued_tasks() {
    auto writer = std::make_unique<FatalBlockingWriter>();
    auto* writer_ptr = writer.get();
    IngestionRuntime runtime(make_schema(), make_config(), std::move(writer));

    IngestionSubmitResult first = runtime.submit(make_request(AckMode::EnqueueOnly));
    assert(first.success);
    assert(first.accepted_rows == 2);

    writer_ptr->wait_until_entered();

    IngestionSubmitResult second = runtime.submit(make_request(AckMode::EnqueueOnly));
    assert(second.success);
    assert(second.accepted_rows == 2);

    writer_ptr->release();

    for (int i = 0; i < 100; ++i) {
        if (runtime.stopped()) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    assert(runtime.stopped());

    IngestionRuntimeStats pre_stop_stats = runtime.stats();
    assert(pre_stop_stats.failed_rows == 2);
    assert(pre_stop_stats.queue_depth == 0);

    runtime.stop();

    IngestionRuntimeStats stats = runtime.stats();
    assert(writer_ptr->write_calls() == 1);
    assert(stats.accepted_rows == 4);
    assert(stats.written_rows == 2);
    assert(stats.failed_rows == 2);
    assert(stats.queue_depth == 0);

    std::cout << "test_fatal_shutdown_keeps_join_and_drops_queued_tasks passed.\n";
}

} // namespace

int main() {
    test_enqueue_returns_before_write_complete();
    test_accept_only_does_not_write();
    test_accept_only_does_not_commit_tags();
    test_accept_only_rejects_integer_tag_out_of_range();
    test_accept_only_accepts_varlen_tag_overflow();
    test_dequeue_only_still_writes_async();
    test_write_failure();
    test_dry_run_skips_tdengine_write();
    test_queue_full_waits_until_timeout();
    test_fatal_shutdown_keeps_join_and_drops_queued_tasks();
    std::cout << "All IngestionRuntime tests passed.\n";
    return 0;
}
