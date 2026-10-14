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
#undef NDEBUG
#include <cassert>
#include <atomic>
#include <chrono>
#include <iostream>
#include <thread>

namespace proto = taosgen::ingress;

namespace {

class FakeWriter : public IIngestionWriter {
public:
    IngestionWriteResult write(IngestionMemoryBlockPtr block, size_t) override {
        size_t rows = block ? block->total_rows : 0;
        write_calls.fetch_add(1);
        return IngestionWriteResult{true, rows, "written"};
    }

    std::atomic<size_t> write_calls{0};
};

SchemaConfig make_schema() {
    SchemaConfig schema;
    schema.name = "meters";
    schema.columns = {
        ColumnConfig("ts", "timestamp"),
        ColumnConfig("current", "double"),
        ColumnConfig("status_time", "timestamp"),
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
    config.rows_per_batch = 100;
    config.max_tables_per_request = 10;
    config.queue_capacity = 4;
    config.request_timeout_ms = 1000;
    config.concurrency = 1;
    config.default_ack_mode = AckMode::AcceptOnly;
    config.auto_create_table = true;
    return config;
}

proto::IngestRequest make_proto_request(proto::AckMode ack_mode) {
    proto::IngestRequest request;
    request.set_session_id("sess-1");
    request.set_request_id("req-1");
    request.set_ack_mode(ack_mode);

    proto::TableBatch* table = request.add_tables();
    table->set_table_name("d1001");
    table->mutable_timestamps()->add_values(1000);
    table->mutable_timestamps()->add_values(2000);

    proto::ColumnValues* current = table->add_columns();
    current->mutable_double_values()->add_values(12.3);
    current->mutable_double_values()->add_values(12.4);

    proto::ColumnValues* status_time = table->add_columns();
    status_time->mutable_timestamp_values()->add_string_values("2026-08-13 10:00:00.000");
    status_time->mutable_timestamp_values()->add_string_values("2026-08-13 10:00:00.001");

    proto::ColumnValues* status = table->add_columns();
    status->mutable_int_values()->add_values(1);
    status->mutable_int_values()->add_values(2);

    table->add_tags()->set_string_value("d1001");

    return request;
}

void test_service_ingest_write_complete() {
    auto writer = std::make_unique<FakeWriter>();
    auto* writer_ptr = writer.get();
    IngestionRuntime runtime(make_schema(), make_config(), std::move(writer));
    IngestionGrpcService service(runtime);

    proto::IngestRequest request = make_proto_request(proto::ACK_MODE_ENQUEUE_ONLY);
    proto::IngestResponse response;
    grpc::Status status = service.Ingest(nullptr, &request, &response);

    assert(status.ok());
    assert(response.code() == 0);
    assert(response.acked_stage() == proto::ACK_STAGE_ENQUEUED);
    assert(response.accepted_rows() == 2);

    for (int i = 0; i < 100 && writer_ptr->write_calls.load() == 0; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    assert(writer_ptr->write_calls.load() == 1);
    runtime.stop();

    std::cout << "test_service_ingest_write_complete passed.\n";
}

void test_service_health_and_stats() {
    auto writer = std::make_unique<FakeWriter>();
    IngestionRuntime runtime(make_schema(), make_config(), std::move(writer));
    IngestionGrpcService service(runtime);

    proto::IngestRequest request = make_proto_request(proto::ACK_MODE_ACCEPT_ONLY);
    proto::IngestResponse response;
    grpc::Status status = service.Ingest(nullptr, &request, &response);
    assert(status.ok());

    proto::HealthResponse health;
    status = service.Health(nullptr, nullptr, &health);
    assert(status.ok());
    assert(health.serving());

    proto::GetStatsResponse stats;
    status = service.GetStats(nullptr, nullptr, &stats);
    assert(status.ok());
    assert(stats.accepted_rows() == 2);
    assert(stats.written_rows() == 0);
    runtime.stop();

    std::cout << "test_service_health_and_stats passed.\n";
}

void test_service_rejects_invalid_request() {
    auto writer = std::make_unique<FakeWriter>();
    IngestionRuntime runtime(make_schema(), make_config(), std::move(writer));
    IngestionGrpcService service(runtime);

    proto::IngestRequest request = make_proto_request(proto::ACK_MODE_ACCEPT_ONLY);
    request.clear_session_id();

    proto::IngestResponse response;
    grpc::Status status = service.Ingest(nullptr, &request, &response);
    assert(!status.ok());
    assert(status.error_code() == grpc::StatusCode::INVALID_ARGUMENT);
    runtime.stop();

    std::cout << "test_service_rejects_invalid_request passed.\n";
}

void test_service_maps_resource_exhausted() {
    auto writer = std::make_unique<FakeWriter>();
    IngressConfig config = make_config();
    config.rows_per_batch = 1;
    config.max_tables_per_request = 1;
    IngestionRuntime runtime(make_schema(), config, std::move(writer));
    IngestionGrpcService service(runtime);

    proto::IngestRequest request = make_proto_request(proto::ACK_MODE_ENQUEUE_ONLY);
    proto::IngestResponse response;
    grpc::Status status = service.Ingest(nullptr, &request, &response);
    assert(!status.ok());
    assert(status.error_code() == grpc::StatusCode::RESOURCE_EXHAUSTED);
    runtime.stop();

    std::cout << "test_service_maps_resource_exhausted passed.\n";
}

} // namespace

int main() {
    test_service_ingest_write_complete();
    test_service_health_and_stats();
    test_service_rejects_invalid_request();
    test_service_maps_resource_exhausted();
    std::cout << "All IngestionGrpcService tests passed.\n";
    return 0;
}
