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
#include <cassert>
#include <chrono>
#include <grpcpp/grpcpp.h>
#include <iostream>

namespace proto = taosgen::ingress;

namespace {

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
    config.listen = "127.0.0.1:0";
    config.service_concurrency = 1;
    config.rows_per_batch = 100;
    config.max_tables_per_request = 10;
    config.queue_capacity = 4;
    config.request_timeout_ms = 1000;
    config.concurrency = 1;
    config.default_ack_mode = AckMode::AcceptOnly;
    config.auto_create_table = true;
    return config;
}

proto::IngestRequest make_request(proto::AckMode ack_mode) {
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

    proto::ColumnValues* status = table->add_columns();
    status->mutable_int_values()->add_values(1);
    status->mutable_int_values()->add_values(2);

    table->add_tags()->set_string_value("d1001");
    return request;
}

void set_client_context_deadline(grpc::ClientContext& context) {
    const auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(5);
    context.set_deadline(deadline);
}

void test_server_accept_only_rpc() {
    IngestionServer server(make_schema(), make_config());
    server.start();
    assert(server.serving());

    auto channel = grpc::CreateChannel(server.listen_address(), grpc::InsecureChannelCredentials());
    auto stub = proto::IngestionService::NewStub(channel);

    proto::HealthRequest health_request;
    proto::HealthResponse health_response;
    grpc::ClientContext health_context;
    set_client_context_deadline(health_context);
    grpc::Status status = stub->Health(&health_context, health_request, &health_response);
    assert(status.ok());
    assert(health_response.serving());

    proto::IngestRequest request = make_request(proto::ACK_MODE_ACCEPT_ONLY);
    proto::IngestResponse response;
    grpc::ClientContext ingest_context;
    set_client_context_deadline(ingest_context);
    status = stub->Ingest(&ingest_context, request, &response);
    assert(status.ok());
    assert(response.code() == 0);
    assert(response.acked_stage() == proto::ACK_STAGE_ACCEPTED);
    assert(response.accepted_rows() == 2);

    proto::GetStatsRequest stats_request;
    proto::GetStatsResponse stats_response;
    grpc::ClientContext stats_context;
    set_client_context_deadline(stats_context);
    status = stub->GetStats(&stats_context, stats_request, &stats_response);
    assert(status.ok());
    assert(stats_response.accepted_rows() == 2);
    assert(stats_response.written_rows() == 0);

    server.shutdown();
    assert(!server.serving());

    std::cout << "test_server_accept_only_rpc passed.\n";
}

void test_server_enqueue_only_returns_enqueued() {
    IngressConfig config = make_config();
    config.default_ack_mode = AckMode::EnqueueOnly;
    IngestionServer server(make_schema(), config);
    server.start();

    auto channel = grpc::CreateChannel(server.listen_address(), grpc::InsecureChannelCredentials());
    auto stub = proto::IngestionService::NewStub(channel);

    proto::IngestRequest request = make_request(proto::ACK_MODE_ENQUEUE_ONLY);
    proto::IngestResponse response;
    grpc::ClientContext context;
    set_client_context_deadline(context);
    grpc::Status status = stub->Ingest(&context, request, &response);
    assert(status.ok());
    assert(response.code() == 0);
    assert(response.acked_stage() == proto::ACK_STAGE_ENQUEUED);
    assert(response.accepted_rows() == 2);

    server.shutdown();

    std::cout << "test_server_ack_mode_enqueue_only_returns_enqueued passed.\n";
}

} // namespace

int main() {
    test_server_accept_only_rpc();
    test_server_enqueue_only_returns_enqueued();
    std::cout << "All IngestionServer tests passed.\n";
    return 0;
}
