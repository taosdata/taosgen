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
#include <new>
#include <stdexcept>

namespace proto = taosgen::ingress;

namespace {

AckMode ack_mode_from_proto(proto::AckMode mode, AckMode default_ack_mode) {
    switch (mode) {
        case proto::ACK_MODE_UNSPECIFIED:
            return default_ack_mode;
        case proto::ACK_MODE_ACCEPT_ONLY:
            return AckMode::AcceptOnly;
        case proto::ACK_MODE_ENQUEUE_ONLY:
            return AckMode::EnqueueOnly;
        default:
            throw std::runtime_error("invalid ingress ack_mode.");
    }
}

void fill_response(const std::string& session_id,
                   const std::string& request_id,
                   AckMode ack_mode,
                   const IngestionSubmitResult& result,
                   proto::IngestResponse* response) {
    response->set_code(result.success ? 0 : 1);
    response->set_message(result.message);
    response->set_session_id(session_id);
    response->set_request_id(request_id);
    response->set_ack_mode(ack_mode_to_proto(ack_mode));
    response->set_acked_stage(ack_stage_to_proto(result.acked_stage));
    response->set_accepted_rows(result.accepted_rows);
    response->set_accepted_tables(result.accepted_tables);
    response->set_queue_depth(result.queue_depth);
}

} // namespace

IngestionGrpcService::IngestionGrpcService(IngestionRuntime& runtime)
    : runtime_(runtime) {}

grpc::Status IngestionGrpcService::Ingest(grpc::ServerContext* /*context*/,
                                          const proto::IngestRequest* request,
                                          proto::IngestResponse* response) {
    try {
        const AckMode effective_ack_mode = ack_mode_from_proto(request->ack_mode(), runtime_.default_ack_mode());
        IngestionSubmitResult result = runtime_.submit_proto(*request, effective_ack_mode);
        fill_response(request->session_id(), request->request_id(), effective_ack_mode, result, response);
        return grpc::Status::OK;
    } catch (const IngestionResourceExhaustedError& e) {
        return grpc::Status(grpc::StatusCode::RESOURCE_EXHAUSTED, e.what());
    } catch (const IngestionUnavailableError& e) {
        return grpc::Status(grpc::StatusCode::UNAVAILABLE, e.what());
    } catch (const std::bad_alloc&) {
        return grpc::Status(grpc::StatusCode::RESOURCE_EXHAUSTED, "ingress service out of memory.");
    } catch (const std::exception& e) {
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, e.what());
    } catch (...) {
        return grpc::Status(grpc::StatusCode::INTERNAL, "ingress service encountered an unexpected error.");
    }
}

grpc::Status IngestionGrpcService::Health(grpc::ServerContext* /*context*/,
                                          const proto::HealthRequest* /*request*/,
                                          proto::HealthResponse* response) {
    response->set_serving(!runtime_.stopped());
    response->set_message(runtime_.stopped() ? "stopped" : "serving");
    return grpc::Status::OK;
}

grpc::Status IngestionGrpcService::GetStats(grpc::ServerContext* /*context*/,
                                            const proto::GetStatsRequest* /*request*/,
                                            proto::GetStatsResponse* response) {
    IngestionRuntimeStats stats = runtime_.stats();
    response->set_accepted_rows(stats.accepted_rows);
    response->set_written_rows(stats.written_rows);
    response->set_failed_rows(stats.failed_rows);
    response->set_queue_depth(stats.queue_depth);
    response->set_inflight_requests(stats.inflight_requests);
    return grpc::Status::OK;
}

proto::AckMode ack_mode_to_proto(AckMode mode) {
    switch (mode) {
        case AckMode::AcceptOnly:
            return proto::ACK_MODE_ACCEPT_ONLY;
        case AckMode::EnqueueOnly:
            return proto::ACK_MODE_ENQUEUE_ONLY;
    }
    return proto::ACK_MODE_ENQUEUE_ONLY;
}

proto::AckStage ack_stage_to_proto(IngestionAckStage stage) {
    switch (stage) {
        case IngestionAckStage::Accepted:
            return proto::ACK_STAGE_ACCEPTED;
        case IngestionAckStage::Enqueued:
            return proto::ACK_STAGE_ENQUEUED;
        case IngestionAckStage::Failed:
            return proto::ACK_STAGE_FAILED;
    }
    return proto::ACK_STAGE_FAILED;
}
