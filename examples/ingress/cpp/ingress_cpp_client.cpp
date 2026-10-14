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

#include "taosgen/ingress/ingestion.grpc.pb.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <iomanip>
#include <iostream>
#include <memory>
#include <numeric>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <grpcpp/grpcpp.h>

namespace proto = taosgen::ingress;

namespace {

enum class WorkloadMode {
    Interlace1,
    Interlace0,
};

struct Options {
    std::string address = "127.0.0.1:7600";
    std::string session_id = "taosgen-ingress-cpp-example";
    int concurrency = 10;
    int deadline_ms = 30000;
    bool skip_write_wait = false;
    proto::AckMode ack_mode = proto::ACK_MODE_ENQUEUE_ONLY;

    WorkloadMode mode = WorkloadMode::Interlace1;
    int total_tables = 1000000;
    int rows_per_table = 100;
    int tables_per_request = 10000;
    int rows_per_table_per_request = 1;
    int rows_per_request = 10000;
};

struct LatencyStats {
    double min_ms = 0;
    double avg_ms = 0;
    double p50_ms = 0;
    double p90_ms = 0;
    double p95_ms = 0;
    double p99_ms = 0;
    double max_ms = 0;
};

struct WriteWaitResult {
    proto::GetStatsResponse stats;
    bool completed = false;
    bool timed_out = false;
    bool failed = false;
    std::string last_error;
};

std::string next_arg(int& index, int argc, char** argv, const std::string& name) {
    if (index + 1 >= argc) {
        throw std::runtime_error("missing value for " + name);
    }
    return argv[++index];
}

int parse_positive_int(const std::string& value, const std::string& name) {
    const int parsed = std::stoi(value);
    if (parsed <= 0) {
        throw std::runtime_error(name + " must be a positive integer");
    }
    return parsed;
}

WorkloadMode parse_mode(const std::string& value) {
    if (value == "interlace1") return WorkloadMode::Interlace1;
    if (value == "interlace0") return WorkloadMode::Interlace0;
    throw std::runtime_error("invalid mode: " + value);
}

std::string mode_name(WorkloadMode mode) {
    switch (mode) {
        case WorkloadMode::Interlace1:
            return "interlace1";
        case WorkloadMode::Interlace0:
            return "interlace0";
    }
    return "unknown";
}

proto::AckMode parse_ack_mode(const std::string& value) {
    if (value == "accept_only") return proto::ACK_MODE_ACCEPT_ONLY;
    if (value == "enqueue_only") return proto::ACK_MODE_ENQUEUE_ONLY;
    throw std::runtime_error("invalid ack mode: " + value);
}

std::string ack_mode_name(proto::AckMode mode) {
    switch (mode) {
        case proto::ACK_MODE_ACCEPT_ONLY:
            return "accept_only";
        case proto::ACK_MODE_ENQUEUE_ONLY:
            return "enqueue_only";
        default:
            return "unspecified";
    }
}

void apply_mode_defaults(Options& opts, WorkloadMode mode) {
    opts.mode = mode;
    if (mode == WorkloadMode::Interlace1) {
        opts.total_tables = 1000000;
        opts.rows_per_table = 100;
        opts.tables_per_request = 10000;
        opts.rows_per_table_per_request = 1;
        opts.rows_per_request = 10000;
    } else {
        opts.total_tables = 10000;
        opts.rows_per_table = 10000;
        opts.tables_per_request = 1;
        opts.rows_per_table_per_request = 10000;
        opts.rows_per_request = 10000;
    }
}

void print_usage(const char* program) {
    std::cout
        << "Usage: " << program << " [options]\n\n"
        << "Common options:\n"
        << "  --mode interlace1|interlace0      Workload scenario. Default: interlace1\n"
        << "  --address HOST:PORT               taosgen ingress address. Default: 127.0.0.1:7600\n"
        << "  --concurrency N                   Client worker count. Default: 10\n"
        << "  --ack-mode MODE                   accept_only|enqueue_only\n"
        << "  --deadline-ms N                   RPC deadline in milliseconds. Default: 30000\n"
        << "  --session-id ID                   Session id written to taosgen logs\n"
        << "  --skip-write-wait                 Do not wait until server stats show written rows\n\n"
        << "Workload options:\n"
        << "  --total-tables N                  Child table count\n"
        << "  --rows-per-table N                Rows generated for each child table\n"
        << "  --tables-per-request N            Tables carried by each request\n"
        << "  --rows-per-table-per-request N    Rows per table in one request\n"
        << "  --rows-per-request N              Rows per TableBatch request in interlace0 mode\n";
}

Options parse_options(int argc, char** argv) {
    Options opts;
    apply_mode_defaults(opts, opts.mode);
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--help") {
            print_usage(argv[0]);
            std::exit(0);
        } else if (arg == "--mode") {
            apply_mode_defaults(opts, parse_mode(next_arg(i, argc, argv, arg)));
        } else if (arg == "--address") {
            opts.address = next_arg(i, argc, argv, arg);
        } else if (arg == "--session-id") {
            opts.session_id = next_arg(i, argc, argv, arg);
        } else if (arg == "--concurrency") {
            opts.concurrency = parse_positive_int(next_arg(i, argc, argv, arg), arg);
        } else if (arg == "--deadline-ms") {
            opts.deadline_ms = parse_positive_int(next_arg(i, argc, argv, arg), arg);
        } else if (arg == "--ack-mode") {
            opts.ack_mode = parse_ack_mode(next_arg(i, argc, argv, arg));
        } else if (arg == "--skip-write-wait") {
            opts.skip_write_wait = true;
        } else if (arg == "--total-tables") {
            opts.total_tables = parse_positive_int(next_arg(i, argc, argv, arg), arg);
        } else if (arg == "--rows-per-table") {
            opts.rows_per_table = parse_positive_int(next_arg(i, argc, argv, arg), arg);
        } else if (arg == "--tables-per-request") {
            opts.tables_per_request = parse_positive_int(next_arg(i, argc, argv, arg), arg);
        } else if (arg == "--rows-per-table-per-request") {
            opts.rows_per_table_per_request = parse_positive_int(next_arg(i, argc, argv, arg), arg);
        } else if (arg == "--rows-per-request") {
            opts.rows_per_request = parse_positive_int(next_arg(i, argc, argv, arg), arg);
        } else {
            throw std::runtime_error("unknown option: " + arg);
        }
    }

    if (opts.session_id.empty()) {
        throw std::runtime_error("--session-id must not be empty");
    }
    if (opts.tables_per_request > opts.total_tables) {
        throw std::runtime_error("--tables-per-request must not exceed --total-tables");
    }
    if (opts.rows_per_table_per_request > opts.rows_per_table) {
        throw std::runtime_error("--rows-per-table-per-request must not exceed --rows-per-table");
    }
    return opts;
}

int64_t now_ms() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

int64_t total_rows(const Options& opts) {
    return static_cast<int64_t>(opts.total_tables) * opts.rows_per_table;
}

int ceil_div(int lhs, int rhs) {
    return (lhs + rhs - 1) / rhs;
}

int request_count(const Options& opts) {
    if (opts.mode == WorkloadMode::Interlace1) {
        const int table_groups = ceil_div(opts.total_tables, opts.tables_per_request);
        const int row_groups = ceil_div(opts.rows_per_table, opts.rows_per_table_per_request);
        return table_groups * row_groups;
    }

    const int row_groups = ceil_div(opts.rows_per_table, opts.rows_per_request);
    return opts.total_tables * row_groups;
}

std::string table_name(int table_index) {
    return "d" + std::to_string(table_index);
}

float current_value(int table_index, int row_index) {
    return static_cast<float>((table_index + row_index) % 10000) / 100.0f;
}

int32_t voltage_value(int table_index, int row_index) {
    return 200 + ((table_index + row_index) % 41);
}

float phase_value(int row_index) {
    return static_cast<float>(std::fmod(row_index * 3.14159265358979323846, 180.0));
}

void fill_table_batch(proto::TableBatch* table,
                      int table_index,
                      int first_row,
                      int rows,
                      int64_t base_ts) {
    table->set_table_name(table_name(table_index));

    auto* ts_values = table->mutable_timestamps()->mutable_values();
    ts_values->Reserve(rows);

    auto* current_values = table->add_columns()->mutable_float_values()->mutable_values();
    current_values->Reserve(rows);

    auto* voltage_values = table->add_columns()->mutable_int_values()->mutable_values();
    voltage_values->Reserve(rows);

    auto* phase_values = table->add_columns()->mutable_float_values()->mutable_values();
    phase_values->Reserve(rows);

    for (int i = 0; i < rows; ++i) {
        const int row_index = first_row + i;
        ts_values->Add(base_ts + row_index);
        current_values->Add(current_value(table_index, row_index));
        voltage_values->Add(voltage_value(table_index, row_index));
        phase_values->Add(phase_value(row_index));
    }
}

void build_interlace1_request(proto::IngestRequest& request,
                              const Options& opts,
                              int request_index,
                              int64_t base_ts) {
    const int table_groups = ceil_div(opts.total_tables, opts.tables_per_request);
    const int table_group = request_index % table_groups;
    const int row_group = request_index / table_groups;
    const int first_table = table_group * opts.tables_per_request;
    const int table_count = std::min(opts.tables_per_request, opts.total_tables - first_table);
    const int first_row = row_group * opts.rows_per_table_per_request;
    const int rows_per_table = std::min(opts.rows_per_table_per_request, opts.rows_per_table - first_row);
    const int request_rows = table_count * rows_per_table;

    auto* batch = request.mutable_interlace_batch();
    auto* range = batch->mutable_table_names()->mutable_range();
    range->set_prefix("d");
    range->set_start(static_cast<uint64_t>(first_table));
    range->set_count(static_cast<uint32_t>(table_count));
    range->set_step(1);
    batch->set_rows_per_table(static_cast<uint32_t>(rows_per_table));

    auto* ts_values = batch->mutable_timestamps()->mutable_values();
    ts_values->Reserve(request_rows);

    auto* current_values = batch->add_columns()->mutable_float_values()->mutable_values();
    current_values->Reserve(request_rows);

    auto* voltage_values = batch->add_columns()->mutable_int_values()->mutable_values();
    voltage_values->Reserve(request_rows);

    auto* phase_values = batch->add_columns()->mutable_float_values()->mutable_values();
    phase_values->Reserve(request_rows);

    for (int offset = 0; offset < table_count; ++offset) {
        const int table_index = first_table + offset;
        for (int row = 0; row < rows_per_table; ++row) {
            const int row_index = first_row + row;
            ts_values->Add(base_ts + row_index);
            current_values->Add(current_value(table_index, row_index));
            voltage_values->Add(voltage_value(table_index, row_index));
            phase_values->Add(phase_value(row_index));
        }
    }
}

void build_interlace0_request(proto::IngestRequest& request,
                              const Options& opts,
                              int request_index,
                              int64_t base_ts) {
    const int row_groups = ceil_div(opts.rows_per_table, opts.rows_per_request);
    const int table_index = request_index / row_groups;
    const int row_group = request_index % row_groups;
    const int first_row = row_group * opts.rows_per_request;
    const int rows = std::min(opts.rows_per_request, opts.rows_per_table - first_row);

    request.mutable_tables()->Reserve(1);
    fill_table_batch(request.add_tables(), table_index, first_row, rows, base_ts);
}

proto::IngestRequest build_request(const Options& opts, int request_index, int64_t base_ts) {
    proto::IngestRequest request;
    request.set_session_id(opts.session_id);
    request.set_request_id(mode_name(opts.mode) + "-req-" + std::to_string(request_index));
    request.set_ack_mode(opts.ack_mode);

    if (opts.mode == WorkloadMode::Interlace1) {
        build_interlace1_request(request, opts, request_index, base_ts);
    } else {
        build_interlace0_request(request, opts, request_index, base_ts);
    }
    return request;
}

uint64_t safe_delta(uint64_t current, uint64_t initial) {
    return current >= initial ? current - initial : 0;
}

LatencyStats calculate_stats(std::vector<double>& values) {
    LatencyStats stats;
    if (values.empty()) return stats;
    std::sort(values.begin(), values.end());
    const double sum = std::accumulate(values.begin(), values.end(), 0.0);
    auto percentile = [&](double pct) {
        const size_t index = std::min(values.size() - 1,
                                      static_cast<size_t>(std::ceil((pct / 100.0) * values.size()) - 1));
        return values[index];
    };
    stats.min_ms = values.front();
    stats.avg_ms = sum / static_cast<double>(values.size());
    stats.p50_ms = percentile(50);
    stats.p90_ms = percentile(90);
    stats.p95_ms = percentile(95);
    stats.p99_ms = percentile(99);
    stats.max_ms = values.back();
    return stats;
}

void wait_until_serving(proto::IngestionService::Stub& stub) {
    for (int i = 0; i < 200; ++i) {
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(500));
        proto::HealthRequest request;
        proto::HealthResponse response;
        grpc::Status status = stub.Health(&context, request, &response);
        if (status.ok() && response.serving()) {
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    throw std::runtime_error("ingress service did not become healthy");
}

proto::GetStatsResponse get_stats(proto::IngestionService::Stub& stub) {
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
    proto::GetStatsRequest request;
    proto::GetStatsResponse response;
    grpc::Status status = stub.GetStats(&context, request, &response);
    if (!status.ok()) {
        throw std::runtime_error("GetStats failed: " + status.error_message());
    }
    return response;
}

WriteWaitResult wait_until_written(proto::IngestionService::Stub& stub,
                                   const proto::GetStatsResponse& initial,
                                   int64_t expected_rows,
                                   int timeout_seconds) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeout_seconds);
    WriteWaitResult result;
    while (std::chrono::steady_clock::now() < deadline) {
        try {
            result.stats = get_stats(stub);
            result.last_error.clear();
            const uint64_t written_delta = safe_delta(result.stats.written_rows(), initial.written_rows());
            const uint64_t failed_delta = safe_delta(result.stats.failed_rows(), initial.failed_rows());
            if (written_delta >= static_cast<uint64_t>(expected_rows) || failed_delta > 0) {
                result.completed = written_delta >= static_cast<uint64_t>(expected_rows);
                result.failed = failed_delta > 0;
                return result;
            }
        } catch (const std::exception& e) {
            result.last_error = e.what();
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    result.timed_out = true;
    return result;
}

} // namespace

int main(int argc, char** argv) {
    try {
        const Options opts = parse_options(argc, argv);
        const int total_requests = request_count(opts);
        const int64_t expected_rows = total_rows(opts);

        auto channel = grpc::CreateChannel(opts.address, grpc::InsecureChannelCredentials());
        auto control_stub = proto::IngestionService::NewStub(channel);
        wait_until_serving(*control_stub);
        const proto::GetStatsResponse initial_stats = get_stats(*control_stub);

        const int64_t base_ts = now_ms();
        std::atomic<int> next_request{0};
        std::atomic<int64_t> accepted_rows{0};
        std::atomic<int64_t> failed_requests{0};
        std::atomic<bool> abort_workers{false};
        std::exception_ptr worker_exception;
        std::mutex worker_exception_mutex;
        std::vector<double> latencies_ms(static_cast<size_t>(total_requests), 0.0);

        const auto start = std::chrono::steady_clock::now();
        std::vector<std::thread> workers;
        workers.reserve(opts.concurrency);

        for (int tid = 0; tid < opts.concurrency; ++tid) {
            workers.emplace_back([&, tid] {
                try {
                    auto stub = proto::IngestionService::NewStub(channel);
                    while (!abort_workers.load()) {
                        const int request_index = next_request.fetch_add(1);
                        if (request_index >= total_requests) {
                            break;
                        }

                        proto::IngestRequest request = build_request(opts, request_index, base_ts);
                        proto::IngestResponse response;
                        grpc::ClientContext context;
                        context.set_deadline(std::chrono::system_clock::now() +
                                             std::chrono::milliseconds(opts.deadline_ms));

                        const auto t0 = std::chrono::steady_clock::now();
                        grpc::Status status = stub->Ingest(&context, request, &response);
                        const auto t1 = std::chrono::steady_clock::now();
                        const double latency_ms =
                            std::chrono::duration<double, std::milli>(t1 - t0).count();

                        latencies_ms[static_cast<size_t>(request_index)] = latency_ms;

                        if (status.ok() && response.code() == 0) {
                            accepted_rows.fetch_add(static_cast<int64_t>(response.accepted_rows()));
                        } else {
                            failed_requests.fetch_add(1);
                            std::cerr << "thread " << tid << ", request " << request_index
                                      << " failed: grpc_code=" << status.error_code()
                                      << " grpc_message=" << status.error_message()
                                      << " response_code=" << response.code()
                                      << " response_message=" << response.message() << "\n";
                        }
                    }
                } catch (...) {
                    {
                        std::lock_guard<std::mutex> lock(worker_exception_mutex);
                        if (!worker_exception) {
                            worker_exception = std::current_exception();
                        }
                    }
                    abort_workers.store(true);
                }
            });
        }

        for (auto& worker : workers) {
            worker.join();
        }

        if (worker_exception) {
            std::rethrow_exception(worker_exception);
        }

        const auto submit_end = std::chrono::steady_clock::now();
        const double submit_duration_s = std::chrono::duration<double>(submit_end - start).count();
        const LatencyStats latency = calculate_stats(latencies_ms);

        WriteWaitResult write_wait;
        if (opts.skip_write_wait) {
            write_wait.stats = get_stats(*control_stub);
        } else {
            write_wait = wait_until_written(
                *control_stub,
                initial_stats,
                accepted_rows.load(),
                std::max(60, opts.deadline_ms / 1000 * 20));
        }

        const auto written_end = std::chrono::steady_clock::now();
        const double write_duration_s = std::chrono::duration<double>(written_end - start).count();
        const uint64_t accepted_delta = safe_delta(write_wait.stats.accepted_rows(), initial_stats.accepted_rows());
        const uint64_t written_delta = safe_delta(write_wait.stats.written_rows(), initial_stats.written_rows());
        const uint64_t failed_delta = safe_delta(write_wait.stats.failed_rows(), initial_stats.failed_rows());

        std::cout << std::fixed << std::setprecision(2);
        std::cout << "Taosgen Ingress C++ Client Summary\n";
        std::cout << "Address: " << opts.address << "\n";
        std::cout << "Mode: " << mode_name(opts.mode) << "\n";
        std::cout << "Ack Mode: " << ack_mode_name(opts.ack_mode) << "\n";
        std::cout << "Client Concurrency: " << opts.concurrency << "\n";
        std::cout << "Total Tables: " << opts.total_tables << "\n";
        std::cout << "Rows Per Table: " << opts.rows_per_table << "\n";
        std::cout << "Expected Rows: " << expected_rows << "\n";
        std::cout << "Total Requests: " << total_requests << "\n";
        std::cout << "Accepted Rows: " << accepted_rows.load() << "\n";
        std::cout << "Failed Requests: " << failed_requests.load() << "\n";
        std::cout << "Client Submit Duration: " << submit_duration_s << " seconds\n";
        std::cout << "Client Accepted Rate: "
                  << (static_cast<double>(accepted_rows.load()) / submit_duration_s)
                  << " rows/second\n";
        if (opts.skip_write_wait) {
            std::cout << "Write Wait Status: skipped\n";
        } else if (write_wait.completed) {
            std::cout << "Write Wait Status: completed\n";
            std::cout << "Write Complete Duration: " << write_duration_s << " seconds\n";
            std::cout << "Write Complete Rate: "
                      << (static_cast<double>(written_delta) / write_duration_s)
                      << " rows/second\n";
        } else if (write_wait.failed) {
            std::cout << "Write Wait Status: failed\n";
            std::cerr << "warning: server reported failed rows before the expected write count was reached\n";
        } else {
            std::cout << "Write Wait Status: timed out\n";
            if (!write_wait.last_error.empty()) {
                std::cerr << "warning: write wait encountered transient error: " << write_wait.last_error << "\n";
            }
            std::cerr << "warning: write wait timed out before the expected rows were written\n";
        }
        std::cout << "RPC Latency: min " << latency.min_ms << "ms, avg " << latency.avg_ms
                  << "ms, p50 " << latency.p50_ms << "ms, p90 " << latency.p90_ms
                  << "ms, p95 " << latency.p95_ms << "ms, p99 " << latency.p99_ms
                  << "ms, max " << latency.max_ms << "ms\n";
        std::cout << "Server Stats Delta: accepted_rows="
                  << accepted_delta
                  << ", written_rows=" << written_delta
                  << ", failed_rows=" << failed_delta
                  << ", queue_depth=" << write_wait.stats.queue_depth()
                  << ", inflight_requests=" << write_wait.stats.inflight_requests() << "\n";

        if (!opts.skip_write_wait && !write_wait.completed) {
            return 3;
        }
        return failed_requests.load() == 0 ? 0 : 2;
    } catch (const std::exception& e) {
        std::cerr << "fatal: " << e.what() << "\n";
        return 1;
    }
}
