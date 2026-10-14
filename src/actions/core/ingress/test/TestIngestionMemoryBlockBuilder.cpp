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

#include "IngestionMemoryBlockBuilder.hpp"
#include "TimestampUtils.hpp"
#include "taosgen/ingress/ingestion.pb.h"
#undef NDEBUG
#include <cassert>
#include <iostream>
#include <memory>
#include <stdexcept>

namespace proto = taosgen::ingress;

namespace {

SchemaConfig make_schema() {
    SchemaConfig schema;
    schema.name = "meters";
    schema.columns = {
        ColumnConfig("ts", "timestamp"),
        ColumnConfig("current", "double"),
        ColumnConfig("status_time", "timestamp"),
        ColumnConfig("status", "int"),
        ColumnConfig("label", "binary(16)"),
        ColumnConfig("payload", "varbinary(8)")
    };
    schema.tags = {
        ColumnConfig("device_id", "binary(64)"),
        ColumnConfig("area", "binary(64)")
    };
    schema.apply(std::string("ms"));
    return schema;
}

IngressConfig make_config() {
    IngressConfig config;
    config.rows_per_batch = 100;
    config.max_tables_per_request = 10;
    config.auto_create_table = true;
    return config;
}

IngestRequestData make_request() {
    IngestRequestData request;
    request.session_id = "sess-1";
    request.request_id = "req-1";
    request.tables.push_back(IngestTableBatch{
        "d1001",
        IngestTimestampValues{{1000, 2000}, {}},
        {
            IngestColumnValues{std::vector<double>{12.3, 12.4}, {}},
            IngestColumnValues{IngestTimestampValues{{}, {"2026-08-13 10:00:00.000", "2026-08-13 10:00:00.001"}}, {}},
            IngestColumnValues{std::vector<int32_t>{1, 2}, {false, true}},
            IngestColumnValues{std::vector<std::string>{"east", "west"}, {}},
            IngestColumnValues{IngestColumnValues::BinaryArray{{1, 2, 3}, {4, 5}}, {}}
        },
        RowType{std::string("d1001"), std::string("east")}
    });
    return request;
}

proto::IngestRequest make_proto_request() {
    proto::IngestRequest request;
    request.set_session_id("sess-1");
    request.set_request_id("req-1");

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
    status->add_nulls(false);
    status->add_nulls(true);

    proto::ColumnValues* label = table->add_columns();
    label->mutable_string_values()->add_values("east");
    label->mutable_string_values()->add_values("west");

    proto::ColumnValues* payload = table->add_columns();
    payload->mutable_binary_values()->add_values(std::string("\x01\x02\x03", 3));
    payload->mutable_binary_values()->add_values(std::string("\x04\x05", 2));

    table->add_tags()->set_string_value("d1001");
    table->add_tags()->set_string_value("east");
    return request;
}

proto::IngestRequest make_proto_interlace_request() {
    proto::IngestRequest request;
    request.set_session_id("sess-1");
    request.set_request_id("req-interlace-1");

    auto* batch = request.mutable_interlace_batch();
    auto* range = batch->mutable_table_names()->mutable_range();
    range->set_prefix("d");
    range->set_start(1001);
    range->set_count(2);
    range->set_step(1);
    batch->set_rows_per_table(2);

    auto* timestamps = batch->mutable_timestamps()->mutable_values();
    timestamps->Add(1000);
    timestamps->Add(1001);
    timestamps->Add(2000);
    timestamps->Add(2001);

    auto* current = batch->add_columns();
    auto* current_values = current->mutable_double_values()->mutable_values();
    current_values->Add(12.3);
    current_values->Add(12.4);
    current_values->Add(22.3);
    current_values->Add(22.4);

    auto* status_time = batch->add_columns();
    auto* status_time_values = status_time->mutable_timestamp_values()->mutable_values();
    status_time_values->Add(3000);
    status_time_values->Add(3001);
    status_time_values->Add(4000);
    status_time_values->Add(4001);

    auto* status = batch->add_columns();
    auto* status_values = status->mutable_int_values()->mutable_values();
    status_values->Add(1);
    status_values->Add(2);
    status_values->Add(3);
    status_values->Add(4);

    auto* label = batch->add_columns();
    auto* label_values = label->mutable_string_values()->mutable_values();
    label_values->Add("east");
    label_values->Add("west");
    label_values->Add("north");
    label_values->Add("south");

    auto* payload = batch->add_columns();
    auto* payload_values = payload->mutable_binary_values()->mutable_values();
    payload_values->Add(std::string("\x01", 1));
    payload_values->Add(std::string("\x02", 1));
    payload_values->Add(std::string("\x03", 1));
    payload_values->Add(std::string("\x04", 1));
    return request;
}

proto::IngestRequest make_proto_request_with_rows(size_t row_count, const std::string& table_name) {
    proto::IngestRequest request;
    request.set_session_id("sess-rows");
    request.set_request_id("req-rows");

    proto::TableBatch* table = request.add_tables();
    table->set_table_name(table_name);
    for (size_t i = 0; i < row_count; ++i) {
        table->mutable_timestamps()->add_values(1000 + static_cast<int64_t>(i));
    }

    proto::ColumnValues* current = table->add_columns();
    for (size_t i = 0; i < row_count; ++i) {
        current->mutable_double_values()->add_values(12.3 + static_cast<double>(i));
    }

    proto::ColumnValues* status_time = table->add_columns();
    for (size_t i = 0; i < row_count; ++i) {
        status_time->mutable_timestamp_values()->add_string_values("2026-08-13 10:00:00.000");
    }

    proto::ColumnValues* status = table->add_columns();
    for (size_t i = 0; i < row_count; ++i) {
        status->mutable_int_values()->add_values(static_cast<int32_t>(i + 1));
    }

    proto::ColumnValues* label = table->add_columns();
    for (size_t i = 0; i < row_count; ++i) {
        label->mutable_string_values()->add_values("east");
    }

    proto::ColumnValues* payload = table->add_columns();
    for (size_t i = 0; i < row_count; ++i) {
        payload->mutable_binary_values()->add_values(std::string("\x01", 1));
    }

    table->add_tags()->set_string_value(table_name);
    table->add_tags()->set_string_value("east");
    return request;
}

void test_build_memory_block() {
    IngestionMemoryBlockBuilder builder(make_schema(), make_config());
    MemoryPool pool(1, 1, 2, builder.column_instances(), builder.tag_instances());

    IngestRequestData request = make_request();
    MemoryPool::MemoryBlock* block = builder.build(request, pool);
    assert(block != nullptr);
    assert(block->used_tables == 1);
    assert(block->total_rows == 2);
    assert(block->bindv_.count == 1);

    const auto& table = block->tables[0];
    assert(std::string(table.table_name) == "d1001");
    assert(table.used_rows == 2);
    assert(table.timestamps[0] == 1000);
    assert(table.timestamps[1] == 2000);
    assert(block->start_time == 1000);
    assert(block->end_time == 2000);

    assert(std::get<double>(table.get_column_cell(0, 0)) == 12.3);
    assert(std::get<double>(table.get_column_cell(1, 0)) == 12.4);

    int64_t expected_ts = TimestampUtils::parse_timestamp(std::string("2026-08-13 10:00:00.000"), "ms");
    assert(std::get<int64_t>(table.get_column_cell(0, 1)) == expected_ts);
    assert(std::holds_alternative<NullValue>(table.get_column_cell(1, 2)));
    assert(std::get<std::string>(table.get_column_cell(0, 3)) == "east");

    auto payload = std::get<std::vector<uint8_t>>(table.get_column_cell(1, 4));
    assert(payload.size() == 2);
    assert(payload[0] == 4);
    assert(payload[1] == 5);

    assert(table.tags_ptr != nullptr);
    assert(std::get<std::string>(table.get_tag_cell(0, 0)) == "d1001");
    assert(std::get<std::string>(table.get_tag_cell(0, 1)) == "east");

    assert(block->bindv_.bind_cols[0][0].buffer_type == TSDB_DATA_TYPE_TIMESTAMP);
    assert(block->bindv_.bind_cols[0][2].buffer_type == TSDB_DATA_TYPE_TIMESTAMP);
    assert(block->bindv_.bind_cols[0][3].is_null[1] == 1);
    assert(block->bindv_.tags[0] != nullptr);

    block->release();
    std::cout << "test_build_memory_block passed.\n";
}

void test_build_proto_memory_block() {
    IngestionMemoryBlockBuilder builder(make_schema(), make_config());
    MemoryPool pool(1, 1, 2, builder.column_instances(), builder.tag_instances());

    proto::IngestRequest request = make_proto_request();
    MemoryPool::MemoryBlock* block = builder.build_proto(request, pool);
    assert(block != nullptr);
    assert(block->used_tables == 1);
    assert(block->total_rows == 2);
    assert(block->bindv_.count == 1);

    const auto& table = block->tables[0];
    assert(std::string(table.table_name) == "d1001");
    assert(table.used_rows == 2);
    assert(table.timestamps[0] == 1000);
    assert(table.timestamps[1] == 2000);
    assert(block->start_time == 1000);
    assert(block->end_time == 2000);

    assert(std::get<double>(table.get_column_cell(0, 0)) == 12.3);
    assert(std::get<double>(table.get_column_cell(1, 0)) == 12.4);

    int64_t expected_ts = TimestampUtils::parse_timestamp(std::string("2026-08-13 10:00:00.000"), "ms");
    assert(std::get<int64_t>(table.get_column_cell(0, 1)) == expected_ts);
    assert(std::holds_alternative<NullValue>(table.get_column_cell(1, 2)));
    assert(std::get<std::string>(table.get_column_cell(0, 3)) == "east");

    auto payload = std::get<std::vector<uint8_t>>(table.get_column_cell(1, 4));
    assert(payload.size() == 2);
    assert(payload[0] == 4);
    assert(payload[1] == 5);

    assert(table.tags_ptr != nullptr);
    assert(std::get<std::string>(table.get_tag_cell(0, 0)) == "d1001");
    assert(std::get<std::string>(table.get_tag_cell(0, 1)) == "east");

    assert(block->bindv_.bind_cols[0][0].buffer_type == TSDB_DATA_TYPE_TIMESTAMP);
    assert(block->bindv_.bind_cols[0][2].buffer_type == TSDB_DATA_TYPE_TIMESTAMP);
    assert(block->bindv_.bind_cols[0][3].is_null[1] == 1);
    assert(block->bindv_.tags[0] != nullptr);

    block->release();
    std::cout << "test_build_proto_memory_block passed.\n";
}

void test_build_proto_interlace_memory_block() {
    IngressConfig config = make_config();
    config.auto_create_table = false;
    IngestionMemoryBlockBuilder builder(make_schema(), config);
    MemoryPool pool(1, 2, 2, builder.column_instances(), builder.tag_instances());

    proto::IngestRequest request = make_proto_interlace_request();
    MemoryPool::MemoryBlock* block = builder.build_proto(request, pool);
    assert(block != nullptr);
    assert(block->used_tables == 2);
    assert(block->total_rows == 4);
    assert(block->bindv_.count == 2);

    const auto& first_table = block->tables[0];
    const auto& second_table = block->tables[1];
    assert(std::string(first_table.table_name) == "d1001");
    assert(std::string(second_table.table_name) == "d1002");
    assert(first_table.used_rows == 2);
    assert(second_table.used_rows == 2);
    assert(first_table.timestamps[0] == 1000);
    assert(first_table.timestamps[1] == 1001);
    assert(second_table.timestamps[0] == 2000);
    assert(second_table.timestamps[1] == 2001);
    assert(block->start_time == 1000);
    assert(block->end_time == 2001);

    assert(std::get<double>(first_table.get_column_cell(0, 0)) == 12.3);
    assert(std::get<double>(first_table.get_column_cell(1, 0)) == 12.4);
    assert(std::get<double>(second_table.get_column_cell(0, 0)) == 22.3);
    assert(std::get<int64_t>(second_table.get_column_cell(1, 1)) == 4001);
    assert(std::get<int32_t>(second_table.get_column_cell(0, 2)) == 3);
    assert(std::get<std::string>(first_table.get_column_cell(0, 3)) == "east");

    auto payload = std::get<std::vector<uint8_t>>(second_table.get_column_cell(1, 4));
    assert(payload.size() == 1);
    assert(payload[0] == 4);
    assert(block->bindv_.tags == nullptr);

    block->release();
    std::cout << "test_build_proto_interlace_memory_block passed.\n";
}

void test_build_proto_interlace_table_name_outlives_builder() {
    MemoryPool::MemoryBlock* block = nullptr;
    MemoryPool* pool = nullptr;
    std::unique_ptr<MemoryPool> pool_holder;
    {
        IngressConfig config = make_config();
        config.auto_create_table = false;
        IngestionMemoryBlockBuilder builder(make_schema(), config);
        pool_holder = std::make_unique<MemoryPool>(1, 2, 2, builder.column_instances(), builder.tag_instances());
        pool = pool_holder.get();

        proto::IngestRequest request = make_proto_interlace_request();
        block = builder.build_proto(request, *pool);
        assert(block != nullptr);
    }

    assert(block != nullptr);
    assert(std::string(block->tables[0].table_name) == "d1001");
    assert(std::string(block->tables[1].table_name) == "d1002");
    block->release();
    pool_holder.reset();

    std::cout << "test_build_proto_interlace_table_name_outlives_builder passed.\n";
}

void test_build_without_auto_create_ignores_schema_tags() {
    IngressConfig config = make_config();
    config.auto_create_table = false;
    IngestionMemoryBlockBuilder builder(make_schema(), config);
    MemoryPool pool(1, 1, 2, builder.column_instances(), builder.tag_instances());

    IngestRequestData request = make_request();
    request.tables[0].tags.clear();

    MemoryPool::MemoryBlock* block = builder.build(request, pool);
    assert(block != nullptr);
    assert(block->used_tables == 1);
    assert(block->total_rows == 2);
    assert(block->tables[0].tags_ptr == nullptr);
    assert(block->bindv_.tags == nullptr);

    block->release();
    std::cout << "test_build_without_auto_create_ignores_schema_tags passed.\n";
}

void test_reject_request_exceeding_per_table_capacity() {
    IngestionMemoryBlockBuilder builder(make_schema(), make_config());
    MemoryPool pool(1, 1, 1, builder.column_instances(), builder.tag_instances());

    IngestRequestData request = make_request();
    request.tables[0].timestamps.values = {1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010};
    request.tables[0].columns[0] = IngestColumnValues{std::vector<double>{12.3, 12.4, 12.5, 12.6, 12.7, 12.8, 12.9, 13.0, 13.1, 13.2, 13.3}, {}};
    request.tables[0].columns[1] = IngestColumnValues{IngestTimestampValues{{}, {
        "2026-08-13 10:00:00.000", "2026-08-13 10:00:00.001", "2026-08-13 10:00:00.002",
        "2026-08-13 10:00:00.003", "2026-08-13 10:00:00.004", "2026-08-13 10:00:00.005",
        "2026-08-13 10:00:00.006", "2026-08-13 10:00:00.007", "2026-08-13 10:00:00.008",
        "2026-08-13 10:00:00.009", "2026-08-13 10:00:00.010"
    }}, {}};
    request.tables[0].columns[2] = IngestColumnValues{std::vector<int32_t>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, {}};
    request.tables[0].columns[3] = IngestColumnValues{std::vector<std::string>{
        "east", "east", "east", "east", "east", "east", "east", "east", "east", "east", "east"
    }, {}};
    request.tables[0].columns[4] = IngestColumnValues{IngestColumnValues::BinaryArray{
        {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}
    }, {}};

    try {
        (void)builder.build(request, pool);
        assert(false && "builder should reject per-table capacity overflow");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("per-table capacity") != std::string::npos);
    }

    std::cout << "test_reject_request_exceeding_per_table_capacity passed.\n";
}

void test_reject_schema_count_expansion() {
    SchemaConfig schema = make_schema();
    schema.columns[1].count = 2;

    try {
        IngestionMemoryBlockBuilder builder(schema, make_config());
        (void)builder;
        assert(false && "builder should reject count expansion");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("count expansion") != std::string::npos);
    }

    std::cout << "test_reject_schema_count_expansion passed.\n";
}

void test_reject_proto_single_table_exceeding_per_table_capacity() {
    IngestionMemoryBlockBuilder builder(make_schema(), make_config());
    MemoryPool pool(1, 1, 2, builder.column_instances(), builder.tag_instances());

    proto::IngestRequest request = make_proto_request_with_rows(4, "d1001");
    request.mutable_tables(0)->mutable_timestamps()->clear_values();
    request.mutable_tables(0)->mutable_timestamps()->clear_string_values();
    for (int i = 0; i < 11; ++i) {
        request.mutable_tables(0)->mutable_timestamps()->add_values(1000 + i);
    }

    try {
        (void)builder.build_proto(request, pool);
        assert(false && "builder should reject per-table capacity overflow");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("per-table capacity") != std::string::npos);
    }

    std::cout << "test_reject_proto_single_table_exceeding_per_table_capacity passed.\n";
}

void test_reject_inconsistent_tags_across_requests() {
    IngestionMemoryBlockBuilder builder(make_schema(), make_config());
    MemoryPool pool(1, 1, 2, builder.column_instances(), builder.tag_instances());

    proto::IngestRequest request1 = make_proto_request();
    MemoryPool::MemoryBlock* block1 = builder.build_proto(request1, pool);
    assert(block1 != nullptr);
    block1->release();

    proto::IngestRequest request2 = make_proto_request();
    request2.mutable_tables(0)->mutable_tags(1)->set_string_value("west");

    try {
        (void)builder.build_proto(request2, pool);
        assert(false && "builder should reject inconsistent tags across requests");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("inconsistent tags") != std::string::npos);
    }

    std::cout << "test_reject_inconsistent_tags_across_requests passed.\n";
}

void test_reject_varlen_overflow() {
    IngestionMemoryBlockBuilder builder(make_schema(), make_config());
    MemoryPool pool(1, 1, 2, builder.column_instances(), builder.tag_instances());

    IngestRequestData request;
    request.session_id = "sess-2";
    request.request_id = "req-2";
    request.tables.push_back(IngestTableBatch{
        "d1001",
        IngestTimestampValues{{1000, 2000}, {}},
        {
            IngestColumnValues{std::vector<double>{12.3, 12.4}, {}},
            IngestColumnValues{IngestTimestampValues{{}, {"2026-08-13 10:00:00.000", "2026-08-13 10:00:00.001"}}, {}},
            IngestColumnValues{std::vector<int32_t>{1, 2}, {}},
            IngestColumnValues{std::vector<std::string>{"east", std::string(32, 'x')}, {}},
            IngestColumnValues{IngestColumnValues::BinaryArray{{1}, {2}}, {}}
        },
        RowType{std::string("d1001"), std::string("east")}
    });

    try {
        (void)builder.build(request, pool);
        assert(false && "builder should reject overflowing varlen values");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("schema capacity") != std::string::npos);
    }

    std::cout << "test_reject_varlen_overflow passed.\n";
}

void test_failed_request_does_not_pollute_tag_cache() {
    IngestionMemoryBlockBuilder builder(make_schema(), make_config());
    MemoryPool pool(1, 1, 2, builder.column_instances(), builder.tag_instances());

    proto::IngestRequest failed_request = make_proto_request();
    failed_request.mutable_tables(0)->mutable_tags(1)->set_string_value("bad-area");
    failed_request.mutable_tables(0)->mutable_columns(3)->mutable_string_values()->set_values(0, std::string(32, 'x'));

    try {
        (void)builder.build_proto(failed_request, pool);
        assert(false && "builder should reject overflowing varlen values");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("schema capacity") != std::string::npos);
    }

    proto::IngestRequest valid_request = make_proto_request();
    valid_request.mutable_tables(0)->mutable_tags(1)->set_string_value("good-area");
    MemoryPool::MemoryBlock* block = builder.build_proto(valid_request, pool);
    assert(block != nullptr);
    assert(block->tables[0].tags_ptr != nullptr);
    assert(std::get<std::string>(block->tables[0].get_tag_cell(0, 1)) == "good-area");
    block->release();

    std::cout << "test_failed_request_does_not_pollute_tag_cache passed.\n";
}

void test_failed_internal_request_does_not_pollute_tag_cache() {
    IngestionMemoryBlockBuilder builder(make_schema(), make_config());
    MemoryPool pool(1, 1, 2, builder.column_instances(), builder.tag_instances());

    IngestRequestData failed_request = make_request();
    failed_request.tables[0].tags[1] = std::string("bad-area");
    failed_request.tables[0].columns[3] = IngestColumnValues{std::vector<std::string>{std::string(32, 'x'), "west"}, {}};

    try {
        (void)builder.build(failed_request, pool);
        assert(false && "builder should reject overflowing varlen values");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("schema capacity") != std::string::npos);
    }

    IngestRequestData valid_request = make_request();
    valid_request.tables[0].tags[1] = std::string("good-area");
    MemoryPool::MemoryBlock* block = builder.build(valid_request, pool);
    assert(block != nullptr);
    assert(block->tables[0].tags_ptr != nullptr);
    assert(std::get<std::string>(block->tables[0].get_tag_cell(0, 1)) == "good-area");
    block->release();

    std::cout << "test_failed_internal_request_does_not_pollute_tag_cache passed.\n";
}

} // namespace

int main() {
    test_build_memory_block();
    test_build_proto_memory_block();
    test_build_proto_interlace_memory_block();
    test_build_proto_interlace_table_name_outlives_builder();
    test_build_without_auto_create_ignores_schema_tags();
    test_reject_request_exceeding_per_table_capacity();
    test_reject_schema_count_expansion();
    test_reject_proto_single_table_exceeding_per_table_capacity();
    test_reject_inconsistent_tags_across_requests();
    test_reject_varlen_overflow();
    test_failed_request_does_not_pollute_tag_cache();
    test_failed_internal_request_does_not_pollute_tag_cache();
    std::cout << "All IngestionMemoryBlockBuilder tests passed.\n";
    return 0;
}
