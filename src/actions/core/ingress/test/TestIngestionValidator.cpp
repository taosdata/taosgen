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

#include "IngestionValidator.hpp"
#undef NDEBUG
#include <cassert>
#include <iostream>

namespace {

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
            IngestColumnValues{IngestTimestampValues{{1000, 2000}, {}}, {}},
            IngestColumnValues{std::vector<int32_t>{1, 2}, {}}
        },
        RowType{std::string("d1001"), std::string("east")}
    });
    return request;
}

void test_validate_success() {
    IngestionValidator validator(make_schema(), make_config());
    auto result = validator.validate(make_request());
    assert(result.row_count == 2);
    assert(result.table_count == 1);
}

void test_validate_without_auto_create_allows_schema_tags() {
    IngressConfig config = make_config();
    config.auto_create_table = false;
    IngestionValidator validator(make_schema(), config);

    auto request = make_request();
    request.tables[0].tags.clear();

    auto result = validator.validate(request);
    assert(result.row_count == 2);
    assert(result.table_count == 1);
}

void test_reject_tags_without_auto_create() {
    IngressConfig config = make_config();
    config.auto_create_table = false;
    IngestionValidator validator(make_schema(), config);

    try {
        validator.validate(make_request());
        assert(false && "tags should require auto_create_table");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("auto_create_table=true") != std::string::npos);
    }
}

void test_reject_missing_tags_with_auto_create() {
    IngestionValidator validator(make_schema(), make_config());
    auto request = make_request();
    request.tables[0].tags.clear();
    try {
        validator.validate(request);
        assert(false && "auto create table should require tags");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("tags are required") != std::string::npos);
    }
}

void test_reject_column_count_mismatch() {
    IngestionValidator validator(make_schema(), make_config());
    auto request = make_request();
    request.tables[0].columns.pop_back();
    try {
        validator.validate(request);
        assert(false && "column count mismatch should throw");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("column count mismatch") != std::string::npos);
    }
}

void test_reject_row_count_mismatch() {
    IngestionValidator validator(make_schema(), make_config());
    auto request = make_request();
    request.tables[0].columns[0] = IngestColumnValues{std::vector<double>{12.3}, {}};
    try {
        validator.validate(request);
        assert(false && "row count mismatch should throw");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("row count mismatch") != std::string::npos);
    }
}

void test_reject_per_table_capacity_overflow() {
    IngestionValidator validator(make_schema(), make_config());
    auto request = make_request();
    request.tables[0].timestamps.values.resize(11, 0);
    request.tables[0].columns[0] = IngestColumnValues{std::vector<double>(11, 12.3), {}};
    request.tables[0].columns[1] = IngestColumnValues{IngestTimestampValues{{}, std::vector<std::string>(11, "2026-08-13 10:00:00.000")}, {}};
    request.tables[0].columns[2] = IngestColumnValues{std::vector<int32_t>(11, 1), {}};
    try {
        validator.validate(request);
        assert(false && "per-table capacity overflow should throw");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("per-table capacity") != std::string::npos);
    }
}

void test_reject_type_mismatch() {
    IngestionValidator validator(make_schema(), make_config());
    auto request = make_request();
    request.tables[0].columns[0] = IngestColumnValues{std::vector<int32_t>{1, 2}, {}};
    try {
        validator.validate(request);
        assert(false && "type mismatch should throw");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("type does not match") != std::string::npos);
    }
}

void test_reject_tag_mismatch() {
    IngestionValidator validator(make_schema(), make_config());
    auto request = make_request();
    request.tables[0].tags = RowType{std::string("d1001")};
    try {
        validator.validate(request);
        assert(false && "tag count mismatch should throw");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("tag count mismatch") != std::string::npos);
    }
}

void test_reject_duplicate_table_inconsistent_tags() {
    IngestionValidator validator(make_schema(), make_config());
    auto request = make_request();
    auto table2 = request.tables[0];
    table2.tags = RowType{std::string("d1001"), std::string("west")};
    request.tables.push_back(table2);
    try {
        validator.validate(request);
        assert(false && "inconsistent duplicate tags should throw");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("inconsistent tags") != std::string::npos);
    }
}

void test_reject_timestamp_mixed_values() {
    IngestionValidator validator(make_schema(), make_config());
    auto request = make_request();
    request.tables[0].timestamps.string_values.push_back("2026-08-13 10:00:00.000");
    try {
        validator.validate(request);
        assert(false && "mixed timestamp arrays should throw");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("cannot contain both") != std::string::npos);
    }
}

} // namespace

int main() {
    test_validate_success();
    test_validate_without_auto_create_allows_schema_tags();
    test_reject_tags_without_auto_create();
    test_reject_missing_tags_with_auto_create();
    test_reject_column_count_mismatch();
    test_reject_row_count_mismatch();
    test_reject_per_table_capacity_overflow();
    test_reject_type_mismatch();
    test_reject_tag_mismatch();
    test_reject_duplicate_table_inconsistent_tags();
    test_reject_timestamp_mixed_values();
    std::cout << "All IngestionValidator tests passed.\n";
    return 0;
}
