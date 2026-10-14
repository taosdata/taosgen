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

#include "StmtV2Data.hpp"
#include <cassert>
#include <cstring>
#include <iostream>

void test_stmt_v2_data_fixed_null_none() {
    ColumnConfigInstanceVector col_instances;
    col_instances.emplace_back(ColumnConfig{"val", "INT"});

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1000, {int32_t(42)}});       // normal
    rows.push_back({2000, {NullValue{}}});        // NULL
    rows.push_back({3000, {NoneValue{}}});        // NONE
    rows.push_back({4000, {int32_t(99)}});        // normal
    batch.table_batches.emplace_back("t1", std::move(rows));
    batch.update_metadata();

    StmtV2Data data(col_instances, std::move(batch));

    assert(data.row_count() == 4);
    assert(data.column_count() == 1);

    const auto* bindv = data.bindv_ptr();
    assert(bindv != nullptr);
    assert(bindv->count == 1);

    // Column bind: index 0 = timestamp, index 1 = val
    const auto& col_bind = bindv->bind_cols[0][1];
    (void)col_bind;
    assert(col_bind.num == 4);

    // Verify is_null array: normal=0, NULL=1, NONE=2, normal=0
    assert(col_bind.is_null[0] == 0);
    assert(col_bind.is_null[1] == 1);
    assert(col_bind.is_null[2] == 2);
    assert(col_bind.is_null[3] == 0);

    // Verify buffer values for normal rows
    const int32_t* buf = static_cast<const int32_t*>(col_bind.buffer);
    (void)buf;
    assert(buf[0] == 42);
    assert(buf[3] == 99);

    std::cout << "test_stmt_v2_data_fixed_null_none passed." << std::endl;
}

void test_stmt_v2_data_varlen_null_none() {
    ColumnConfigInstanceVector col_instances;
    col_instances.emplace_back(ColumnConfig{"desc", "VARCHAR(32)"});

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1000, {std::string("hello")}});   // normal
    rows.push_back({2000, {NullValue{}}});             // NULL
    rows.push_back({3000, {NoneValue{}}});             // NONE
    rows.push_back({4000, {std::string("world")}});    // normal
    batch.table_batches.emplace_back("t1", std::move(rows));
    batch.update_metadata();

    StmtV2Data data(col_instances, std::move(batch));

    assert(data.row_count() == 4);

    const auto* bindv = data.bindv_ptr();
    const auto& col_bind = bindv->bind_cols[0][1];
    (void)col_bind;
    assert(col_bind.num == 4);

    // Verify is_null: normal=0, NULL=1, NONE=2, normal=0
    assert(col_bind.is_null[0] == 0);
    assert(col_bind.is_null[1] == 1);
    assert(col_bind.is_null[2] == 2);
    assert(col_bind.is_null[3] == 0);

    // Verify lengths: normal has data, NULL/NONE have 0
    assert(col_bind.length[0] == 5);  // "hello"
    assert(col_bind.length[1] == 0);  // NULL
    assert(col_bind.length[2] == 0);  // NONE
    assert(col_bind.length[3] == 5);  // "world"

    std::cout << "test_stmt_v2_data_varlen_null_none passed." << std::endl;
}

void test_stmt_v2_data_mixed_columns_null_none() {
    ColumnConfigInstanceVector col_instances;
    col_instances.emplace_back(ColumnConfig{"val", "FLOAT"});
    col_instances.emplace_back(ColumnConfig{"name", "VARCHAR(16)"});

    MultiBatch batch;
    std::vector<RowData> rows;
    // Row 0: normal float, NULL string
    rows.push_back({1000, {3.14f, NullValue{}}});
    // Row 1: NONE float, normal string
    rows.push_back({2000, {NoneValue{}, std::string("test")}});
    // Row 2: NULL float, NONE string
    rows.push_back({3000, {NullValue{}, NoneValue{}}});
    batch.table_batches.emplace_back("t1", std::move(rows));
    batch.update_metadata();

    StmtV2Data data(col_instances, std::move(batch));
    assert(data.row_count() == 3);
    assert(data.column_count() == 2);

    const auto* bindv = data.bindv_ptr();

    // Fixed column (float): index 1
    const auto& float_bind = bindv->bind_cols[0][1];
    (void)float_bind;
    assert(float_bind.is_null[0] == 0);  // normal
    assert(float_bind.is_null[1] == 2);  // NONE
    assert(float_bind.is_null[2] == 1);  // NULL

    // Var-len column (varchar): index 2
    const auto& str_bind = bindv->bind_cols[0][2];
    (void)str_bind;
    assert(str_bind.is_null[0] == 1);  // NULL
    assert(str_bind.is_null[1] == 0);  // normal
    assert(str_bind.is_null[2] == 2);  // NONE

    assert(str_bind.length[0] == 0);  // NULL
    assert(str_bind.length[1] == 4);  // "test"
    assert(str_bind.length[2] == 0);  // NONE

    std::cout << "test_stmt_v2_data_mixed_columns_null_none passed." << std::endl;
}

void test_stmt_v2_data_all_null_none_rows() {
    ColumnConfigInstanceVector col_instances;
    col_instances.emplace_back(ColumnConfig{"val", "INT"});
    col_instances.emplace_back(ColumnConfig{"desc", "VARCHAR(16)"});

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1000, {NullValue{}, NullValue{}}});
    rows.push_back({2000, {NoneValue{}, NoneValue{}}});
    batch.table_batches.emplace_back("t1", std::move(rows));
    batch.update_metadata();

    StmtV2Data data(col_instances, std::move(batch));
    assert(data.row_count() == 2);

    const auto* bindv = data.bindv_ptr();

    // Fixed column
    const auto& int_bind = bindv->bind_cols[0][1];
    (void)int_bind;
    assert(int_bind.is_null[0] == 1);
    assert(int_bind.is_null[1] == 2);

    // Var-len column
    const auto& str_bind = bindv->bind_cols[0][2];
    (void)str_bind;
    assert(str_bind.is_null[0] == 1);
    assert(str_bind.is_null[1] == 2);
    assert(str_bind.length[0] == 0);
    assert(str_bind.length[1] == 0);

    std::cout << "test_stmt_v2_data_all_null_none_rows passed." << std::endl;
}

void test_stmt_v2_data_multi_table_null_none() {
    ColumnConfigInstanceVector col_instances;
    col_instances.emplace_back(ColumnConfig{"val", "INT"});

    MultiBatch batch;
    std::vector<RowData> rows1;
    rows1.push_back({1000, {NullValue{}}});
    rows1.push_back({2000, {int32_t(10)}});
    batch.table_batches.emplace_back("t1", std::move(rows1));

    std::vector<RowData> rows2;
    rows2.push_back({3000, {NoneValue{}}});
    rows2.push_back({4000, {int32_t(20)}});
    batch.table_batches.emplace_back("t2", std::move(rows2));

    batch.update_metadata();

    StmtV2Data data(col_instances, std::move(batch));
    assert(data.row_count() == 4);

    const auto* bindv = data.bindv_ptr();
    (void)bindv;
    assert(bindv->count == 2);

    // Table 1
    assert(bindv->bind_cols[0][1].is_null[0] == 1);  // NULL
    assert(bindv->bind_cols[0][1].is_null[1] == 0);  // normal

    // Table 2
    assert(bindv->bind_cols[1][1].is_null[0] == 2);  // NONE
    assert(bindv->bind_cols[1][1].is_null[1] == 0);  // normal

    std::cout << "test_stmt_v2_data_multi_table_null_none passed." << std::endl;
}

void test_stmt_v2_data_timestamp_column_bind_type() {
    ColumnConfigInstanceVector col_instances;
    col_instances.emplace_back(ColumnConfig{"event_time", "TIMESTAMP"});

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1000, {int64_t(1700000000000)}});
    rows.push_back({2000, {int64_t(1700000001000)}});
    batch.table_batches.emplace_back("t1", std::move(rows));
    batch.update_metadata();

    StmtV2Data data(col_instances, std::move(batch));

    const auto* bindv = data.bindv_ptr();
    assert(bindv != nullptr);
    assert(bindv->count == 1);
    assert(bindv->bind_cols[0][0].buffer_type == TSDB_DATA_TYPE_TIMESTAMP);
    assert(bindv->bind_cols[0][1].buffer_type == TSDB_DATA_TYPE_TIMESTAMP);

    const int64_t* col_buf = static_cast<const int64_t*>(bindv->bind_cols[0][1].buffer);
    (void)col_buf;
    assert(col_buf[0] == 1700000000000LL);
    assert(col_buf[1] == 1700000001000LL);

    std::cout << "test_stmt_v2_data_timestamp_column_bind_type passed." << std::endl;
}

int main() {
    test_stmt_v2_data_fixed_null_none();
    test_stmt_v2_data_varlen_null_none();
    test_stmt_v2_data_mixed_columns_null_none();
    test_stmt_v2_data_all_null_none_rows();
    test_stmt_v2_data_multi_table_null_none();
    test_stmt_v2_data_timestamp_column_bind_type();

    std::cout << "All StmtV2Data tests passed." << std::endl;
    return 0;
}
