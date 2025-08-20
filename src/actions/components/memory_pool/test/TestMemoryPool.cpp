#include "MemoryPool.hpp"
#include <cassert>
#include <iostream>
#include <string>
#include <vector>

void test_memory_pool_basic() {
    ColumnConfigInstanceVector col_instances;
    col_instances.emplace_back(ColumnConfig{"col1", "INT"});
    col_instances.emplace_back(ColumnConfig{"col2", "VARCHAR(16)"});

    MemoryPool pool(1, 1, 4, col_instances);

    auto* block = pool.acquire_block();
    assert(block != nullptr);
    assert(block->tables.size() == 1);
    assert(block->tables[0].columns.size() == 2);

    // Write a row of data
    RowData row;
    row.timestamp = 1234567890;
    row.columns = {int32_t(42), std::string("hello")};
    block->tables[0].add_row(row);

    // Verify the write
    assert(block->tables[0].timestamps[0] == 1234567890);
    auto& col1 = block->tables[0].columns[0];
    int32_t* col1_data = static_cast<int32_t*>(col1.fixed_data);
    (void)col1_data;
    assert(col1_data[0] == 42);
    auto& col2 = block->tables[0].columns[1];
    std::string str(col2.var_data, col2.lengths[0]);
    assert(str == "hello");

    // Test get_cell
    ColumnType cell0 = block->tables[0].get_cell(0, 0);
    assert(std::get<int32_t>(cell0) == 42);
    ColumnType cell1 = block->tables[0].get_cell(0, 1);
    assert(std::get<std::string>(cell1) == "hello");

    // Test get_cell_as_string
    std::string cell0_str = block->tables[0].get_cell_as_string(0, 0);
    std::string cell1_str = block->tables[0].get_cell_as_string(0, 1);
    assert(cell0_str.find("42") != std::string::npos);
    assert(cell1_str == "hello");

    pool.release_block(block);

    std::cout << "test_memory_pool_basic passed." << std::endl;
}

void test_memory_pool_multi_batch() {
    ColumnConfigInstanceVector col_instances;
    col_instances.emplace_back(ColumnConfig{"col1", "INT"});
    col_instances.emplace_back(ColumnConfig{"col2", "VARCHAR(8)"});

    MemoryPool pool(1, 2, 2, col_instances);

    MultiBatch batch;
    batch.table_batches.push_back({"t1", { {1000, {1, std::string("a")}}, {1010, {2, std::string("b")}} }});
    batch.table_batches.push_back({"t2", { {1000, {3, std::string("c")}}, {1010, {4, std::string("d")}} }});
    batch.update_metadata();

    auto* block = pool.convert_to_memory_block(std::move(batch));
    assert(block != nullptr);
    assert(block->used_tables == 2);

    // Validate table 1
    const auto& t1 = block->tables[0];
    assert(std::string(t1.table_name) == "t1");
    assert(t1.used_rows == 2);
    int32_t* t1_col1 = static_cast<int32_t*>(t1.columns[0].fixed_data);
    (void)t1_col1;
    assert(t1_col1[0] == 1 && t1_col1[1] == 2);
    std::string t1_col2_0(t1.columns[1].var_data + t1.columns[1].var_offsets[0], t1.columns[1].lengths[0]);
    std::string t1_col2_1(t1.columns[1].var_data + t1.columns[1].var_offsets[1], t1.columns[1].lengths[1]);
    assert(t1_col2_0 == "a" && t1_col2_1 == "b");

    // Validate get_cell and get_cell_as_string
    assert(std::get<int32_t>(t1.get_cell(0, 0)) == 1);
    assert(std::get<int32_t>(t1.get_cell(1, 0)) == 2);
    assert(std::get<std::string>(t1.get_cell(0, 1)) == "a");
    assert(std::get<std::string>(t1.get_cell(1, 1)) == "b");
    assert(t1.get_cell_as_string(0, 0) == "1");
    assert(t1.get_cell_as_string(1, 0) == "2");
    assert(t1.get_cell_as_string(0, 1) == "a");
    assert(t1.get_cell_as_string(1, 1) == "b");

    // Validate table 2
    const auto& t2 = block->tables[1];
    assert(std::string(t2.table_name) == "t2");
    assert(t2.used_rows == 2);
    int32_t* t2_col1 = static_cast<int32_t*>(t2.columns[0].fixed_data);
    (void)t2_col1;
    assert(t2_col1[0] == 3 && t2_col1[1] == 4);
    std::string t2_col2_0(t2.columns[1].var_data + t2.columns[1].var_offsets[0], t2.columns[1].lengths[0]);
    std::string t2_col2_1(t2.columns[1].var_data + t2.columns[1].var_offsets[1], t2.columns[1].lengths[1]);
    assert(t2_col2_0 == "c" && t2_col2_1 == "d");

    // Validate get_cell and get_cell_as_string
    assert(std::get<int32_t>(t2.get_cell(0, 0)) == 3);
    assert(std::get<int32_t>(t2.get_cell(1, 0)) == 4);
    assert(std::get<std::string>(t2.get_cell(0, 1)) == "c");
    assert(std::get<std::string>(t2.get_cell(1, 1)) == "d");
    assert(t2.get_cell_as_string(0, 0) == "3");
    assert(t2.get_cell_as_string(1, 0) == "4");
    assert(t2.get_cell_as_string(0, 1) == "c");
    assert(t2.get_cell_as_string(1, 1) == "d");

    pool.release_block(block);

    std::cout << "test_memory_pool_multi_batch passed." << std::endl;
}

void test_memory_pool_acquire_release() {
    ColumnConfigInstanceVector col_instances;
    col_instances.emplace_back(ColumnConfig{"col1", "INT"});
    MemoryPool pool(1, 1, 1, col_instances);

    auto* block1 = pool.acquire_block();
    assert(block1 != nullptr);
    pool.release_block(block1);

    auto* block2 = pool.acquire_block();
    assert(block2 == block1); // Should be the same block reused
    pool.release_block(block2);

    std::cout << "test_memory_pool_acquire_release passed." << std::endl;
}

void test_memory_pool_get_cell_out_of_range() {
    ColumnConfigInstanceVector col_instances;
    col_instances.emplace_back(ColumnConfig{"col1", "INT"});
    MemoryPool pool(1, 1, 1, col_instances);

    auto* block = pool.acquire_block();
    RowData row;
    row.timestamp = 1;
    row.columns = {int32_t(1)};
    block->tables[0].add_row(row);

    bool caught = false;
    try {
        block->tables[0].get_cell(1, 0); // row_index out of range
    } catch (const std::out_of_range&) {
        caught = true;
    }
    assert(caught);

    caught = false;
    try {
        block->tables[0].get_cell(0, 1); // col_index out of range
    } catch (const std::out_of_range&) {
        caught = true;
    }
    assert(caught);

    pool.release_block(block);
    std::cout << "test_memory_pool_get_cell_out_of_range passed." << std::endl;
}

void test_memory_pool_get_cell_null() {
    std::cout << "test_memory_pool_get_cell_null skipped (not implemented)." << std::endl;
}

int main() {
    test_memory_pool_basic();
    test_memory_pool_multi_batch();
    test_memory_pool_acquire_release();
    test_memory_pool_get_cell_out_of_range();
    test_memory_pool_get_cell_null();

    std::cout << "All MemoryPool tests passed." << std::endl;
    return 0;
}