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

    pool.release_block(block);

    std::cout << "test_memory_pool_basic passed." << std::endl;
}

void test_memory_pool_multi_batch() {
    ColumnConfigInstanceVector col_instances;
    col_instances.emplace_back(ColumnConfig{"col1", "INT"});
    col_instances.emplace_back(ColumnConfig{"col2", "VARCHAR(8)"});

    MemoryPool pool(1, 2, 2, col_instances);

    MultiBatch batch;
    batch.start_time = 1000;
    batch.end_time = 1010;
    batch.total_rows = 4;
    batch.table_batches.push_back({"t1", { {1000, {1, std::string("a")}}, {1010, {2, std::string("b")}} }});
    batch.table_batches.push_back({"t2", { {1000, {3, std::string("c")}}, {1010, {4, std::string("d")}} }});

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

int main() {
    test_memory_pool_basic();
    test_memory_pool_multi_batch();
    test_memory_pool_acquire_release();

    std::cout << "All MemoryPool tests passed." << std::endl;
    return 0;
}