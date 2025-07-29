#include "TableDataManager.hpp"
#include <cassert>
#include <iostream>

InsertDataConfig create_test_config() {
    InsertDataConfig config;
    
    // Configure columns for generator mode
    config.source.columns.source_type = "generator";
    
    // Setup timestamp strategy
    auto& ts_config = config.source.columns.generator.timestamp_strategy.timestamp_config;
    ts_config.start_timestamp = Timestamp{1000};
    ts_config.timestamp_step = 10;
    ts_config.timestamp_precision = "ms";
    
    // Setup schema
    auto& schema = config.source.columns.generator.schema;
    schema = {
        {"col1", "INT", "random", 1, 100},
        {"col2", "FLOAT", "random", 0.0, 1.0}
    };
    
    // Setup control parameters
    config.control.data_generation.per_table_rows = 100;
    config.control.insert_control.per_request_rows = 10;
    config.target.timestamp_precision = "ms";
    
    return config;
}

void test_init_with_empty_tables() {
    auto config = create_test_config();
    auto col_instances = ColumnConfigInstanceFactory::create(config.source.columns.generator.schema);
    MemoryPool pool(1, 1, 1, col_instances);
    TableDataManager manager(pool, config, col_instances);
    
    assert(!manager.init({}));
    std::cout << "test_init_with_empty_tables passed.\n";
}

void test_init_with_valid_tables() {
    auto config = create_test_config();
    auto col_instances = ColumnConfigInstanceFactory::create(config.source.columns.generator.schema);
    MemoryPool pool(1, 2, 1, col_instances);
    TableDataManager manager(pool, config, col_instances);
    
    std::vector<std::string> table_names = {"test_table_1", "test_table_2"};
    assert(manager.init(table_names));
    assert(manager.current_table() == "test_table_1");
    
    // Verify table states initialization
    const auto& states = manager.table_states();
    (void)states;
    assert(states.size() == 2);
    assert(states[0].table_name == "test_table_1");
    assert(states[1].table_name == "test_table_2");
    
    std::cout << "test_init_with_valid_tables passed.\n";
}

void test_has_more() {
    auto config = create_test_config();
    config.control.data_generation.per_table_rows = 5;
    auto col_instances = ColumnConfigInstanceFactory::create(config.source.columns.generator.schema);
    MemoryPool pool(1, 1, 5, col_instances);
    TableDataManager manager(pool, config, col_instances);
    
    std::vector<std::string> table_names = {"test_table"};
    assert(manager.init(table_names));
    
    int rows_generated = 0;
    while (manager.has_more()) {
        auto block = manager.next_multi_batch();
        assert(block);
        rows_generated += block.value()->total_rows;
        block.value()->release();
    }
    
    const auto& states = manager.table_states();
    (void)states;
    assert(states.size() == 1);
    assert(states[0].rows_generated == 5);
    
    std::cout << "test_has_more passed.\n";
}

void test_table_completion() {
    auto config = create_test_config();
    config.control.data_generation.per_table_rows = 2;
    auto col_instances = ColumnConfigInstanceFactory::create(config.source.columns.generator.schema);
    MemoryPool pool(1, 2, 2, col_instances);
    TableDataManager manager(pool, config, col_instances);
    
    std::vector<std::string> table_names = {"test_table_1", "test_table_2"};
    assert(manager.init(table_names));
    
    while (manager.has_more()) {
        auto block = manager.next_multi_batch();
        assert(block);
        block.value()->release();
    }
    
    const auto& states = manager.table_states();
    for (const auto& state : states) {
        (void)state;
        assert(state.completed  || state.rows_generated >= config.control.data_generation.per_table_rows);
        assert(state.rows_generated == 2);
    }
    
    std::cout << "test_table_completion passed.\n";
}

void test_data_generation_basic() {
    auto config = create_test_config();
    config.control.data_generation.per_table_rows = 5;
    config.control.data_generation.interlace_mode.enabled = false;
    auto col_instances = ColumnConfigInstanceFactory::create(config.source.columns.generator.schema);
    MemoryPool pool(1, 1, 5, col_instances);
    TableDataManager manager(pool, config, col_instances);
    
    std::vector<std::string> table_names = {"test_table_1"};
    assert(manager.init(table_names));
    
    // Verify generated data
    int row_count = 0;
    int64_t last_timestamp = 0;
    
    while (auto block = manager.next_multi_batch()) {
        const auto* batch = block.value();
        for (const auto& table : batch->tables) {
            assert(std::string(table.table_name) == "test_table_1");
            assert(table.used_rows == 5);
            assert(table.columns.size() == 2);

            for (size_t row_idx = 0; row_idx < table.used_rows; ++row_idx) {
                int64_t timestamp = table.timestamps[row_idx];
                assert(timestamp == 1000 + static_cast<int64_t >(row_idx) * 10);

                last_timestamp = timestamp;
                row_count++;
            }
        }
        block.value()->release();
    }
    
    (void)row_count;
    (void)last_timestamp;
    assert(row_count == 5);
    assert(last_timestamp == 1040);  // 1000 + (5-1)*10
    
    std::cout << "test_data_generation_basic passed.\n";
}

void test_data_generation_with_interlace() {
    auto config = create_test_config();
    config.control.data_generation.interlace_mode.enabled = true;
    config.control.data_generation.interlace_mode.rows = 2;
    config.control.data_generation.per_table_rows = 4;
    auto col_instances = ColumnConfigInstanceFactory::create(config.source.columns.generator.schema);
    MemoryPool pool(1, 2, 4, col_instances);
    TableDataManager manager(pool, config, col_instances);
    
    std::vector<std::string> table_names = {"test_table_1", "test_table_2"};
    assert(manager.init(table_names));
    
    // Verify batch generation with interlace mode
    auto block = manager.next_multi_batch();
    assert(block);

    const auto* batch = block.value();
    assert(batch->tables.size() == 2);
    const auto& table1 = batch->tables[0];
    const auto& table2 = batch->tables[1];
    (void)table1;
    (void)table2;
    assert(table1.used_rows == 2);
    assert(table2.used_rows == 2);

    // Verify timestamps of first batch
    assert(table1.timestamps[0] == 1000);
    assert(table1.timestamps[1] == 1010);
    assert(table2.timestamps[0] == 1000);
    assert(table2.timestamps[1] == 1010);

    block.value()->release();

    // Verify no more data available
    auto block2 = manager.next_multi_batch();
    assert(block2);

    auto batch2 = block2.value();
    assert(!manager.has_more());
    
    assert(batch2->tables.size() == 2);
    const auto& table1_2 = batch2->tables[0];
    const auto& table2_2 = batch2->tables[1];
    (void)table1_2;
    (void)table2_2;

    assert(table1_2.used_rows == 2);
    assert(table2_2.used_rows == 2);

    assert(table1_2.timestamps[0] == 1020);
    assert(table1_2.timestamps[1] == 1030);
    assert(table2_2.timestamps[0] == 1020);
    assert(table2_2.timestamps[1] == 1030);

    block2.value()->release();

    std::cout << "test_data_generation_with_interlace passed.\n";
}

void test_per_request_rows_limit() {
    auto config = create_test_config();
    config.control.insert_control.per_request_rows = 3;
    config.control.data_generation.per_table_rows = 10;
    config.control.data_generation.interlace_mode.enabled = false;
    auto col_instances = ColumnConfigInstanceFactory::create(config.source.columns.generator.schema);
    MemoryPool pool(1, 2, 10, col_instances);
    TableDataManager manager(pool, config, col_instances);
    
    std::vector<std::string> table_names = {"test_table_1", "test_table_2"};
    assert(manager.init(table_names));
    
    // Count total rows across all batches
    int total_rows = 0;
    while (manager.has_more()) {
        auto block = manager.next_multi_batch();
        assert(block);

        const auto* batch = block.value();
        assert(batch->total_rows <= 3);
        total_rows += batch->total_rows;

        block.value()->release();
    }
    
    assert(total_rows == 20);
    
    std::cout << "test_per_request_rows_limit passed.\n";
}

void test_data_generation_with_flow_control() {
    auto config = create_test_config();
    config.control.data_generation.flow_control.enabled = true;
    config.control.data_generation.flow_control.rate_limit = 100;  // 100 rows per second
    config.control.data_generation.per_table_rows = 5;
    auto col_instances = ColumnConfigInstanceFactory::create(config.source.columns.generator.schema);
    MemoryPool pool(1, 1, 5, col_instances);
    TableDataManager manager(pool, config, col_instances);
    
    std::vector<std::string> table_names = {"test_table_1"};
    assert(manager.init(table_names));
    
    auto start_time = std::chrono::steady_clock::now();
    
    while (auto block = manager.next_multi_batch()) {
        const auto* batch = block.value();

        for (const auto& table : batch->tables) {
            (void)table;
            assert(std::string(table.table_name) == "test_table_1");
            assert(table.used_rows > 0);
        }

        block.value()->release();
    }
    
    auto end_time = std::chrono::steady_clock::now();
    auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
    
    // With rate limit of 100 rows/s, 5 rows should take at least 50ms
    (void)duration_ms;
    assert(duration_ms >= 50);
    
    std::cout << "test_data_generation_with_flow_control passed.\n";
}

int main() {
    test_init_with_empty_tables();
    test_init_with_valid_tables();
    test_init_with_valid_tables();
    test_has_more();
    test_table_completion();
    test_data_generation_basic();
    test_data_generation_with_interlace();
    test_per_request_rows_limit();
    test_data_generation_with_flow_control();
    
    std::cout << "All tests passed.\n";
    return 0;
}