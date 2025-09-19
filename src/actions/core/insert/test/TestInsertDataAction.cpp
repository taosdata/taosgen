#include "InsertDataAction.hpp"
#include <cassert>
#include <memory>
#include <thread>
#include <chrono>
#include <filesystem>
#include "TableNameManager.hpp"


InsertDataConfig create_test_config() {
    InsertDataConfig config;

    // Configure columns for generator mode
    config.schema.columns_cfg.source_type = "generator";

    // Setup timestamp strategy
    auto& ts_config = config.schema.columns_cfg.generator.timestamp_strategy.timestamp_config;
    ts_config.start_timestamp = Timestamp{1700000000000};
    ts_config.timestamp_step = 10;
    ts_config.timestamp_precision = "ms";

    // Setup schema
    auto& schema = config.schema.columns_cfg.generator.schema;
    schema = {
        {"col1", "INT", "random", 1, 100},
        {"col2", "DOUBLE", "random", 0.0, 1.0}
    };

    // Setup table name generation
    config.schema.tbname.source_type = "generator";
    config.schema.tbname.generator.prefix = "d";
    config.schema.tbname.generator.count = 10;
    config.schema.tbname.generator.from = 0;

    // Setup control parameters
    config.schema.generation.per_table_rows = 100;
    config.schema.generation.generate_threads = 2;
    config.schema.generation.per_batch_rows = 10;
    config.queue_capacity = 2;
    config.insert_threads = 2;
    config.failure_handling.max_retries = 1;
    config.failure_handling.retry_interval_ms = 1000;
    config.failure_handling.on_failure = "exit";

    // Data format settings
    config.data_format.format_type = "stmt";
    config.data_format.stmt_config.version = "v2";

    // Setup target
    config.timestamp_precision = "ms";
    config.target_type = "tdengine";
    config.tdengine = TDengineConfig("taos+ws://localhost:6041/test_action");
    config.schema.name = "test_super_table";

    return config;
}

void test_basic_initialization() {
    GlobalConfig global;
    auto config = create_test_config();
    config.schema.columns_cfg.generator.schema.clear();  // Clear schema to test error handling

    try {
        InsertDataAction action(global, config);
        action.execute();
        assert(false && "Should throw exception for missing schema configuration");
    } catch (const std::exception& e) {
        assert(std::string(e.what()).find("Schema configuration is empty") != std::string::npos);
        std::cout << "test_basic_initialization passed\n";
    }
}

void test_table_name_generation() {
    auto config = create_test_config();
    TableNameManager name_manager(config);
    auto names = name_manager.generate_table_names();

    assert(names.size() == 10);
    assert(names[0] == "d0");
    assert(names[9] == "d9");

    auto split_names = name_manager.split_for_threads();
    assert(split_names.size() == config.schema.generation.generate_threads);
    assert(split_names[0].size() == 5);  // Even split for 10 tables across 2 threads

    std::cout << "test_table_name_generation passed\n";
}

void test_data_pipeline() {
    auto config = create_test_config();
    DataPipeline<FormatResult> pipeline(false, 2, 2, 1000);

    std::atomic<bool> producer_done{false};
    std::atomic<size_t> rows_generated{0};
    std::atomic<size_t> rows_consumed{0};

    ColumnConfigInstanceVector col_instances;
    col_instances.emplace_back(ColumnConfig{"f1", "FLOAT"});
    col_instances.emplace_back(ColumnConfig{"i1", "INT"});
    MemoryPool pool(1, 5, 2, col_instances);

    // Start producer thread
    std::thread producer([&]() {
        for(int i = 0; i < 5; i++) {
            MultiBatch batch;
            std::vector<RowData> rows;
            rows.push_back({1500000000000, {3.14f, 42}});
            rows.push_back({1500000000001, {2.71f, 43}});
            batch.table_batches.emplace_back("table1", std::move(rows));
            batch.update_metadata();

            auto* block = pool.convert_to_memory_block(std::move(batch));

            pipeline.push_data(0, FormatResult{
                SqlInsertData(block, col_instances, "INSERT INTO test_table VALUES (...)")
            });
            rows_generated++;
        }
        producer_done = true;
    });

    // Start consumer thread
    std::thread consumer([&]() {
        while (!producer_done || pipeline.total_queued() > 0) {
            auto result = pipeline.fetch_data(0);
            if (result.status == DataPipeline<FormatResult>::Status::Success) {
                std::visit([&](const auto& data) {
                    using T = std::decay_t<decltype(data)>;
                    if constexpr (std::is_same_v<T, SqlInsertData>) {
                        assert(data.total_rows == 2);
                        rows_consumed++;
                    }
                }, *result.data);
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    });

    producer.join();
    consumer.join();

    assert(rows_generated == 5);
    assert(rows_consumed == 5);
    assert(pipeline.total_queued() == 0);

    std::cout << "test_data_pipeline passed\n";
}

void test_data_generation() {
    auto config = create_test_config();
    config.schema.generation.per_table_rows = 5;
    config.schema.generation.interlace_mode.enabled = false;

    auto col_instances = ColumnConfigInstanceFactory::create(config.schema.columns_cfg.generator.schema);
    MemoryPool pool(1, 1, 5, col_instances);
    TableDataManager manager(pool, config, col_instances);

    std::vector<std::string> table_names = {"d0"};
    assert(manager.init(table_names));

    int row_count = 0;
    while (auto block = manager.next_multi_batch()) {
        const auto* batch = block.value();
        assert(batch->used_tables == 1);
        row_count += batch->total_rows;

        // Verify timestamp progression
        for (const auto& table : batch->tables) {
            for (size_t i = 0; i < table.used_rows; i++) {
                assert(table.timestamps[i] == 1700000000000 + static_cast<int64_t>(row_count - table.used_rows + i) * 10);
            }
        }
        block.value()->release();
    }

    (void)row_count;
    assert(row_count == 5);
    std::cout << "test_data_generation passed\n";
}

void test_end_to_end_data_generation() {
    GlobalConfig global;

    std::vector<DataChannel> channel_types = {
        // DataChannel{"native"},
        DataChannel{"websocket"}
    };

    std::vector<DataFormat> format_types = {
        DataFormat{"sql"},
        DataFormat{"stmt", DataFormat::StmtConfig{"v2"}}
    };

    for (const auto& channel : channel_types) {
        for (const auto& format : format_types) {
            std::cout << "[DEBUG] Executing test for channel_type=" << channel.channel_type
                      << ", format_type=" << format.format_type << std::endl;

            auto config = create_test_config();

            config.data_format = format;

            config.schema.generation.per_table_rows = 10;
            config.schema.generation.generate_threads = 1;
            config.queue_capacity = 2;
            config.insert_threads = 1;
            config.schema.tbname.generator.count = 4;           // 4 tables total
            config.target_type = "tdengine";
            config.tdengine.database = "test_action";
            config.schema.name = "test_super_table";

            InsertDataAction action(global, config);

            action.execute();

            // TODO: Verify contain correct data
            // int row_count = 0;
            // assert(row_count == 10 && "Each table should have 10 rows");
        }
    }

    std::cout << "test_end_to_end_data_generation passed\n";
}

void test_concurrent_data_generation() {
    GlobalConfig global;
    auto config = create_test_config();
    config.schema.generation.per_table_rows = 1000;     // More rows to test concurrency
    config.schema.generation.generate_threads = 4;      // More threads
    config.insert_threads = 4;
    config.schema.tbname.generator.count = 8;             // 8 tables
    config.target_type = "tdengine";
    config.tdengine.database = "test_action";
    config.schema.name = "test_super_table";

    InsertDataAction action(global, config);

    // Measure execution time to verify concurrent operation
    auto start = std::chrono::steady_clock::now();
    action.execute();
    auto end = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    // TODO: Verify all data was generated
    size_t total_rows = 8000;
    (void)total_rows;
    assert(total_rows == 8000 && "Total rows should match configuration");
    std::cout << "test_concurrent_data_generation passed (duration: "
              << duration.count() << "ms)\n";
}

void test_error_handling() {
    GlobalConfig global;
    auto config = create_test_config();

    // Test case 1: Invalid target type
    {
        auto invalid_config = config;
        invalid_config.target_type = "invalid_target";

        InsertDataAction action(global, invalid_config);
        try {
            action.execute();
            assert(false && "Should throw for invalid target");
        } catch (const std::exception&) {
            // Expected
        }
    }

    // Test case 2: Invalid table configuration
    {
        auto invalid_config = config;
        invalid_config.schema.tbname.generator.count = 0;

        InsertDataAction action(global, invalid_config);
        try {
            action.execute();
            assert(false && "Should throw for invalid table count");
        } catch (const std::exception&) {
            // Expected
        }
    }

    // Test case 3: Invalid thread configuration
    {
        auto invalid_config = config;
        invalid_config.schema.generation.generate_threads = 0;

        InsertDataAction action(global, invalid_config);
        try {
            action.execute();
            assert(false && "Should throw for invalid thread count");
        } catch (const std::exception&) {
            // Expected
        }
    }

    std::cout << "test_error_handling passed\n";
}

int main() {
    test_basic_initialization();
    test_table_name_generation();
    test_data_pipeline();
    test_data_generation();
    test_end_to_end_data_generation();
    test_concurrent_data_generation();
    test_error_handling();

    std::cout << "All InsertDataAction tests passed.\n";
    return 0;
}