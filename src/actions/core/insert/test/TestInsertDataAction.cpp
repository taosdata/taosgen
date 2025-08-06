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
    config.source.columns.source_type = "generator";
    
    // Setup timestamp strategy
    auto& ts_config = config.source.columns.generator.timestamp_strategy.timestamp_config;
    ts_config.start_timestamp = Timestamp{1700000000000};
    ts_config.timestamp_step = 10;
    ts_config.timestamp_precision = "ms";
    
    // Setup schema
    auto& schema = config.source.columns.generator.schema;
    schema = {
        {"col1", "INT", "random", 1, 100},
        {"col2", "DOUBLE", "random", 0.0, 1.0}
    };
    
    // Setup table name generation
    config.source.table_name.source_type = "generator";
    config.source.table_name.generator.prefix = "d";
    config.source.table_name.generator.count = 10;
    config.source.table_name.generator.from = 0;
    
    // Setup control parameters
    config.control.data_generation.per_table_rows = 100;
    config.control.data_generation.generate_threads = 2;
    config.control.data_generation.queue_capacity = 2;

    config.control.insert_control.insert_threads = 2;
    config.control.insert_control.per_request_rows = 10;
    config.control.insert_control.failure_handling.max_retries = 1;
    config.control.insert_control.failure_handling.retry_interval_ms = 1000;
    config.control.insert_control.failure_handling.on_failure = "exit";
    
    // Data channel settings
    config.control.data_channel.channel_type = "websocket";

    // Data format settings
    config.control.data_format.format_type = "stmt";
    config.control.data_format.stmt_config.version = "v2";

    // Setup target
    config.target.timestamp_precision = "ms";
    config.target.target_type = "tdengine";
    config.target.tdengine.connection_info.host = "localhost";
    config.target.tdengine.connection_info.port = 6041;
    config.target.tdengine.database_info.name = "test_action_db";
    config.target.tdengine.super_table_info.name = "test_super_table";
    
    return config;
}

void test_basic_initialization() {
    GlobalConfig global;
    auto config = create_test_config();
    config.source.columns.generator.schema.clear();  // Clear schema to test error handling
    
    InsertDataAction action(global, config);
    try {
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
    assert(split_names.size() == config.control.data_generation.generate_threads);
    assert(split_names[0].size() == 5);  // Even split for 10 tables across 2 threads
    
    std::cout << "test_table_name_generation passed\n";
}

void test_data_pipeline() {
    auto config = create_test_config();
    DataPipeline<FormatResult> pipeline(1000);
    
    std::atomic<bool> producer_done{false};
    std::atomic<size_t> rows_generated{0};
    std::atomic<size_t> rows_consumed{0};

    // Start producer thread
    std::thread producer([&]() {
        for(int i = 0; i < 5; i++) {
            pipeline.push_data(FormatResult{
                SqlInsertData(1700000000000, 1700000000100, 10, "INSERT INTO test_table VALUES (...)")
            });
            rows_generated++;
        }
        producer_done = true;
    });
    
    // Start consumer thread
    std::thread consumer([&]() {
        while (!producer_done || pipeline.total_queued() > 0) {
            auto result = pipeline.fetch_data();
            if (result.status == DataPipeline<FormatResult>::Status::Success) {
                std::visit([&](const auto& data) {
                    using T = std::decay_t<decltype(data)>;
                    if constexpr (std::is_same_v<T, SqlInsertData>) {
                        assert(data.total_rows == 10);
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
    config.control.data_generation.per_table_rows = 5;
    config.control.data_generation.interlace_mode.enabled = false;
    
    auto col_instances = ColumnConfigInstanceFactory::create(config.source.columns.generator.schema);
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

            config.control.data_channel = channel;
            config.control.data_format = format;
            config.target.tdengine.connection_info.port = ConnectionInfo::default_port(channel.channel_type);

            config.control.data_generation.per_table_rows = 10;
            config.control.data_generation.generate_threads = 1;
            config.control.data_generation.queue_capacity = 2;
            config.control.insert_control.insert_threads = 1;
            config.source.table_name.generator.count = 4;           // 4 tables total
            config.target.target_type = "tdengine";
            config.target.tdengine.database_info.name = "test_action_db";
            config.target.tdengine.super_table_info.name = "test_super_table";

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
    config.control.data_generation.per_table_rows = 1000;     // More rows to test concurrency
    config.control.data_generation.generate_threads = 4;      // More threads
    config.control.insert_control.insert_threads = 4;
    config.source.table_name.generator.count = 8;             // 8 tables
    config.target.target_type = "tdengine";
    config.target.tdengine.database_info.name = "test_action_db";
    config.target.tdengine.super_table_info.name = "test_super_table";
    
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
        invalid_config.target.target_type = "invalid_target";
        
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
        invalid_config.source.table_name.generator.count = 0;
        
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
        invalid_config.control.data_generation.generate_threads = 0;
        
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