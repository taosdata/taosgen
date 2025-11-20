#include <iostream>
#include <cassert>
#include "FormatterRegistrar.hpp"
#include "MqttInsertDataFormatter.hpp"

// Helper to create a base config for tests
InsertDataConfig create_base_config() {
    InsertDataConfig config;
    config.data_format.format_type = "mqtt";
    auto& mqtt_format = config.data_format.mqtt;
    mqtt_format.records_per_message = 2;
    mqtt_format.topic = "test/topic";
    mqtt_format.compression = "none";
    mqtt_format.encoding = "utf8";
    mqtt_format.content_type = "json";
    return config;
}

void test_mqtt_format_insert_data_single_table() {
    auto config = create_base_config();

    ColumnConfigInstanceVector col_instances;
    col_instances.emplace_back(ColumnConfig{"f1", "FLOAT"});
    col_instances.emplace_back(ColumnConfig{"i1", "INT"});

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1500000000000, {3.14f, 42}});
    rows.push_back({1500000000001, {2.71f, 43}});
    batch.table_batches.emplace_back("table1", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 2, col_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));

    MqttInsertDataFormatter formatter(config.data_format);
    FormatResult result = formatter.format(config, col_instances, block);

    assert(std::holds_alternative<MqttInsertData>(result));
    const auto& msg_data = std::get<MqttInsertData>(result);
    (void)msg_data;
    assert(msg_data.start_time == 1500000000000);
    assert(msg_data.end_time == 1500000000001);
    assert(msg_data.total_rows == 2);
    assert(msg_data.data.size() == 1);  // records_per_message=2, 2 rows => 1 message
    std::cout << "test_mqtt_format_insert_data_single_table passed!" << std::endl;
}

void test_mqtt_format_insert_data_multiple_tables() {
    auto config = create_base_config();

    ColumnConfigInstanceVector col_instances;
    col_instances.emplace_back(ColumnConfig{"f1", "FLOAT"});
    col_instances.emplace_back(ColumnConfig{"i1", "INT"});

    MultiBatch batch;
    // First table
    std::vector<RowData> rows1;
    rows1.push_back({1500000000000, {3.14f, 42}});
    rows1.push_back({1500000000001, {2.71f, 43}});
    batch.table_batches.emplace_back("table1", std::move(rows1));

    // Second table
    std::vector<RowData> rows2;
    rows2.push_back({1500000000002, {1.23f, 44}});
    rows2.push_back({1500000000003, {4.56f, 45}});
    batch.table_batches.emplace_back("table2", std::move(rows2));

    batch.update_metadata();

    MemoryPool pool(1, 2, 2, col_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));

    auto formatter = FormatterFactory::instance().create_formatter<InsertDataConfig>(config.data_format);
    FormatResult result = formatter->format(config, col_instances, block);

    assert(std::holds_alternative<MqttInsertData>(result));
    const auto& msg_data = std::get<MqttInsertData>(result);
    (void)msg_data;
    assert(msg_data.start_time == 1500000000000);
    assert(msg_data.end_time == 1500000000003);
    assert(msg_data.total_rows == 4);
    assert(msg_data.data.size() == 2);  // 4 rows, records_per_message=2 => 2 messages
    std::cout << "test_mqtt_format_insert_data_multiple_tables passed!" << std::endl;
}

void test_mqtt_format_insert_data_empty_batch() {
    auto config = create_base_config();
    ColumnConfigInstanceVector col_instances;
    col_instances.emplace_back(ColumnConfig{"f1", "FLOAT"});

    MultiBatch batch;

    MemoryPool pool(1, 1, 1, col_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));
    (void)block;
    assert(block == nullptr);

    std::vector<RowData> rows;
    rows.push_back({1500000000000, {3.14f}});
    batch.table_batches.emplace_back("table1", std::move(rows));
    batch.update_metadata();

    block = pool.convert_to_memory_block(std::move(batch));
    assert(block != nullptr);

    MqttInsertDataFormatter formatter(config.data_format);
    FormatResult result = formatter.format(config, col_instances, block);

    assert(std::holds_alternative<MqttInsertData>(result));
    const auto& msg_data = std::get<MqttInsertData>(result);
    (void)msg_data;
    assert(msg_data.total_rows == 1);

    // Test with a null memory block
    FormatResult result_for_null = formatter.format(config, col_instances, nullptr);
    assert(std::holds_alternative<std::string>(result_for_null));
    assert(std::get<std::string>(result_for_null).empty());

    std::cout << "test_mqtt_format_insert_data_empty_batch passed!" << std::endl;
}

void test_mqtt_format_insert_data_with_empty_rows() {
    auto config = create_base_config();

    ColumnConfigInstanceVector col_instances;
    col_instances.emplace_back(ColumnConfig{"f1", "FLOAT"});
    col_instances.emplace_back(ColumnConfig{"i1", "INT"});

    MultiBatch batch;
    // Table with data
    std::vector<RowData> rows1;
    rows1.push_back({1500000000000, {3.14f, 42}});
    rows1.push_back({1500000000001, {2.71f, 43}});
    batch.table_batches.emplace_back("table1", std::move(rows1));

    // Table without data
    std::vector<RowData> empty_rows;
    batch.table_batches.emplace_back("table2", std::move(empty_rows));

    // Another table with data
    std::vector<RowData> rows3;
    rows3.push_back({1500000000002, {1.23f, 44}});
    batch.table_batches.emplace_back("table3", std::move(rows3));

    batch.update_metadata();

    MemoryPool pool(1, 3, 2, col_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));


    auto formatter = FormatterFactory::instance().create_formatter<InsertDataConfig>(config.data_format);
    FormatResult result = formatter->format(config, col_instances, block);

    assert(std::holds_alternative<MqttInsertData>(result));
    const auto& msg_data = std::get<MqttInsertData>(result);

    // Verify the timing information
    (void)msg_data;
    assert(msg_data.start_time == 1500000000000);
    assert(msg_data.end_time == 1500000000002);

    // Verify total rows
    assert(msg_data.total_rows == 3);           // 2 rows from table1 + 1 row from table3
    assert(msg_data.data.size() == 2);          // 3 rows, records_per_message=2 => 2 messages (one full, one partial)

    std::cout << "test_mqtt_format_insert_data_with_empty_rows passed!" << std::endl;
}

int main() {
    test_mqtt_format_insert_data_single_table();
    test_mqtt_format_insert_data_multiple_tables();
    test_mqtt_format_insert_data_empty_batch();
    test_mqtt_format_insert_data_with_empty_rows();
    std::cout << "All mqtt formatter tests passed!" << std::endl;
    return 0;
}