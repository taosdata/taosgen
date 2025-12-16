#include "FormatterRegistrar.hpp"
#include "MqttInsertDataFormatter.hpp"
#include <iostream>
#include <cassert>

// Helper to create a base config for tests
InsertDataConfig create_base_config() {
    InsertDataConfig config;
    config.data_format.format_type = "mqtt";
    auto& mqtt_format = config.data_format.mqtt;
    mqtt_format.records_per_message = 1;
    mqtt_format.topic = "telemetry/{table}";
    mqtt_format.compression = "none";
    mqtt_format.encoding = "utf8";
    mqtt_format.content_type = "json";
    mqtt_format.tbname_key = "table_name";
    return config;
}

void test_mqtt_format_single_record_per_message() {
    InsertDataConfig config = create_base_config();
    config.data_format.mqtt.records_per_message = 1;

    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"f1", "FLOAT"});
    col_instances.emplace_back(ColumnConfig{"i1", "INT"});

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1500000000000, {3.14f, 42}});
    rows.push_back({1500000000001, {2.71f, 43}});
    batch.table_batches.emplace_back("table1", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 2, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));

    MqttInsertDataFormatter formatter(config.data_format);
    FormatResult result = formatter.format(config, col_instances, tag_instances, block);

    assert(std::holds_alternative<MqttInsertData>(result));
    const auto& mqtt_data = std::get<MqttInsertData>(result);
    assert(mqtt_data.start_time == 1500000000000);
    assert(mqtt_data.end_time == 1500000000001);
    assert(mqtt_data.total_rows == 2);
    assert(mqtt_data.data.size() == 2);

    const auto& messages = mqtt_data.data;
    assert(messages.size() == 2);

    // Message 1
    assert(messages[0].first == "telemetry/table1");
    nlohmann::json payload1 = nlohmann::json::parse(messages[0].second);
    assert(payload1["ts"] == 1500000000000);
    assert(payload1["f1"] == 3.14f);
    assert(payload1["i1"] == 42);
    assert(payload1["table_name"] == "table1");

    // Message 2
    assert(messages[1].first == "telemetry/table1");
    nlohmann::json payload2 = nlohmann::json::parse(messages[1].second);
    assert(payload2["ts"] == 1500000000001);
    assert(payload2["f1"] == 2.71f);
    assert(payload2["i1"] == 43);
    assert(payload2["table_name"] == "table1");

    std::cout << "test_mqtt_format_single_record_per_message passed!" << std::endl;
}

void test_mqtt_format_multiple_records_per_message() {
    InsertDataConfig config = create_base_config();
    config.data_format.mqtt.records_per_message = 2;

    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"f1", "FLOAT"});

    MultiBatch batch;
    // Table 1
    std::vector<RowData> rows1;
    rows1.push_back({1500000000000, {1.1f}});
    rows1.push_back({1500000000001, {2.2f}});
    batch.table_batches.emplace_back("table1", std::move(rows1));
    // Table 2
    std::vector<RowData> rows2;
    rows2.push_back({1500000000002, {3.3f}});
    batch.table_batches.emplace_back("table2", std::move(rows2));
    batch.update_metadata();

    MemoryPool pool(1, 2, 2, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));

    MqttInsertDataFormatter formatter(config.data_format);
    FormatResult result = formatter.format(config, col_instances, tag_instances, block);

    assert(std::holds_alternative<MqttInsertData>(result));
    const auto& mqtt_data = std::get<MqttInsertData>(result);
    assert(mqtt_data.start_time == 1500000000000);
    assert(mqtt_data.end_time == 1500000000002);
    assert(mqtt_data.total_rows == 3);
    assert(mqtt_data.data.size() == 2);

    const auto& messages = mqtt_data.data;
    assert(messages.size() == 2);

    // Message 1 (full batch)
    assert(messages[0].first == "telemetry/table1");
    nlohmann::json payload1 = nlohmann::json::parse(messages[0].second);
    assert(payload1.is_array());
    assert(payload1.size() == 2);
    assert(payload1[0]["ts"] == 1500000000000);
    assert(payload1[0]["f1"] == 1.1f);
    assert(payload1[0]["table_name"] == "table1");
    assert(payload1[1]["ts"] == 1500000000001);
    assert(payload1[1]["f1"] == 2.2f);
    assert(payload1[1]["table_name"] == "table1");

    // Message 2 (partial batch)
    assert(messages[1].first == "telemetry/table2");
    nlohmann::json payload2 = nlohmann::json::parse(messages[1].second);
    assert(payload2.is_array());
    assert(payload2.size() == 1);
    assert(payload2[0]["ts"] == 1500000000002);
    assert(payload2[0]["f1"] == 3.3f);
    assert(payload2[0]["table_name"] == "table2");

    std::cout << "test_mqtt_format_multiple_records_per_message passed!" << std::endl;
}

void test_mqtt_format_empty_batch() {
    auto config = create_base_config();

    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"f1", "FLOAT"});

    MultiBatch batch;
    MemoryPool pool(1, 1, 1, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));
    (void)block;
    assert(block == nullptr);

    MqttInsertDataFormatter formatter(config.data_format);
    FormatResult result = formatter.format(config, col_instances, tag_instances, block);
    (void)result;
    assert(std::holds_alternative<std::string>(result));
    assert(std::get<std::string>(result).empty());

    std::cout << "test_mqtt_format_empty_batch passed!" << std::endl;
}

void test_mqtt_format_insert_data_with_empty_rows() {
    auto config = create_base_config();

    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
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

    MemoryPool pool(1, 3, 2, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));


    auto formatter = FormatterFactory::instance().create_formatter<InsertDataConfig>(config.data_format);
    FormatResult result = formatter->format(config, col_instances, tag_instances, block);

    assert(std::holds_alternative<MqttInsertData>(result));
    const auto& msg_data = std::get<MqttInsertData>(result);

    // Verify the timing information
    (void)msg_data;
    assert(msg_data.start_time == 1500000000000);
    assert(msg_data.end_time == 1500000000002);

    // Verify total rows
    assert(msg_data.total_rows == 3);
    assert(msg_data.data.size() == 3);

    std::cout << "test_mqtt_format_insert_data_with_empty_rows passed!" << std::endl;
}

void test_mqtt_format_with_compression() {
    auto config = create_base_config();
    config.data_format.mqtt.records_per_message = 1;
    config.data_format.mqtt.compression = "gzip";
    config.data_format.mqtt.tbname_key = "";

    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"f1", "FLOAT"});

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1500000000000, {3.14f}});
    batch.table_batches.emplace_back("table1", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 1, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));

    MqttInsertDataFormatter formatter(config.data_format);
    FormatResult result = formatter.format(config, col_instances, tag_instances, block);

    assert(std::holds_alternative<MqttInsertData>(result));
    const auto& mqtt_data = std::get<MqttInsertData>(result);
    const auto& messages = mqtt_data.data;

    assert(messages.size() == 1);

    std::string uncompressed_payload = nlohmann::ordered_json{{"ts", 1500000000000}, {"f1", 3.14f}}.dump();
    std::string decompressed_payload = Compressor::decompress(messages[0].second, CompressionType::GZIP);

    assert(decompressed_payload == uncompressed_payload);
    assert(messages[0].second != uncompressed_payload);

    std::cout << "test_mqtt_format_with_compression passed!" << std::endl;
}

void test_mqtt_format_factory_creation() {
    DataFormat format;
    format.format_type = "mqtt";

    auto formatter = FormatterFactory::instance().create_formatter<InsertDataConfig>(format);
    assert(formatter != nullptr);

    auto* mqtt_formatter = dynamic_cast<MqttInsertDataFormatter*>(formatter.get());
    assert(mqtt_formatter != nullptr);
    (void)mqtt_formatter;

    std::cout << "test_mqtt_format_factory_creation passed!" << std::endl;
}

int main() {
    test_mqtt_format_single_record_per_message();
    test_mqtt_format_multiple_records_per_message();
    test_mqtt_format_empty_batch();
    test_mqtt_format_insert_data_with_empty_rows();
    test_mqtt_format_with_compression();
    test_mqtt_format_factory_creation();
    std::cout << "All tests passed!" << std::endl;
    return 0;
}