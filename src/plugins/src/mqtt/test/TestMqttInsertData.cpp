#include "FormatResult.hpp"
#include "MqttInsertData.hpp"
#include <cassert>
#include <cstring>
#include <iostream>

void test_mqtt_insert_data_basic() {
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

    // Prepare message batches
    MqttMessageBatch msg_batch;
    msg_batch.emplace_back("topic1", "payload1");
    msg_batch.emplace_back("topic2", "payload2");

    MqttInsertData msg_data(block, col_instances, tag_instances, std::move(msg_batch));
    assert(msg_data.type == typeid(MqttInsertData));
    assert(msg_data.start_time == 1500000000000);
    assert(msg_data.end_time == 1500000000001);
    assert(msg_data.total_rows == 2);
    assert(msg_data.data.size() == 2);
    assert(msg_data.data[0].first == "topic1");
    assert(msg_data.data[0].second == "payload1");
    assert(msg_data.data[1].first == "topic2");
    assert(msg_data.data[1].second == "payload2");

    std::cout << "test_mqtt_insert_data_basic passed!" << std::endl;
}

void test_mqtt_insert_data_move_ctor() {
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

    MqttMessageBatch msg_batch;
    msg_batch.emplace_back("topic", "payload");

    MqttInsertData original(block, col_instances, tag_instances, std::move(msg_batch));
    assert(original.type == typeid(MqttInsertData));

    MqttInsertData moved(std::move(original));
    assert(moved.type == typeid(MqttInsertData));
    assert(moved.data.size() == 1);
    assert(moved.data[0].first == "topic");
    assert(moved.data[0].second == "payload");

    std::cout << "test_mqtt_insert_data_move_ctor passed!" << std::endl;
}

void test_format_result_with_mqtt_insert_data() {
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

    MqttMessageBatch msg_batch;
    msg_batch.emplace_back("topic", "payload");

    auto payload = std::make_unique<MqttInsertData>(block, col_instances, tag_instances, std::move(msg_batch));
    FormatResult result = std::move(payload);

    assert(std::holds_alternative<InsertFormatResult>(result));
    const auto& ptr = std::get<InsertFormatResult>(result);

    if (auto* mqtt_ptr = dynamic_cast<MqttInsertData*>(ptr.get())) {
        const auto& mqtt_data = *mqtt_ptr;
        (void)mqtt_data;

        assert(mqtt_data.data.size() == 1);
        assert(mqtt_data.data[0].first == "topic");
        assert(mqtt_data.data[0].second == "payload");
    } else {
        throw std::runtime_error("Unexpected derived type in BaseInsertData");
    }


    std::cout << "test_format_result_with_mqtt_insert_data passed!" << std::endl;
}

int main() {
    test_mqtt_insert_data_basic();
    test_mqtt_insert_data_move_ctor();
    test_format_result_with_mqtt_insert_data();

    std::cout << "All tests passed!" << std::endl;
    return 0;
}