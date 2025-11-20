#include <cassert>
#include <cstring>
#include <iostream>
#include "FormatResult.hpp"

void test_sql_data() {
    // Test construction and move semantics
    std::string test_sql = "SELECT * FROM test_table;";
    const size_t sql_length = test_sql.length();
    SqlData sql_data(std::move(test_sql));

    // Test size
    (void)sql_length;
    assert(sql_data.size() == sql_length);

    // Test string content
    assert(sql_data.str() == "SELECT * FROM test_table;");

    // Test C-string
    assert(strcmp(sql_data.c_str(), "SELECT * FROM test_table;") == 0);

    // Test move constructor
    SqlData moved_data(std::move(sql_data));
    assert(moved_data.size() == sql_length);
    assert(moved_data.str() == "SELECT * FROM test_table;");

    std::cout << "test_sql_data passed!" << std::endl;
}

void test_base_insert_data() {
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

    BaseInsertData data(block, col_instances);
    (void)data;
    assert(data.type == BaseInsertData::DataType::BASE);
    assert(data.start_time == 1500000000000);
    assert(data.end_time == 1500000000001);
    assert(data.total_rows == 2);

    std::cout << "test_base_insert_data passed!" << std::endl;
}

void test_sql_insert_data() {
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

    SqlInsertData data(block, col_instances, "INSERT INTO test_table VALUES(1,2,3);");

    // Test base class members
    assert(data.start_time == 1500000000000);
    assert(data.end_time == 1500000000001);
    assert(data.total_rows == 2);

    // Test SqlData member
    assert(data.data.str() == "INSERT INTO test_table VALUES(1,2,3);");

    std::cout << "test_sql_insert_data passed!" << std::endl;
}

void test_format_result_variant() {
    // Test string variant
    FormatResult result1 = std::string("Test string");
    assert(std::holds_alternative<std::string>(result1));
    assert(std::get<std::string>(result1) == "Test string");

    // Test SqlInsertData variant
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

    FormatResult result2 = SqlInsertData(block, col_instances, "INSERT DATA");
    assert(std::holds_alternative<SqlInsertData>(result2));
    const auto& sql_data = std::get<SqlInsertData>(result2);
    (void)sql_data;
    assert(sql_data.start_time == 1500000000000);
    assert(sql_data.end_time == 1500000000001);
    assert(sql_data.total_rows == 2);
    assert(sql_data.data.str() == "INSERT DATA");

    // Test variant type checking
    assert(!std::holds_alternative<StmtV2InsertData>(result1));
    assert(!std::holds_alternative<std::string>(result2));

    std::cout << "test_format_result_variant passed!" << std::endl;
}

void test_stmt_v2_insert_data() {
    // Create test data
    ColumnConfigInstanceVector col_instances;
    col_instances.emplace_back(ColumnConfig{"f1", "FLOAT"});

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1500000000000, {3.14f}});
    batch.table_batches.emplace_back("test_table", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 1, col_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));

    // Create StmtV2InsertData
    StmtV2InsertData data(block, col_instances);

    // Test base class members
    assert(data.start_time == 1500000000000);
    assert(data.end_time == 1500000000000);
    assert(data.total_rows == 1);

    // Test StmtV2Data members
    assert(data.row_count() == 1);
    assert(data.column_count() == 1);

    std::cout << "test_stmt_v2_insert_data passed!" << std::endl;
}

void test_mqtt_insert_data_basic() {
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

    // Prepare message batches
    MqttMessageBatch msg_batch;
    msg_batch.emplace_back("topic1", "payload1");
    msg_batch.emplace_back("topic2", "payload2");

    MqttInsertData msg_data(block, col_instances, std::move(msg_batch));
    assert(msg_data.type == BaseInsertData::DataType::MQTT);
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
    col_instances.emplace_back(ColumnConfig{"f1", "FLOAT"});

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1500000000000, {3.14f}});
    batch.table_batches.emplace_back("table1", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 1, col_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));

    MqttMessageBatch msg_batch;
    msg_batch.emplace_back("topic", "payload");

    MqttInsertData original(block, col_instances, std::move(msg_batch));
    assert(original.type == BaseInsertData::DataType::MQTT);

    MqttInsertData moved(std::move(original));
    assert(moved.type == BaseInsertData::DataType::MQTT);
    assert(moved.data.size() == 1);
    assert(moved.data[0].first == "topic");
    assert(moved.data[0].second == "payload");

    std::cout << "test_mqtt_insert_data_move_ctor passed!" << std::endl;
}

void test_format_result_with_mqtt_insert_data() {
    ColumnConfigInstanceVector col_instances;
    col_instances.emplace_back(ColumnConfig{"f1", "FLOAT"});

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1500000000000, {3.14f}});
    batch.table_batches.emplace_back("table1", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 1, col_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));

    MqttMessageBatch msg_batch;
    msg_batch.emplace_back("topic", "payload");

    MqttInsertData msg_data(block, col_instances, std::move(msg_batch));
    FormatResult result = std::move(msg_data);

    assert(std::holds_alternative<MqttInsertData>(result));
    const auto& ref = std::get<MqttInsertData>(result);
    (void)ref;
    assert(ref.data.size() == 1);
    assert(ref.data[0].first == "topic");
    assert(ref.data[0].second == "payload");

    std::cout << "test_format_result_with_mqtt_insert_data passed!" << std::endl;
}

int main() {
    test_sql_data();
    test_base_insert_data();
    test_sql_insert_data();
    test_format_result_variant();
    test_stmt_v2_insert_data();
    test_mqtt_insert_data_basic();
    test_mqtt_insert_data_move_ctor();
    test_format_result_with_mqtt_insert_data();

    std::cout << "All tests passed!" << std::endl;
    return 0;
}