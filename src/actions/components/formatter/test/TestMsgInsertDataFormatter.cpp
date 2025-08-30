#include <iostream>
#include <cassert>
#include "FormatterRegistrar.hpp"
#include "MsgInsertDataFormatter.hpp"

void test_msg_format_insert_data_single_table() {
    DataFormat format;
    format.format_type = "msg";

    InsertDataConfig config;
    config.target.mqtt.batch_messages = 2;
    config.target.mqtt.topic = "test/topic";
    config.target.mqtt.compression = "";
    config.target.mqtt.encoding = "";

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

    MsgInsertDataFormatter formatter(format);
    FormatResult result = formatter.format(config, col_instances, block);

    assert(std::holds_alternative<MsgInsertData>(result));
    const auto& msg_data = std::get<MsgInsertData>(result);
    (void)msg_data;
    assert(msg_data.start_time == 1500000000000);
    assert(msg_data.end_time == 1500000000001);
    assert(msg_data.total_rows == 2);
    assert(msg_data.data.size() == 1); // batch_messages=2, 2 rows, 1 batch
    std::cout << "test_msg_format_insert_data_single_table passed!" << std::endl;
}

void test_msg_format_insert_data_multiple_tables() {
    DataFormat format;
    format.format_type = "msg";

    InsertDataConfig config;
    config.target.mqtt.batch_messages = 2;
    config.target.mqtt.topic = "test/topic";
    config.target.mqtt.compression = "";
    config.target.mqtt.encoding = "";

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

    auto formatter = FormatterFactory::instance().create_formatter<InsertDataConfig>(format);
    FormatResult result = formatter->format(config, col_instances, block);

    assert(std::holds_alternative<MsgInsertData>(result));
    const auto& msg_data = std::get<MsgInsertData>(result);
    (void)msg_data;
    assert(msg_data.start_time == 1500000000000);
    assert(msg_data.end_time == 1500000000003);
    assert(msg_data.total_rows == 4);
    assert(msg_data.data.size() == 2); // 4 rows, batch_messages=2, 2 batches
    std::cout << "test_msg_format_insert_data_multiple_tables passed!" << std::endl;
}

void test_msg_format_insert_data_empty_batch() {
    DataFormat format;
    format.format_type = "msg";

    InsertDataConfig config;
    config.target.mqtt.batch_messages = 2;
    config.target.mqtt.topic = "test/topic";
    config.target.mqtt.compression = "";
    config.target.mqtt.encoding = "";

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

    MsgInsertDataFormatter formatter(format);
    FormatResult result = formatter.format(config, col_instances, block);

    assert(std::holds_alternative<MsgInsertData>(result));
    const auto& msg_data = std::get<MsgInsertData>(result);
    (void)msg_data;
    assert(msg_data.total_rows == 1);
    std::cout << "test_msg_format_insert_data_empty_batch passed!" << std::endl;
}

void test_msg_format_insert_data_with_empty_rows() {
    DataFormat format;
    format.format_type = "msg";

    InsertDataConfig config;
    config.target.mqtt.batch_messages = 2;
    config.target.mqtt.topic = "test/topic";
    config.target.mqtt.compression = "";
    config.target.mqtt.encoding = "";

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

    auto formatter = FormatterFactory::instance().create_formatter<InsertDataConfig>(format);
    FormatResult result = formatter->format(config, col_instances, block);

    assert(std::holds_alternative<MsgInsertData>(result));
    const auto& msg_data = std::get<MsgInsertData>(result);

    // Verify the timing information excludes empty table
    (void)msg_data;
    assert(msg_data.start_time == 1500000000000);
    assert(msg_data.end_time == 1500000000002);

    // Verify total rows only counts non-empty tables
    assert(msg_data.total_rows == 3);  // 2 rows from table1 + 1 row from table3

    std::cout << "test_msg_format_insert_data_with_empty_rows passed!" << std::endl;
}

int main() {
    test_msg_format_insert_data_single_table();
    test_msg_format_insert_data_multiple_tables();
    test_msg_format_insert_data_empty_batch();
    test_msg_format_insert_data_with_empty_rows();
    std::cout << "All msg formatter tests passed!" << std::endl;
    return 0;
}