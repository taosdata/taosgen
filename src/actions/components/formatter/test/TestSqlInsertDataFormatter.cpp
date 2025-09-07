#include <iostream>
#include <cassert>
#include "FormatterRegistrar.hpp"
#include "SqlInsertDataFormatter.hpp"

void test_format_insert_data_single_table() {
    DataFormat format;
    format.format_type = "sql";

    InsertDataConfig config;
    config.tdengine.database = "test_db";

    ColumnConfigInstanceVector col_instances;
    col_instances.emplace_back(ColumnConfig{"f1", "FLOAT"});
    col_instances.emplace_back(ColumnConfig{"i1", "INT"});
    col_instances.emplace_back(ColumnConfig{"s1", "VARCHAR(20)"});

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1500000000000, {3.14f, 42, std::string("value1")}});
    rows.push_back({1500000000001, {2.71f, 43, std::string("value2")}});
    batch.table_batches.push_back({"table1", rows});
    batch.update_metadata();

    MemoryPool pool(1, 1, 2, col_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));
    if (block) {
        assert(block->used_tables == 1);
        auto& table_block = block->tables[0];

        // Verify timestamps
        assert(table_block.timestamps[0] == 1500000000000);
        assert(table_block.timestamps[1] == 1500000000001);

        // Verify float column
        float* f1_data = static_cast<float*>(table_block.columns[0].fixed_data);
        (void)f1_data;
        assert(f1_data[0] == 3.14f);
        assert(f1_data[1] == 2.71f);

        // Verify integer column
        int32_t* i1_data = static_cast<int32_t*>(table_block.columns[1].fixed_data);
        (void)i1_data;
        assert(i1_data[0] == 42);
        assert(i1_data[1] == 43);

        // Verify string column
        auto& s1_col = table_block.columns[2];
        assert(s1_col.lengths[0] == 6);
        assert(s1_col.lengths[1] == 6);
        assert(s1_col.var_offsets[0] == 0);
        assert(s1_col.var_offsets[1] == 6);
        assert(s1_col.current_offset == 12);

        std::string value1(s1_col.var_data, 6);
        std::string value2(s1_col.var_data + 6, 6);
        assert(value1 == "value1");
        assert(value2 == "value2");
    }

    SqlInsertDataFormatter formatter(format);
    FormatResult result = formatter.format(config, col_instances, block);

    assert(std::holds_alternative<SqlInsertData>(result));
    assert(std::get<SqlInsertData>(result).data.str() ==
           "INSERT INTO `test_db`.`table1` VALUES "
           "(1500000000000,3.14,42,'value1')"
           "(1500000000001,2.71,43,'value2');");
    std::cout << "test_format_insert_data_single_table passed!" << std::endl;
}

void test_format_insert_data_multiple_tables() {
    DataFormat format;
    format.format_type = "sql";

    InsertDataConfig config;
    config.tdengine.database = "test_db";

    ColumnConfigInstanceVector col_instances;
    col_instances.emplace_back(ColumnConfig{"f1", "FLOAT"});
    col_instances.emplace_back(ColumnConfig{"i1", "INT"});

    MultiBatch batch;
    // First table
    std::vector<RowData> rows1;
    rows1.push_back({1500000000000, {3.14f, 42}});
    rows1.push_back({1500000000001, {2.71f, 43}});
    batch.table_batches.push_back({"table1", rows1});

    // Second table
    std::vector<RowData> rows2;
    rows2.push_back({1500000000002, {1.23f, 44}});
    rows2.push_back({1500000000003, {4.56f, 45}});
    batch.table_batches.push_back({"table2", rows2});

    batch.update_metadata();

    MemoryPool pool(1, 2, 2, col_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));
    (void)block;

    auto formatter = FormatterFactory::instance().create_formatter<InsertDataConfig>(format);
    FormatResult result = formatter->format(config, col_instances, block);

    assert(std::holds_alternative<SqlInsertData>(result));
    assert(std::get<SqlInsertData>(result).data.str() ==
           "INSERT INTO `test_db`.`table1` VALUES "
           "(1500000000000,3.14,42)"
           "(1500000000001,2.71,43) "
           "`test_db`.`table2` VALUES "
           "(1500000000002,1.23,44)"
           "(1500000000003,4.56,45);");
    std::cout << "test_format_insert_data_multiple_tables passed!" << std::endl;
}

void test_format_insert_data_empty_rows() {
    DataFormat format;
    format.format_type = "sql";

    InsertDataConfig config;
    config.tdengine.database = "test_db";

    ColumnConfigInstanceVector col_instances;
    col_instances.emplace_back(ColumnConfig{"f1", "FLOAT"});

    MultiBatch batch;
    batch.table_batches.push_back({"table1", {}});
    batch.total_rows = 0;

    MemoryPool pool(1, 1, 1, col_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));

    SqlInsertDataFormatter formatter(format);
    FormatResult result = formatter.format(config, col_instances, block);

    assert(std::holds_alternative<std::string>(result));
    assert(std::get<std::string>(result) == "");
    std::cout << "test_format_insert_data_empty_rows passed!" << std::endl;
}

void test_format_insert_data_different_types() {
    DataFormat format;
    format.format_type = "sql";

    InsertDataConfig config;
    config.tdengine.database = "test_db";

    ColumnConfigInstanceVector col_instances;
    col_instances.emplace_back(ColumnConfig{"f1", "FLOAT"});
    col_instances.emplace_back(ColumnConfig{"b1", "BOOL"});
    col_instances.emplace_back(ColumnConfig{"s1", "NCHAR(20)"});
    col_instances.emplace_back(ColumnConfig{"j1", "JSON"});

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1500000000000, {3.14f, true, std::u16string(u"测试"), JsonValue{ std::string("{\"key\":\"value\"}") }}});
    batch.table_batches.push_back({"table1", rows});
    batch.update_metadata();

    MemoryPool pool(1, 1, 1, col_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));

    SqlInsertDataFormatter formatter(format);
    FormatResult result = formatter.format(config, col_instances, block);

    assert(std::holds_alternative<SqlInsertData>(result));
    assert(std::get<SqlInsertData>(result).data.str() ==
           "INSERT INTO `test_db`.`table1` VALUES "
           "(1500000000000,3.14,true,'测试','{\"key\":\"value\"}');");
    std::cout << "test_format_insert_data_different_types passed!" << std::endl;
}

int main() {
    test_format_insert_data_single_table();
    test_format_insert_data_multiple_tables();
    test_format_insert_data_empty_rows();
    test_format_insert_data_different_types();
    std::cout << "All tests passed!" << std::endl;
    return 0;
}