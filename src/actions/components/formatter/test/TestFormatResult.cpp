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
    BaseInsertData data(
        BaseInsertData::DataType::SQL,
        1500000000000,
        1500000001000,
        100
    );

    assert(data.type == BaseInsertData::DataType::SQL);
    assert(data.start_time == 1500000000000);
    assert(data.end_time == 1500000001000);
    assert(data.total_rows == 100);

    // Test STMT_V2 type
    BaseInsertData stmt_data(
        BaseInsertData::DataType::STMT_V2,
        1500000002000,
        1500000003000,
        200
    );

    assert(stmt_data.type == BaseInsertData::DataType::STMT_V2);
    assert(stmt_data.start_time == 1500000002000);
    assert(stmt_data.end_time == 1500000003000);
    assert(stmt_data.total_rows == 200);

    std::cout << "test_base_insert_data passed!" << std::endl;
}

void test_sql_insert_data() {
    SqlInsertData data(
        1500000000000,
        1500000001000,
        100,
        "INSERT INTO test_table VALUES(1,2,3);"
    );

    // Test base class members
    assert(data.start_time == 1500000000000);
    assert(data.end_time == 1500000001000);
    assert(data.total_rows == 100);

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
    FormatResult result2 = SqlInsertData{
        1500000000000, 1500000001000, 100, "INSERT DATA"
    };
    assert(std::holds_alternative<SqlInsertData>(result2));
    const auto& sql_data = std::get<SqlInsertData>(result2);
    (void)sql_data;
    assert(sql_data.start_time == 1500000000000);
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
    StmtV2InsertData data{block, col_instances};

    // Test base class members
    assert(data.start_time == 1500000000000);
    assert(data.end_time == 1500000000000);
    assert(data.total_rows == 1);

    // Test StmtV2Data members
    assert(data.data.row_count() == 1);
    assert(data.data.column_count() == 1);

    std::cout << "test_stmt_v2_insert_data passed!" << std::endl;
}

int main() {
    test_sql_data();
    test_base_insert_data();
    test_sql_insert_data();
    test_format_result_variant();
    test_stmt_v2_insert_data();

    std::cout << "All tests passed!" << std::endl;
    return 0;
}