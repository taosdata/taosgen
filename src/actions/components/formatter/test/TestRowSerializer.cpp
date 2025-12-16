#include "RowSerializer.hpp"
#include "ColumnConfig.hpp"
#include "MemoryPool.hpp"
#include <cassert>
#include <iostream>
#include <vector>
#include <stdexcept>

void test_basic_serialization() {
    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"temp", "FLOAT"});
    col_instances.emplace_back(ColumnConfig{"humidity", "INT"});
    col_instances.emplace_back(ColumnConfig{"location", "VARCHAR(20)"});

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1609459200000, {25.5f, 60, std::string("factory-1")}});
    batch.table_batches.emplace_back("weather", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 1, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));
    const auto& tb = block->tables[0];

    // --- Test original to_json ---
    nlohmann::ordered_json result = RowSerializer::to_json(col_instances, tag_instances, tb, 0, "tb_name");
    assert(result["tb_name"] == "weather");
    assert(result["ts"] == 1609459200000);
    assert(result["temp"] == 25.5f);
    assert(result["humidity"] == 60);
    assert(result["location"] == "factory-1");
    (void)result;

    // --- Test inplace version ---
    nlohmann::ordered_json result_inplace;
    RowSerializer::to_json_inplace(col_instances, tag_instances, tb, 0, "tb_name", result_inplace);
    assert(result_inplace["tb_name"] == "weather");
    assert(result_inplace["ts"] == 1609459200000);
    assert(result_inplace["temp"] == 25.5f);
    assert(result_inplace["humidity"] == 60);
    assert(result_inplace["location"] == "factory-1");
    (void)result_inplace;

    std::cout << "test_basic_serialization passed." << std::endl;
}

void test_without_tbname_key() {
    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"value", "DOUBLE"});

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1609459201000, {123.456}});
    batch.table_batches.emplace_back("t1", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 1, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));
    const auto& tb = block->tables[0];

    // --- Test original to_json ---
    nlohmann::ordered_json result = RowSerializer::to_json(col_instances, tag_instances, tb, 0, "");
    assert(result.find("tb_name") == result.end());
    assert(result["ts"] == 1609459201000);
    assert(result["value"] == 123.456);
    (void)result;

    // --- Test inplace version ---
    nlohmann::ordered_json result_inplace;
    RowSerializer::to_json_inplace(col_instances, tag_instances, tb, 0, "", result_inplace);
    assert(result_inplace.find("tb_name") == result_inplace.end());
    assert(result_inplace["ts"] == 1609459201000);
    assert(result_inplace["value"] == 123.456);
    (void)result_inplace;

    std::cout << "test_without_tbname_key passed." << std::endl;
}

void test_boolean_type() {
    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"status", "BOOL"});

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1, {true}});
    rows.push_back({2, {false}});
    batch.table_batches.emplace_back("t1", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 2, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));
    const auto& tb = block->tables[0];

    // --- Test original to_json ---
    nlohmann::ordered_json result1 = RowSerializer::to_json(col_instances, tag_instances, tb, 0, "");
    nlohmann::ordered_json result2 = RowSerializer::to_json(col_instances, tag_instances, tb, 1, "");
    assert(result1["status"] == true);
    assert(result2["status"] == false);
    (void)result1;
    (void)result2;

    // --- Test inplace version ---
    nlohmann::ordered_json result_inplace;
    RowSerializer::to_json_inplace(col_instances, tag_instances, tb, 0, "", result_inplace);
    assert(result_inplace["status"] == true);
    result_inplace.clear(); // Clear for reuse
    RowSerializer::to_json_inplace(col_instances, tag_instances, tb, 1, "", result_inplace);
    assert(result_inplace["status"] == false);
    (void)result_inplace;

    std::cout << "test_boolean_type passed." << std::endl;
}

void test_out_of_range_exception() {
    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"c1", "INT"});

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1, {100}});
    batch.table_batches.emplace_back("t1", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 1, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));
    const auto& tb = block->tables[0];

    bool exception_thrown = false;
    try {
        // Test original to_json
        RowSerializer::to_json(col_instances, tag_instances, tb, 1, "");
        assert(false && "Should have thrown an exception for to_json");
    } catch (const std::out_of_range& e) {
        std::string msg = e.what();
        assert(msg.find("is out of range") != std::string::npos);
        exception_thrown = true;
    }
    assert(exception_thrown);

    // --- Test inplace version ---
    exception_thrown = false;
    try {
        nlohmann::ordered_json result_inplace;
        RowSerializer::to_json_inplace(col_instances, tag_instances, tb, 1, "", result_inplace);
        assert(false && "Should have thrown an exception for to_json_inplace");
    } catch (const std::out_of_range& e) {
        std::string msg = e.what();
        assert(msg.find("is out of range") != std::string::npos);
        exception_thrown = true;
    }
    assert(exception_thrown);
    (void)exception_thrown;

    std::cout << "test_out_of_range_exception passed." << std::endl;
}

int main() {
    test_basic_serialization();
    test_without_tbname_key();
    test_boolean_type();
    test_out_of_range_exception();
    std::cout << "All RowSerializer tests passed." << std::endl;
    return 0;
}