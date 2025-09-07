#include <iostream>
#include <cassert>
#include <fstream>
#include "ColumnsCSVReader.hpp"


void test_validate_config_empty_file_path() {
    ColumnsCSV config;
    config.file_path = "";
    config.has_header = true;

    try {
        ColumnsCSVReader columns_csv(config, std::nullopt);
        assert(false && "Expected exception for empty file path");
    } catch (const std::invalid_argument& e) {
        std::cout << "test_validate_config_empty_file_path passed\n";
    }
}

void test_validate_config_mismatched_column_types() {
    ColumnsCSV config;
    config.file_path = "test.csv";
    config.has_header = true;

    // Explicitly set timestamp strategy to generator mode
    config.timestamp_strategy.generator = TimestampGeneratorConfig{};

    std::ofstream test_file("test.csv");
    test_file << "name,age,city\n";
    test_file << "Alice,30,New York\n";
    test_file.close();

    ColumnConfigVector col_configs = {
        {"name", "varchar(20)"},
        {"age", "int"}            // Mismatched size
    };
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    try {
        ColumnsCSVReader columns_csv(config, instances);
        assert(false && "Expected exception for mismatched column types size");
    } catch (const std::invalid_argument& e) {
        std::cout << "test_validate_config_mismatched_column_types passed\n";
    }
}

void test_generate_table_data_with_default_timestamp() {
    ColumnsCSV config;
    config.enabled = true;
    config.timestamp_strategy.strategy_type = "csv";
    config.timestamp_strategy.csv.enabled = true;
    config.timestamp_strategy.csv.timestamp_index = 0;
    config.file_path = "timestamp.csv";
    config.has_header = true;

    // Default timestamp strategy (TimestampCSVConfig)
    std::ofstream test_file("timestamp.csv");
    test_file << "timestamp,name,city\n"; // First column is the timestamp in milliseconds
    test_file << "1622505600000,Alice,New York\n";
    test_file << "1622592000000,Bob,Los Angeles\n";
    test_file.close();

    ColumnConfigVector col_configs = {
        {"name", "varchar(20)"},
        {"city", "varchar(20)"}
    };
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    ColumnsCSVReader columns_csv(config, instances);
    auto table_data = columns_csv.generate();

    assert(table_data.size() == 1 && "Expected 1 table");
    assert(table_data[0].timestamps.size() == 2 && "Expected 2 timestamps");
    assert(table_data[0].timestamps[0] == 1622505600000 && "Expected first timestamp to match");
    assert(table_data[0].timestamps[1] == 1622592000000 && "Expected second timestamp to match");
    assert(table_data[0].rows.size() == 2 && "Expected 2 rows of data");
    assert(std::get<std::string>(table_data[0].rows[0][0]) == "Alice" && "Expected first column to be 'Alice'");
    assert(std::get<std::string>(table_data[0].rows[0][1]) == "New York" && "Expected second column to be 'New York'");
    std::cout << "test_generate_table_data_with_default_timestamp passed\n";
}

void test_generate_table_data_with_timestamp() {
    ColumnsCSV config;
    config.enabled = true;
    config.timestamp_strategy.strategy_type = "csv";
    config.timestamp_strategy.csv.enabled = true;
    config.timestamp_strategy.csv.timestamp_index = 1;
    config.file_path = "timestamp.csv";
    config.has_header = true;

    std::ofstream test_file("timestamp.csv");
    test_file << "name,timestamp,city\n";
    test_file << "Alice,1622505600000,New York\n";
    test_file << "Bob,1622592000000,Los Angeles\n";
    test_file.close();

    ColumnConfigVector col_configs = {
        {"name", "varchar(20)"},
        {"city", "varchar(20)"}
    };
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    ColumnsCSVReader columns_csv(config, instances);
    auto table_data = columns_csv.generate();

    assert(table_data.size() == 1 && "Expected 1 table");
    assert(table_data[0].timestamps.size() == 2 && "Expected 2 timestamps");
    assert(table_data[0].timestamps[0] == 1622505600000 && "Expected first timestamp to match");
    assert(table_data[0].timestamps[1] == 1622592000000 && "Expected second timestamp to match");
    assert(table_data[0].rows.size() == 2 && "Expected 2 rows of data");
    assert(std::get<std::string>(table_data[0].rows[0][0]) == "Alice" && "Expected first column to be 'Alice'");
    assert(std::get<std::string>(table_data[0].rows[0][1]) == "New York" && "Expected second column to be 'New York'");
    std::cout << "test_generate_table_data_with_timestamp passed\n";
}

void test_generate_table_data_with_generated_timestamp() {
    ColumnsCSV config;
    config.file_path = "generated_timestamp.csv";
    config.has_header = true;

    // Explicitly set timestamp strategy to generator mode
    config.timestamp_strategy.generator = TimestampGeneratorConfig{};

    std::ofstream test_file("generated_timestamp.csv");
    test_file << "name,age,city\n";
    test_file << "Alice,30,New York\n";
    test_file << "Bob,25,Los Angeles\n";
    test_file.close();

    ColumnConfigVector col_configs = {
        {"name", "varchar(20)"},
        {"age", "int"},
        {"city", "varchar(20)"}
    };
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    ColumnsCSVReader columns_csv(config, instances);
    auto table_data = columns_csv.generate();

    assert(table_data.size() == 1 && "Expected 1 table");
    assert(table_data[0].timestamps.size() == 2 && "Expected 2 timestamps");
    assert(table_data[0].rows.size() == 2 && "Expected 2 rows of data");
    assert(std::get<std::string>(table_data[0].rows[0][0]) == "Alice" && "Expected first column to be 'Alice'");
    assert(std::get<int32_t>(table_data[0].rows[0][1]) == 30 && "Expected second column to be 30");
    assert(std::get<std::string>(table_data[0].rows[0][2]) == "New York" && "Expected third column to be 'New York'");
    std::cout << "test_generate_table_data_with_generated_timestamp passed\n";
}


void test_generate_table_data_include_tbname() {
    ColumnsCSV config;
    config.file_path = "include_tbname.csv";
    config.has_header = true;
    config.tbname_index = 0; // table name column
    config.timestamp_strategy.generator = TimestampGeneratorConfig{};

    std::ofstream test_file("include_tbname.csv");
    test_file << "table_name,age,city\n";
    test_file << "table1,30,New York\n";
    test_file << "table2,25,Los Angeles\n";
    test_file.close();

    ColumnConfigVector col_configs = {
        {"age", "int"},
        {"city", "varchar(20)"}
    };
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    ColumnsCSVReader columns_csv(config, instances);
    auto table_data = columns_csv.generate();

    // Verify table count
    assert(table_data.size() == 2 && "Expected 2 tables");

    // Check table names and data
    bool table1_found = false;
    bool table2_found = false;

    for (const auto& table : table_data) {
        if (table.table_name == "table1") {
            table1_found = true;
            assert(table.timestamps.size() == 1 && "Expected 1 timestamp for table1");
            assert(table.rows.size() == 1 && "Expected 1 row of data for table1");
            assert(std::get<int32_t>(table.rows[0][0]) == 30 && "Expected first column to be 30 for table1");
            assert(std::get<std::string>(table.rows[0][1]) == "New York" && "Expected second column to be 'New York' for table1");
        } else if (table.table_name == "table2") {
            table2_found = true;
            assert(table.timestamps.size() == 1 && "Expected 1 timestamp for table2");
            assert(table.rows.size() == 1 && "Expected 1 row of data for table2");
            assert(std::get<int32_t>(table.rows[0][0]) == 25 && "Expected first column to be 25 for table2");
            assert(std::get<std::string>(table.rows[0][1]) == "Los Angeles" && "Expected second column to be 'Los Angeles' for table2");
        }
    }

    // Verify both tables are found
    (void)table1_found;
    (void)table2_found;
    assert(table1_found && "Expected table1 to be found");
    assert(table2_found && "Expected table2 to be found");

    std::cout << "test_generate_table_data_include_tbname passed\n";
}

void test_generate_table_data_default_column_types() {
    ColumnsCSV config;
    config.enabled = true;
    config.timestamp_strategy.strategy_type = "csv";
    config.timestamp_strategy.csv.enabled = true;
    config.timestamp_strategy.csv.timestamp_index = 0;
    config.file_path = "default.csv";
    config.has_header = true;

    std::ofstream test_file("default.csv");
    test_file << "timestamp,name,age,city\n";
    test_file << "1622505600000,Alice,30,New York\n";
    test_file << "1622592000000,Bob,25,Los Angeles\n";
    test_file.close();

    ColumnsCSVReader columns_csv(config, std::nullopt); // No column types provided
    auto table_data = columns_csv.generate();

    assert(table_data.size() == 1 && "Expected 1 table");
    assert(table_data[0].rows.size() == 2 && "Expected 2 rows of data");
    assert(std::get<std::string>(table_data[0].rows[0][0]) == "Alice" && "Expected first column to be 'Alice'");
    assert(std::get<std::string>(table_data[0].rows[0][1]) == "30" && "Expected second column to be '30'");
    assert(std::get<std::string>(table_data[0].rows[0][2]) == "New York" && "Expected third column to be 'New York'");
    std::cout << "test_generate_table_data_default_column_types passed\n";
}

int main() {
    test_validate_config_empty_file_path();
    test_validate_config_mismatched_column_types();
    test_generate_table_data_with_default_timestamp();
    test_generate_table_data_with_timestamp();
    test_generate_table_data_with_generated_timestamp();
    test_generate_table_data_include_tbname();
    test_generate_table_data_default_column_types();

    std::cout << "All tests passed!\n";
    return 0;
}