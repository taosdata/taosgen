#include <iostream>
#include <cassert>
#include <fstream>
#include "CreateChildTableAction.hpp"
#include "ActionFactory.hpp"
#include "DatabaseConnector.hpp"
#include "CreateChildTableConfig.hpp"


void test_create_child_table_action_from_generator() {
    GlobalConfig global;

    ConnectionInfo conn_info;
    conn_info.host = "localhost";
    conn_info.port = 6030;
    conn_info.user = "root";
    conn_info.password = "taosdata";

    DataChannel channel;
    channel.channel_type = "native";

    DataFormat format;
    format.format_type = "sql";

    CreateChildTableConfig config;
    config.connection_info = conn_info;
    config.data_format = format;
    config.data_channel = channel;
    config.database_info.name = "test_action_db";
    config.super_table_info.name = "test_super_table";

    // Configure child table name generator
    config.child_table_info.table_name.source_type = "generator";
    config.child_table_info.table_name.generator.prefix = "d";
    config.child_table_info.table_name.generator.count = 10;

    // Configure tag generator
    config.child_table_info.tags.source_type = "generator";
    config.child_table_info.tags.generator.schema = {
        {"tag1", "float", "random", 1.5, 3.5},
        {"tag2", "varchar(20)", "random"}
    };

    // Create action instance
    std::cout << "Creating action instance for child table from generator..." << std::endl;
    auto action = ActionFactory::instance().create_action(
        global,
        "actions/create-child-table",
        config
    );

    // Execute child table creation action
    std::cout << "Executing child table creation..." << std::endl;
    action->execute();


    std::cout << "=== Test completed ===" << std::endl;
}

void test_create_child_table_action_from_csv() {
    GlobalConfig global;
    ConnectionInfo conn_info;
    conn_info.host = "localhost";
    conn_info.port = 6030;
    conn_info.user = "root";
    conn_info.password = "taosdata";

    DataChannel channel;
    channel.channel_type = "native";

    DataFormat format;
    format.format_type = "sql";

    CreateChildTableConfig config;
    config.connection_info = conn_info;
    config.data_format = format;
    config.data_channel = channel;
    config.database_info.name = "test_action_db";
    config.super_table_info.name = "test_super_table";


    std::ofstream test_file("table_names_and_tags.csv");
    test_file << "tag1,tag2,tag3,table\n";
    test_file << "30.125,New York,not,table1\n";
    test_file << "3.1415926,Los Angeles,me,table2\n";
    test_file.close();

    // Configure child table name CSV file path
    config.child_table_info.table_name.source_type = "csv";
    config.child_table_info.table_name.csv.file_path = "table_names_and_tags.csv";
    config.child_table_info.table_name.csv.has_header = true;
    config.child_table_info.table_name.csv.tbname_index = 3; 

    // Configure tag CSV file path
    config.child_table_info.tags.source_type = "csv";
    config.child_table_info.tags.csv.file_path = "table_names_and_tags.csv";
    config.child_table_info.tags.csv.exclude_indices = {2,3};
    config.child_table_info.tags.csv.schema = {
        {"tag1", "float"},
        {"tag2", "varchar(20)"}
    };

    // Create action instance
    std::cout << "Creating action instance for child table from CSV..." << std::endl;
    auto action = ActionFactory::instance().create_action(
        global,
        "actions/create-child-table",
        config
    );

    // Execute child table creation action from CSV
    std::cout << "Executing child table creation from CSV..." << std::endl;
    action->execute();

    std::cout << "=== Test completed ===" << std::endl;
}


int main() {
    std::cout << "Running create-child-table-action tests..." << std::endl;

    try {
        test_create_child_table_action_from_generator();
        test_create_child_table_action_from_csv();
    } catch (const std::exception& e) {
        std::cerr << "Test exception: " << e.what() << std::endl;
        return 1;
    }

    std::cout << "All action tests completed." << std::endl;
    return 0;
}