#include <iostream>
#include <cassert>
#include "CreateDatabaseAction.hpp"
#include "ActionFactory.hpp"
#include "DatabaseConnector.hpp"
#include "CreateDatabaseConfig.hpp"


void test_create_database_action() {
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

    CreateDatabaseConfig config;
    config.connection_info = conn_info;
    config.data_format = format;
    config.data_channel = channel;
    config.database_info.name = "test_action";
    config.database_info.drop_if_exists = true;

    // Create action instance
    std::cout << "Creating action instance..." << std::endl;
    auto action = ActionFactory::instance().create_action(
        global,
        "actions/create-database",
        config
    );

    // Execute database creation action
    std::cout << "Executing database creation..." << std::endl;
    action->execute();

    // // Verify result
    // std::cout << "Verifying database creation..." << std::endl;
    // auto connector = ConnectorFactory::create(channel, conn_info);
    // if (!connector->connect()) {
    //     std::cerr << "Verification failed: cannot connect to database" << std::endl;
    //     return;
    // }

    // // Check if database exists
    // bool db_exists = false;
    // connector->query("SHOW DATABASES", [&](const QueryResult& result) {
    //     for (const auto& row : result.rows) {
    //         if (row[0] == config.database_info.name) {
    //             db_exists = true;
    //             break;
    //         }
    //     }
    // });

    // connector->close();

    // if (db_exists) {
    //     std::cout << "Verification succeeded: database '" << config.database_info.name
    //               << "' created" << std::endl;
    // } else {
    //     std::cerr << "Verification failed: database not created" << std::endl;
    // }

    // Clean up test database
    // std::cout << "Cleaning up test database..." << std::endl;
    // auto cleanup_connector = ConnectorFactory::create(channel, conn_info);
    // if (cleanup_connector->connect()) {
    //     cleanup_connector->execute("DROP DATABASE IF EXISTS `" + config.database_info.name + "`");
    //     cleanup_connector->close();
    // }

    std::cout << "=== Test completed ===" << std::endl;
}

int main() {
    std::cout << "Running create-database-action tests..." << std::endl;

    try {
        test_create_database_action();
    } catch (const std::exception& e) {
        std::cerr << "Test exception: " << e.what() << std::endl;
        return 1;
    }

    std::cout << "All action tests completed." << std::endl;
    return 0;
}