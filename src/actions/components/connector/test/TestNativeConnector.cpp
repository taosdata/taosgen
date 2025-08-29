#include "ConnectorFactory.hpp"
#include "NativeConnector.hpp"
#include <iostream>

void test_native_connector_create_database() {
    // Set connection info
    ConnectionInfo conn_info;
    conn_info.host = "localhost";
    conn_info.port = 6030;
    conn_info.user = "root";
    conn_info.password = "taosdata";

    // Set data channel
    DataChannel channel;
    channel.channel_type = "native";

    // Create connector instance using ConnectorFactory::create
    auto connector = ConnectorFactory::create(channel, conn_info);

    // Test connection
    if (!connector->connect()) {
        std::cerr << "Failed to connect to TDengine." << std::endl;
        return;
    }
    std::cout << "Connected to TDengine successfully." << std::endl;

    // Test SQL execution
    std::string sql = "DROP DATABASE IF EXISTS `test_connector`";
    if (!connector->execute(sql)) {
        std::cerr << "Failed to execute SQL: " << sql << std::endl;
        connector->close();
        return;
    }
    std::cout << "SQL executed successfully: " << sql << std::endl;

    sql = "CREATE DATABASE IF NOT EXISTS `test_connector`";
    if (!connector->execute(sql)) {
        std::cerr << "Failed to execute SQL: " << sql << std::endl;
        connector->close();
        return;
    }
    std::cout << "SQL executed successfully: " << sql << std::endl;

    // Close connection
    connector->close();
    std::cout << "Connection closed successfully." << std::endl;
}

int main() {
    std::cout << "Running NativeConnector tests..." << std::endl;

    // Call test function
    test_native_connector_create_database();

    std::cout << "NativeConnector tests completed." << std::endl;
    return 0;
}