#include "ConnectorSource.hpp"
#include "DummyConnector.hpp"
#include <cassert>
#include <iostream>

void test_connector_source_with_pool() {
    DataChannel channel;
    ConnectionInfo info;
    info.pool_config.enabled = true;
    info.pool_config.min_pool_size = 2;
    info.pool_config.max_pool_size = 4;
    info.pool_config.connection_timeout = 100;

    ConnectorSource source(channel, info);

    assert(source.total_connections() == info.pool_config.min_pool_size);
    assert(source.available_connections() == info.pool_config.min_pool_size);
    assert(source.active_connections() == 0);

    auto conn = source.get_connection();
    assert(conn != nullptr);
    assert(source.available_connections() == info.pool_config.min_pool_size - 1);
    assert(source.active_connections() == 1);

    conn->close();

    std::cout << "test_connector_source_with_pool passed\n";
}

void test_connector_source_without_pool() {
    DataChannel channel;
    ConnectionInfo info;
    info.pool_config.enabled = false;

    ConnectorSource source(channel, info);

    assert(source.total_connections() == 0);
    assert(source.available_connections() == 0);
    assert(source.active_connections() == 0);

    auto conn = source.get_connection();
    assert(conn != nullptr);

    std::cout << "test_connector_source_without_pool passed\n";
}

int main() {
    test_connector_source_with_pool();
    test_connector_source_without_pool();

    std::cout << "All ConnectorSource tests passed!" << std::endl;
    return 0;
}