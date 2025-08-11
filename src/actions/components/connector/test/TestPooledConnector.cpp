#include "PooledConnector.hpp"
#include "DummyConnector.hpp"
#include <cassert>
#include <iostream>
#include <memory>
#include <stdexcept>


void test_pooled_connector_constructor_valid() {
    DataChannel channel;
    ConnectionInfo info;
    ConnectionPoolImpl pool(channel, info);
    auto real_conn = std::make_unique<DummyConnector>();
    PooledConnector pooled(std::move(real_conn), pool);
    // No exception, object constructed
    assert(true);
    std::cout << "test_pooled_connector_constructor_valid passed\n";
}

void test_pooled_connector_constructor_nullptr() {
    DataChannel channel;
    ConnectionInfo info;
    ConnectionPoolImpl pool(channel, info);
    bool exception_thrown = false;
    try {
        PooledConnector pooled(nullptr, pool);
    } catch (const std::invalid_argument&) {
        exception_thrown = true;
    }
    assert(exception_thrown);
    std::cout << "test_pooled_connector_constructor_nullptr passed\n";
}

int main() {
    test_pooled_connector_constructor_valid();
    test_pooled_connector_constructor_nullptr();
    std::cout << "All PooledConnector constructor tests passed!" << std::endl;
    return 0;
}