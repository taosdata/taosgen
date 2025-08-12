#include "ConnectionPoolImpl.hpp"
#include <cassert>
#include <iostream>
#include <chrono>

void test_pool_initialization() {
    DataChannel channel;
    ConnectionInfo info;
    info.pool_config.enabled = true;
    info.pool_config.min_pool_size = 2;
    info.pool_config.max_pool_size = 4;
    info.pool_config.connection_timeout = 100;

    ConnectionPoolImpl pool(channel, info);

    assert(pool.total_connections() == info.pool_config.min_pool_size);
    assert(pool.available_connections() == info.pool_config.min_pool_size);
    assert(pool.active_connections() == 0);

    std::cout << "test_pool_initialization passed\n";
}

void test_get_and_return_connection() {
    DataChannel channel;
    ConnectionInfo info;
    info.pool_config.enabled = true;
    info.pool_config.min_pool_size = 2;
    info.pool_config.max_pool_size = 4;
    info.pool_config.connection_timeout = 100;

    ConnectionPoolImpl pool(channel, info);

    auto conn = pool.get_connector();
    assert(conn != nullptr);
    assert(pool.available_connections() == info.pool_config.min_pool_size - 1);
    assert(pool.active_connections() == 1);

    pool.return_connection(std::move(conn));
    assert(pool.available_connections() == info.pool_config.min_pool_size);
    assert(pool.active_connections() == 0);

    std::cout << "test_get_and_return_connection passed\n";
}

void test_pool_max_size() {
    DataChannel channel;
    ConnectionInfo info;
    info.pool_config.enabled = true;
    info.pool_config.max_pool_size = 2;
    info.pool_config.min_pool_size = 1;
    info.pool_config.connection_timeout = 100;

    ConnectionPoolImpl pool(channel, info);

    auto c1 = pool.get_connector();
    auto c2 = pool.get_connector();
    assert(pool.total_connections() == 2);
    assert(pool.available_connections() == 0);
    assert(pool.active_connections() == 2);

    pool.return_connection(std::move(c1));
    pool.return_connection(std::move(c2));
    assert(pool.available_connections() == 2);

    std::cout << "test_pool_max_size passed\n";
}

void test_connection_timeout() {
    DataChannel channel;
    ConnectionInfo info;
    info.pool_config.enabled = true;
    info.pool_config.min_pool_size = 1;
    info.pool_config.max_pool_size = 1;
    info.pool_config.connection_timeout = 100; // 100ms

    ConnectionPoolImpl pool(channel, info);

    // Get the only available connection
    auto conn = pool.get_connector();
    assert(conn != nullptr);

    // Now the pool has no available connections, getting another should timeout and throw
    bool timeout_thrown = false;
    auto start = std::chrono::steady_clock::now();
    try {
        pool.get_connector();
    } catch (const std::runtime_error& e) {
        timeout_thrown = std::string(e.what()).find("Timeout") != std::string::npos;
    }
    auto end = std::chrono::steady_clock::now();
    assert(timeout_thrown);

    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    int expected = info.pool_config.connection_timeout;
    int tolerance = 30;
    assert(std::abs(elapsed_ms - expected) < tolerance);

    // After returning the connection, it can be acquired again
    pool.return_connection(std::move(conn));
    auto conn2 = pool.get_connector();
    assert(conn2 != nullptr);

    std::cout << "test_connection_timeout passed\n";
}

int main() {
    test_pool_initialization();
    test_get_and_return_connection();
    test_pool_max_size();
    test_connection_timeout();

    std::cout << "All ConnectionPoolImpl tests passed!" << std::endl;
    return 0;
}