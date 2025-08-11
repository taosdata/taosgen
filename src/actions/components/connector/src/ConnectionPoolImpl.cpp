#include "ConnectionPoolImpl.hpp"
#include "ConnectorFactory.hpp"
#include "PooledConnector.hpp"
#include <iostream>
#include <stdexcept>

ConnectionPoolImpl::ConnectionPoolImpl(
    const DataChannel& channel, const ConnectionInfo& conn_info)
    : channel_(channel), conn_info_(conn_info)
{
    initialize();
}

ConnectionPoolImpl::~ConnectionPoolImpl() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        is_shutting_down_ = true;
    }

    condition_.notify_all();
    close_all_connections();
}

void ConnectionPoolImpl::initialize() {
    create_connections(conn_info_.pool_config.min_pool_size);
}

void ConnectionPoolImpl::create_connections(size_t count) {
    std::lock_guard<std::mutex> lock(mutex_);

    for (size_t i = 0; i < count; ++i) {
        if (total_count_ >= conn_info_.pool_config.max_pool_size) break;

        try {
            std::unique_ptr<DatabaseConnector> conn = ConnectorFactory::create(channel_, conn_info_);

            if (conn->connect()) {
                available_connections_.push(std::move(conn));
                total_count_++;
            } else {
                std::cerr << "Connection creation failed: connect error" << std::endl;
            }
        } catch (const std::exception& e) {
            std::cerr << "Connection creation failed: " << e.what() << std::endl;
        }
    }
}

std::unique_ptr<DatabaseConnector> ConnectionPoolImpl::get_connection() {
    std::unique_lock<std::mutex> lock(mutex_);

    // Expand the pool if needed
    if (available_connections_.empty() && total_count_ < conn_info_.pool_config.max_pool_size) {
        lock.unlock();
        create_connections(1);
        lock.lock();
    }

    // Wait for an available connection
    if (available_connections_.empty()) {
        auto status = condition_.wait_for(
            lock,
            std::chrono::milliseconds(conn_info_.pool_config.connection_timeout),
            [this] { return !available_connections_.empty() ||
                is_shutting_down_;
            }
        );

        if (!status) {
            throw std::runtime_error("Timeout waiting for database connection");
        }
    }

    if (is_shutting_down_) {
        throw std::runtime_error("Connection pool is shutting down");
    }

    // Take out a connection
    auto real_conn = std::move(available_connections_.front());
    available_connections_.pop();

    // Check if the connection is valid
    if (!real_conn->is_valid()) {
        lock.unlock();
        try {
            auto new_conn = ConnectorFactory::create(channel_, conn_info_);
            if (new_conn->connect()) {
                real_conn = std::move(new_conn);
            } else {
                throw std::runtime_error("Failed to reconnect");
            }
        } catch (...) {
            std::lock_guard<std::mutex> lock(mutex_);
            total_count_--;
            throw;
        }
        lock.lock();
    }

    return std::make_unique<PooledConnector>(std::move(real_conn), *this);
}

void ConnectionPoolImpl::return_connection(
    std::unique_ptr<DatabaseConnector> conn)
{
    conn->reset_state();

    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (is_shutting_down_) {
            conn->close();
            return;
        }

        available_connections_.push(std::move(conn));
    }
    condition_.notify_one();
}

void ConnectionPoolImpl::close_all_connections() {
    std::queue<std::unique_ptr<DatabaseConnector>> to_close;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        std::swap(available_connections_, to_close);
        total_count_ = 0;
    }

    while (!to_close.empty()) {
        auto conn = std::move(to_close.front());
        to_close.pop();
        conn->close();
    }
}

size_t ConnectionPoolImpl::total_connections() const {
    return total_count_.load();
}

size_t ConnectionPoolImpl::available_connections() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return available_connections_.size();
}

size_t ConnectionPoolImpl::active_connections() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return total_count_.load() - available_connections_.size();
}