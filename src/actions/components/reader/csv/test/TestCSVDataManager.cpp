#include "CSVDataManager.hpp"
#include <iostream>
#include <cassert>
#include <fstream>
#include <thread>
#include <vector>
#include <numeric>


// Helper to create a dummy CSV file for tests
void create_test_file(const std::string& filename, const std::string& content) {
    std::ofstream test_file(filename);
    test_file << content;
    test_file.close();
}

void test_manager_singleton_and_reset() {
    auto& manager1 = CSVDataManager::instance();
    auto& manager2 = CSVDataManager::instance();
    assert(&manager1 == &manager2 && "instance() should return the same object");

    // Create a file and get its cache
    const std::string filename = "reset_test.csv";
    create_test_file(filename, "col1\nval1");
    auto cache1 = manager1.get_cache_for_file(filename);
    assert(cache1 != nullptr);

    // Reset the manager
    manager1.reset();
    manager2.reset();

    // Get the cache again and verify it's a new instance
    auto cache2 = manager1.get_cache_for_file(filename);
    assert(cache2 != nullptr);
    assert(cache1 != cache2 && "reset() should clear caches, leading to new instances");

    (void)cache1;
    (void)cache2;

    std::remove(filename.c_str());
    std::cout << "test_manager_singleton_and_reset passed\n";
}

void test_manager_cache_retrieval() {
    CSVDataManager::instance().reset();
    auto& manager = CSVDataManager::instance();

    const std::string filename1 = "file1.csv";
    const std::string filename2 = "file2.csv";

    // Get cache for the same file twice
    auto cache1_first_call = manager.get_cache_for_file(filename1);
    auto cache1_second_call = manager.get_cache_for_file(filename1);
    assert(cache1_first_call == cache1_second_call && "Should get the same cache instance for the same file path");

    // Get cache for a different file
    auto cache2 = manager.get_cache_for_file(filename2);
    assert(cache1_first_call != cache2 && "Should get different cache instances for different file paths");

    (void)cache1_first_call;
    (void)cache1_second_call;
    (void)cache2;

    std::cout << "test_manager_cache_retrieval passed\n";
}

void test_cache_loads_data_successfully() {
    CSVDataManager::instance().reset();
    const std::string filename = "success.csv";
    create_test_file(filename, "name,value\nAlice,100\nBob,200");

    ColumnsCSV config;
    config.file_path = filename;
    config.has_header = true;
    config.timestamp_strategy.generator = TimestampGeneratorConfig{};

    ColumnConfigVector col_configs = {{"name", "varchar(20)"}, {"value", "int"}};
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    auto cache = CSVDataManager::instance().get_cache_for_file(filename);
    const auto& all_tables = cache->get_data(config, instances);

    assert(all_tables.count("default_table") == 1);
    const auto& data = all_tables.at("default_table");
    assert(data.rows.size() == 2);
    assert(std::get<std::string>(data.rows[0][0]) == "Alice");
    assert(std::get<int32_t>(data.rows[0][1]) == 100);
    assert(std::get<std::string>(data.rows[1][0]) == "Bob");
    assert(std::get<int32_t>(data.rows[1][1]) == 200);
    (void)data;

    std::remove(filename.c_str());
    std::cout << "test_cache_loads_data_successfully passed\n";
}

void test_cache_propagates_error() {
    CSVDataManager::instance().reset();
    const std::string filename = "error.csv";
    create_test_file(filename, "name,value\nAlice,not-an-int");

    ColumnsCSV config;
    config.file_path = filename;
    config.has_header = true;
    config.timestamp_strategy.generator = TimestampGeneratorConfig{};

    ColumnConfigVector col_configs = {{"name", "varchar(20)"}, {"value", "int"}};
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    auto cache = CSVDataManager::instance().get_cache_for_file(filename);

    try {
        cache->get_data(config, instances);
        assert(false && "Expected an exception for invalid data format");
    } catch (const std::runtime_error& e) {
        std::string error_message = e.what();
        assert(error_message.find("Failed to load CSV file") != std::string::npos);
        assert(error_message.find("stoll") != std::string::npos);
    }

    std::remove(filename.c_str());
    std::cout << "test_cache_propagates_error passed\n";
}

void test_multithreaded_access() {
    CSVDataManager::instance().reset();
    const std::string filename = "multithread.csv";
    create_test_file(filename, "id\n1\n2\n3\n4\n5");

    std::vector<std::thread> threads;
    int num_threads = 10;
    std::vector<std::shared_ptr<CSVFileCache>> caches(num_threads);

    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&, i]() {
            // All threads request the cache for the same file
            caches[i] = CSVDataManager::instance().get_cache_for_file(filename);
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // Verify all threads got the exact same cache instance
    for (size_t i = 1; i < caches.size(); ++i) {
        assert(caches[0] == caches[i] && "All threads should receive the same cache instance");
    }

    std::remove(filename.c_str());
    std::cout << "test_multithreaded_access passed\n";
}

int main() {
    test_manager_singleton_and_reset();
    test_manager_cache_retrieval();
    test_cache_loads_data_successfully();
    test_cache_propagates_error();
    test_multithreaded_access();

    std::cout << "All CSVDataManager tests passed!\n";
    return 0;
}