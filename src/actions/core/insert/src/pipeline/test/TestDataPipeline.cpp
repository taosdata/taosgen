#include "DataPipeline.hpp"
#include <cassert>
#include <thread>
#include <vector>
#include <iostream>

void test_basic_operations() {
    DataPipeline<int> pipeline(2);

    pipeline.push_data(42);
    pipeline.push_data(43);

    auto result1 = pipeline.fetch_data();
    auto result2 = pipeline.fetch_data();

    assert(result1.status == DataPipeline<int>::Status::Success);
    assert(result2.status == DataPipeline<int>::Status::Success);
    assert(result1.data);
    assert(result2.data);
    assert(*result1.data == 42);
    assert(*result2.data == 43);

    std::cout << "test_basic_operations passed.\n";
}

void test_termination() {
    DataPipeline<int> pipeline(2);

    pipeline.push_data(42);
    pipeline.terminate();

    try {
        pipeline.push_data(43);
        assert(false && "Should have thrown exception");
    } catch (const std::runtime_error&) {
        // Expected
    }

    auto result = pipeline.fetch_data();
    assert(result.status == DataPipeline<int>::Status::Success);
    assert(result.data);
    assert(*result.data == 42);
    result = pipeline.fetch_data();
    assert(result.status == DataPipeline<int>::Status::Terminated);

    std::cout << "test_termination passed.\n";
}

void test_concurrent_operations() {
    DataPipeline<int> pipeline(100);
    std::atomic<bool> stop{false};

    // Producer threads
    std::vector<std::thread> producers;
    for (size_t i = 0; i < 2; i++) {
        producers.emplace_back([&pipeline, &stop]() {
            int count = 0;
            while (!stop && count < 100) {
                try {
                    pipeline.push_data(count++);
                } catch (const std::runtime_error&) {
                    break;
                }
            }
        });
    }

    // Consumer threads
    std::vector<std::thread> consumers;
    std::atomic<size_t> consumed{0};
    for (size_t i = 0; i < 2; i++) {
        consumers.emplace_back([&pipeline, &consumed, &stop]() {
            while (!stop) {
                auto result = pipeline.fetch_data();
                if (result.status == DataPipeline<int>::Status::Success) {
                    consumed++;
                } else if (result.status == DataPipeline<int>::Status::Terminated) {
                    break;
                }
            }
        });
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    stop = true;
    pipeline.terminate();

    for (auto& p : producers) p.join();
    for (auto& c : consumers) c.join();

    assert(consumed == 200);
    std::cout << "test_concurrent_operations passed.\n";
}

void test_queue_monitoring() {
    DataPipeline<int> pipeline(2);

    assert(pipeline.total_queued() == 0);

    pipeline.push_data(42);
    pipeline.push_data(43);

    assert(pipeline.total_queued() == 2);

    auto result = pipeline.fetch_data();
    assert(result.status == DataPipeline<int>::Status::Success);

    assert(pipeline.total_queued() == 1);

    std::cout << "test_queue_monitoring passed.\n";
}

int main() {
    test_basic_operations();
    test_termination();
    test_concurrent_operations();
    test_queue_monitoring();

    std::cout << "All DataPipeline tests passed.\n";
    return 0;
}