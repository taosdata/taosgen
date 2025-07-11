#include "ProcessUtils.h"
#include <cassert>
#include <iostream>
#include <thread>
#include <chrono>

void test_get_memory_usage_mb() {
    double mem = ProcessUtils::get_memory_usage_mb();
    assert(mem > 0);
    std::cout << "test_get_memory_usage_mb passed, mem = " << mem << " MB\n";
}

void test_get_thread_count() {
    int threads = ProcessUtils::get_thread_count();
    assert(threads > 0);
    std::cout << "test_get_thread_count passed, threads = " << threads << "\n";
}

void test_get_cpu_usage_percent() {
    ProcessUtils::get_cpu_usage_percent();
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    double cpu = ProcessUtils::get_cpu_usage_percent();
    assert(cpu >= 0.0);
    std::cout << "test_get_cpu_usage_percent passed, cpu = " << cpu << "%\n";
}

int main() {
    test_get_memory_usage_mb();
    test_get_thread_count();
    test_get_cpu_usage_percent();

    std::cout << "All ProcessUtils tests passed!\n";
    return 0;
}