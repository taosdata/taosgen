/*
 * Copyright (c) 2025 TAOS Data, Inc.
 *
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#include "ProcessUtils.hpp"
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

void test_get_system_free_memory_gb() {
    double free_mem = ProcessUtils::get_system_free_memory_gb();
    assert(free_mem >= 0.0);
    std::cout << "test_get_system_free_memory_gb passed, free_mem = " << free_mem << " GB\n";
}

int main() {
    test_get_memory_usage_mb();
    test_get_thread_count();
    test_get_cpu_usage_percent();
    test_get_system_free_memory_gb();

    std::cout << "All ProcessUtils tests passed!\n";
    return 0;
}