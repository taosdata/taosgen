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

#include "TableNameManager.hpp"
#include <cassert>
#include <iostream>

void test_table_name_generation_with_generator() {
    InsertDataConfig config;
    config.schema.tbname.source_type = "generator";
    config.schema.tbname.generator.prefix = "test_table_";
    config.schema.tbname.generator.count = 10;
    config.schema.tbname.generator.from = 1;

    TableNameManager manager(config);
    auto names = manager.generate_table_names();

    assert(names.size() == 10);
    assert(names[0] == "test_table_1");
    assert(names[9] == "test_table_10");

    std::cout << "test_table_name_generation_with_generator passed.\n";
}

void test_index_range_split() {
    InsertDataConfig config;
    config.schema.tbname.source_type = "generator";
    config.schema.tbname.generator.prefix = "test_";
    config.schema.tbname.generator.count = 10;
    config.schema.generation.generate_threads = 3;
    config.thread_allocation = "index_range";

    TableNameManager manager(config);
    auto split_result = manager.split_for_threads();

    assert(split_result.size() == 3);
    assert(split_result[0].size() == 4);  // 10/3 rounded up
    assert(split_result[1].size() == 3);
    assert(split_result[2].size() == 3);

    std::cout << "test_index_range_split passed.\n";
}

int main() {
    test_table_name_generation_with_generator();
    test_index_range_split();

    std::cout << "All tests passed.\n";
    return 0;
}