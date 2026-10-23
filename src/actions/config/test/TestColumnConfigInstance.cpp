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

#include <iostream>
#include <cassert>
#include "ColumnConfigInstance.hpp"

void test_column_instance_single() {
    std::cout << "Running test_column_instance_single..." << std::endl;

    ColumnConfig config("col", "int", std::nullopt);
    ColumnConfigInstance instance(config);

    assert(instance.name() == "col");
    assert(instance.type() == "int");
    assert(instance.type_tag() == ColumnTypeTag::INT);

    std::cout << "test_column_instance_single passed!" << std::endl;
}

void test_column_instance_multiple() {
    std::cout << "Running test_column_instance_multiple..." << std::endl;

    ColumnConfig config("col", "int", std::nullopt);
    config.count = 3;

    auto instances = ColumnConfigInstanceFactory::create(config);

    assert(instances.size() == 3);
    assert(instances[0].name() == "col1");
    assert(instances[1].name() == "col2");
    assert(instances[2].name() == "col3");

    for (const auto& instance : instances) {
        (void)instance;
        assert(instance.type() == "int");
        assert(instance.type_tag() == ColumnTypeTag::INT);
    }

    std::cout << "test_column_instance_multiple passed!" << std::endl;
}

void test_column_instance_with_length() {
    std::cout << "Running test_column_instance_with_length..." << std::endl;

    ColumnConfig config("col", "varchar(50)", std::nullopt);
    ColumnConfigInstance instance(config);

    assert(instance.name() == "col");
    assert(instance.type() == "varchar(50)");
    assert(instance.type_tag() == ColumnTypeTag::VARCHAR);

    std::cout << "test_column_instance_with_length passed!" << std::endl;
}

void test_column_instance_with_min_max() {
    std::cout << "Running test_column_instance_with_min_max..." << std::endl;

    ColumnConfig config("col", "float", std::nullopt, 1.0, 10.0);
    ColumnConfigInstance instance(config);

    assert(instance.name() == "col");
    assert(instance.type() == "float");
    assert(instance.type_tag() == ColumnTypeTag::FLOAT);

    std::cout << "test_column_instance_with_min_max passed!" << std::endl;
}

int main() {
    std::cout << "Running ColumnConfigInstance tests..." << std::endl;

    test_column_instance_single();
    test_column_instance_multiple();
    test_column_instance_with_length();
    test_column_instance_with_min_max();

    std::cout << "All ColumnConfigInstance tests passed!" << std::endl;
    return 0;
}