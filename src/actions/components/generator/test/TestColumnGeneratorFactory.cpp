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
#include "ColumnGeneratorFactory.hpp"


void test_factory_create_random_column_generator() {
    ColumnConfig config;
    config.gen_type = std::string("random");
    config.type = "int";
    config.min = 5;
    config.max = 15;

    ColumnConfigInstance instance(config);

    auto generator = ColumnGeneratorFactory::create("", instance);
    assert(generator != nullptr); // Ensure the generator is created successfully

    for (int i = 0; i < 100; ++i) {
        ColumnType value = generator->generate();
        assert(std::holds_alternative<int>(value));
        int int_value = std::get<int>(value);
        (void)int_value;
        assert(int_value >= 5 && int_value < 15);
    }

    std::cout << "test_factory_create_random_column_generator passed.\n";
}

int main() {
    test_factory_create_random_column_generator();

    std::cout << "All tests passed.\n";
    return 0;
}