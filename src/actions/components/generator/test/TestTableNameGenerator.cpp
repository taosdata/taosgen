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
#include "TableNameGenerator.hpp"

void test_generate_table_names() {
    TableNameConfig::Generator config;
    config.prefix = "table_";
    config.from = 1;
    config.count = 5;

    TableNameGenerator generator(config);
    std::vector<std::string> names = generator.generate();

    assert(names.size() == 5);
    assert(names[0] == "table_1");
    assert(names[1] == "table_2");
    assert(names[2] == "table_3");
    assert(names[3] == "table_4");
    assert(names[4] == "table_5");

    std::cout << "test_generate_table_names passed.\n";
}

void test_generate_empty_table_names() {
    TableNameConfig::Generator config;
    config.prefix = "table_";
    config.from = 1;
    config.count = 0;

    TableNameGenerator generator(config);
    std::vector<std::string> names = generator.generate();

    assert(names.empty());

    std::cout << "test_generate_empty_table_names passed.\n";
}

int main() {
    test_generate_table_names();
    test_generate_empty_table_names();

    std::cout << "All tests passed.\n";
    return 0;
}