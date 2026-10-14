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

#include "SinkPluginFactory.hpp"
#include "InsertDataConfig.hpp"
#include "ColumnConfigInstance.hpp"

#include <cassert>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>

// Creates a basic InsertDataConfig for a given target type.
InsertDataConfig create_test_config(const std::string& target_type) {
    InsertDataConfig config;
    config.target_type = target_type;
    return config;
}

void test_create_unsupported_writer() {
    std::string unsupported_type = "non_existent_db";
    auto config = create_test_config(unsupported_type);

    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;

    try {
        auto sink = SinkPluginFactory::create_sink_plugin(
            config,
            col_instances,
            tag_instances
        );
        assert(false && "Should throw for unsupported type");
    } catch (const std::invalid_argument& e) {
        std::string expected_msg = "Unsupported sink plugin type: " + unsupported_type;
        assert(std::string(e.what()) == expected_msg);
    }

    std::cout << "test_create_unsupported_writer passed." << std::endl;
}

void test_create_csv_writer_throws() {
    auto config = create_test_config("csv");

    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;

    try {
        auto sink = SinkPluginFactory::create_sink_plugin(
            config,
            col_instances,
            tag_instances
        );
        assert(false && "Should throw for csv type");
    } catch (const std::invalid_argument& e) {
        assert(std::string(e.what()) == "Unsupported sink plugin type: csv");
    }

    std::cout << "test_create_csv_writer_throws passed." << std::endl;
}

int main() {
    test_create_unsupported_writer();
    test_create_csv_writer_throws();

    std::cout << "All SinkPluginFactory tests passed." << std::endl;
    return 0;
}