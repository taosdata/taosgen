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
#include "FormatterRegistrar.hpp"


void test_format_drop_database() {
    DataFormat format;
    format.format_type = "sql";
    CreateDatabaseConfig config;
    config.tdengine.database = "test_db";
    config.tdengine.drop_if_exists = true;

    auto formatter = FormatterFactory::create_formatter<CreateDatabaseConfig>(format);
    FormatResult result = formatter->format(config);

    assert(std::holds_alternative<std::vector<std::string>>(result));
    const auto& stmts = std::get<std::vector<std::string>>(result);
    (void)stmts;
    assert(stmts.size() == 2);
    assert(stmts[0] == "DROP DATABASE IF EXISTS `test_db`");
    std::cout << "test_format_drop_database passed!" << std::endl;
}

void test_format_create_database_without_properties() {
    DataFormat format;
    format.format_type = "sql";
    CreateDatabaseConfig config;
    config.tdengine.database = "test_db";
    config.tdengine.drop_if_exists = false;

    auto formatter = FormatterFactory::create_formatter<CreateDatabaseConfig>(format);
    FormatResult result = formatter->format(config);

    assert(std::holds_alternative<std::vector<std::string>>(result));
    const auto& stmts = std::get<std::vector<std::string>>(result);
    (void)stmts;
    assert(stmts.size() == 1);
    assert(stmts[0] == "CREATE DATABASE IF NOT EXISTS `test_db` precision 'ms'");
    std::cout << "test_format_create_database_without_properties passed!" << std::endl;
}

void test_format_create_database_with_properties() {
    DataFormat format;
    format.format_type = "sql";
    CreateDatabaseConfig config;
    config.tdengine.database = "test_db";
    config.tdengine.drop_if_exists = true;
    config.tdengine.properties = "KEEP 3650";

    auto formatter = FormatterFactory::create_formatter<CreateDatabaseConfig>(format);
    FormatResult result = formatter->format(config);

    assert(std::holds_alternative<std::vector<std::string>>(result));
    const auto& stmts = std::get<std::vector<std::string>>(result);
    (void)stmts;
    assert(stmts.size() == 2);
    assert(stmts[0] == "DROP DATABASE IF EXISTS `test_db`");
    assert(stmts[1] == "CREATE DATABASE IF NOT EXISTS `test_db` KEEP 3650 precision 'ms'");
    std::cout << "test_format_create_database_with_properties passed!" << std::endl;
}

void test_format_create_database_with_precision_properties() {
    DataFormat format;
    format.format_type = "sql";
    CreateDatabaseConfig config;
    config.tdengine.database = "test_db";
    config.tdengine.properties = "precision 'us' KEEP 3650";
    config.tdengine.precision = "us";

    auto formatter = FormatterFactory::create_formatter<CreateDatabaseConfig>(format);
    FormatResult result = formatter->format(config);

    assert(std::holds_alternative<std::vector<std::string>>(result));
    const auto& stmts = std::get<std::vector<std::string>>(result);
    (void)stmts;
    assert(stmts.size() == 1);
    assert(stmts[0] == "CREATE DATABASE IF NOT EXISTS `test_db` precision 'us' KEEP 3650");
    std::cout << "test_format_create_database_with_precision_properties passed!" << std::endl;
}

int main() {
    test_format_drop_database();
    test_format_create_database_without_properties();
    test_format_create_database_with_properties();
    test_format_create_database_with_precision_properties();
    std::cout << "All tests passed!" << std::endl;
    return 0;
}
