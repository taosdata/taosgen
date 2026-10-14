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

#include "TagsCSVReader.hpp"
#include <iostream>
#include <cassert>
#include <fstream>

void test_validate_config_empty_file_path() {
    TagsCSV config;
    config.file_path = "";
    config.has_header = true;

    try {
        TagsCSVReader tags_csv(config, std::nullopt);
        assert(false && "Expected exception for empty file path");
    } catch (const std::invalid_argument& e) {
        std::cout << "test_validate_config_empty_file_path passed\n";
    }
}

void test_validate_config_mismatched_tag_types() {
    TagsCSV config;
    config.file_path = "test.csv";
    config.has_header = true;

    std::ofstream test_file("test.csv");
    test_file << "name,age,city\n";
    test_file << "Alice,30,New York\n";
    test_file.close();

    ColumnConfigVector tag_configs = {
        {"name", "varchar(20)"},
        {"age", "int"}
    };
    auto instances = ColumnConfigInstanceFactory::create(tag_configs);

    try {
        TagsCSVReader tags_csv(config, instances);
        assert(false && "Expected exception for mismatched tag types size");
    } catch (const std::invalid_argument& e) {
        std::cout << "test_validate_config_mismatched_tag_types passed\n";
    }
}

void test_generate_tags_valid_csv() {
    TagsCSV config;
    config.file_path = "valid.csv";
    config.has_header = true;

    std::ofstream test_file("valid.csv");
    test_file << "name,age,city\n";
    test_file << "Alice,30,New York\n";
    test_file << "Bob,25,Los Angeles\n";
    test_file.close();

    ColumnConfigVector tag_configs = {
        {"name", "varchar(20)"},
        {"age", "int"},
        {"city", "varchar(20)"}
    };
    auto instances = ColumnConfigInstanceFactory::create(tag_configs);

    TagsCSVReader tags_csv(config, instances);
    auto tags = tags_csv.generate();

    assert(tags.size() == 2 && "Expected 2 rows of tags");
    assert(std::get<std::string>(tags[0][0]) == "Alice" && "Expected first tag to be 'Alice'");
    assert(std::get<int32_t>(tags[0][1]) == 30 && "Expected second tag to be 30");
    assert(std::get<std::string>(tags[0][2]) == "New York" && "Expected third tag to be 'New York'");
    std::cout << "test_generate_tags_valid_csv passed\n";
}

void test_generate_tags_excluded_columns() {
    TagsCSV config;
    config.file_path = "excluded.csv";
    config.has_header = true;
    config.exclude_indices = {1}; // Exclude the second column

    std::ofstream test_file("excluded.csv");
    test_file << "name,age,city\n";
    test_file << "Alice,30,New York\n";
    test_file << "Bob,25,Los Angeles\n";
    test_file.close();

    ColumnConfigVector tag_configs = {
        {"name", "varchar(20)"},
        {"city", "varchar(20)"}
    };
    auto instances = ColumnConfigInstanceFactory::create(tag_configs);


    TagsCSVReader tags_csv(config, instances);
    auto tags = tags_csv.generate();

    assert(tags.size() == 2 && "Expected 2 rows of tags");
    assert(std::get<std::string>(tags[0][0]) == "Alice" && "Expected first tag to be 'Alice'");
    assert(std::get<std::string>(tags[0][1]) == "New York" && "Expected second tag to be 'New York'");
    std::cout << "test_generate_tags_excluded_columns passed\n";
}

void test_generate_tags_default_tag_types() {
    TagsCSV config;
    config.file_path = "default.csv";
    config.has_header = true;

    std::ofstream test_file("default.csv");
    test_file << "name,age,city\n";
    test_file << "Alice,30,New York\n";
    test_file << "Bob,25,Los Angeles\n";
    test_file.close();

    TagsCSVReader tags_csv(config, std::nullopt); // No tag types provided
    auto tags = tags_csv.generate();

    assert(tags.size() == 2 && "Expected 2 rows of tags");
    assert(std::get<std::string>(tags[0][0]) == "Alice" && "Expected first tag to be 'Alice'");
    assert(std::get<std::string>(tags[0][1]) == "30" && "Expected second tag to be '30'");
    assert(std::get<std::string>(tags[0][2]) == "New York" && "Expected third tag to be 'New York'");
    std::cout << "test_generate_tags_default_tag_types passed\n";
}

void test_generate_tags_numeric_null_literals() {
    TagsCSV config;
    config.file_path = "tags_null_literals.csv";
    config.has_header = true;

    std::ofstream test_file("tags_null_literals.csv");
    test_file << "id,region,score\n";
    test_file << "NA,North,100\n";
    test_file << "NULL,South,90\n";
    test_file << ",West,80\n";
    test_file.close();

    ColumnConfigVector tag_configs = {
        {"id", "int"},
        {"region", "varchar(20)"},
        {"score", "int"}
    };
    auto instances = ColumnConfigInstanceFactory::create(tag_configs);

    TagsCSVReader tags_csv(config, instances);
    auto tags = tags_csv.generate();

    assert(tags.size() == 3 && "Expected 3 rows of tags");
    assert(std::holds_alternative<NullValue>(tags[0][0]));
    assert(std::holds_alternative<NullValue>(tags[1][0]));
    assert(std::holds_alternative<NullValue>(tags[2][0]));

    assert(std::get<std::string>(tags[0][1]) == "North");
    assert(std::get<int32_t>(tags[0][2]) == 100);

    std::remove("tags_null_literals.csv");
    std::cout << "test_generate_tags_numeric_null_literals passed\n";
}

void test_generate_tags_bool_null_literals() {
    TagsCSV config;
    config.file_path = "tags_bool_null_literals.csv";
    config.has_header = true;

    std::ofstream test_file("tags_bool_null_literals.csv");
    test_file << "enabled,region\n";
    test_file << ",North\n";
    test_file << "NULL,South\n";
    test_file << "NA,West\n";
    test_file.close();

    ColumnConfigVector tag_configs = {
        {"enabled", "bool"},
        {"region", "varchar(20)"}
    };
    auto instances = ColumnConfigInstanceFactory::create(tag_configs);

    TagsCSVReader tags_csv(config, instances);
    auto tags = tags_csv.generate();

    assert(tags.size() == 3 && "Expected 3 rows of tags");
    assert(std::holds_alternative<NullValue>(tags[0][0]));
    assert(std::holds_alternative<NullValue>(tags[1][0]));
    assert(std::holds_alternative<NullValue>(tags[2][0]));

    assert(std::get<std::string>(tags[0][1]) == "North");

    std::remove("tags_bool_null_literals.csv");
    std::cout << "test_generate_tags_bool_null_literals passed\n";
}

int main() {
    test_validate_config_empty_file_path();
    test_validate_config_mismatched_tag_types();
    test_generate_tags_valid_csv();
    test_generate_tags_excluded_columns();
    test_generate_tags_default_tag_types();
    test_generate_tags_numeric_null_literals();
    test_generate_tags_bool_null_literals();

    std::cout << "All tests passed!\n";
    return 0;
}