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

#include "ColumnConfig.hpp"
#include "taos.h"
#include <iostream>
#include <cassert>

void test_get_type_tag() {
    assert(ColumnConfig::get_type_tag("bool") == ColumnTypeTag::BOOL);
    assert(ColumnConfig::get_type_tag("tinyint") == ColumnTypeTag::TINYINT);
    assert(ColumnConfig::get_type_tag("timestamp") == ColumnTypeTag::TIMESTAMP);
    assert(ColumnConfig::get_type_tag("varchar") == ColumnTypeTag::VARCHAR);
    assert(ColumnConfig::get_type_tag("json") == ColumnTypeTag::JSON);

    try {
        ColumnConfig::get_type_tag("unsupported_type");
        assert(false); // Should not reach here
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()) == "Unsupported type: unsupported_type");
    }
}

void test_column_config_default_constructor() {
    ColumnConfig config;
    assert(config.name.empty());
    assert(config.type.empty());
    assert(config.type_tag == ColumnTypeTag::UNKNOWN);
    assert(config.primary_key == false);
    assert(config.count == 1);
}

void test_column_config_with_name_type() {
    ColumnConfig config("column1", "int");
    assert(config.name == "column1");
    assert(config.type == "int");
    assert(config.gen_type == "random");
    assert(config.type_tag == ColumnTypeTag::INT);
}

void test_timestamp_column_config() {
    ColumnConfig config("ts_col", "timestamp");
    assert(config.type_tag == ColumnTypeTag::TIMESTAMP);
    constexpr size_t int64_type_index = variant_index<int64_t, ColumnType>::value;
    (void)int64_type_index;
    assert(config.type_index == int64_type_index);
    assert(config.get_fixed_type_size() == sizeof(int64_t));
    assert(config.get_taos_type() == TSDB_DATA_TYPE_TIMESTAMP);
}

void test_column_config_with_name_type_gen_type() {
    ColumnConfig config("column1", "int", "random");
    assert(config.name == "column1");
    assert(config.type == "int");
    assert(config.gen_type == "random");
    assert(config.type_tag == ColumnTypeTag::INT);
}

void test_column_config_with_name_type_gen_type_len() {
    ColumnConfig config("column2", "varchar(255)", "random");
    assert(config.name == "column2");
    assert(config.type == "varchar(255)");
    assert(config.gen_type == "random");
    assert(config.len.has_value());
    assert(config.len.value() == 255);
    assert(config.type_tag == ColumnTypeTag::VARCHAR);
}

void test_column_config_with_name_type_gen_type_min_max() {
    ColumnConfig config("column3", "double", "random", 0.0, 100.0);
    assert(config.name == "column3");
    assert(config.type == "double");
    assert(config.gen_type == "random");
    assert(config.min.has_value());
    assert(config.min.value() == 0.0);
    assert(config.max.has_value());
    assert(config.max.value() == 100.0);
    assert(config.type_tag == ColumnTypeTag::DOUBLE);
}

void test_parse_tag() {
    ColumnConfig config("column4", "varchar(10)", "random");
    config.parse_type();
    assert(config.type_tag == ColumnTypeTag::VARCHAR);
}

int main() {
    test_get_type_tag();
    test_column_config_default_constructor();
    test_column_config_with_name_type();
    test_timestamp_column_config();
    test_column_config_with_name_type_gen_type();
    test_column_config_with_name_type_gen_type_len();
    test_column_config_with_name_type_gen_type_min_max();
    test_parse_tag();

    std::cout << "All tests passed!" << std::endl;
    return 0;
}
